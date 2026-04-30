// InodeTable :: a bi-directional map of paths to inodes.
//
// Copyright (c) 2016-2022 by William R. Fraser
//

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::sync::Arc;

use crate::{EntryName, FolderPath, Inode};

pub type Generation = u64;
pub type LookupCount = u64;

pub trait InodeToPath: std::fmt::Debug {
    fn add_leaf(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)>;
    fn add_dir(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)>;
    fn add_or_get_leaf(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)>;
    fn add_or_get_dir(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)>;
    fn forget(&mut self, inode: Inode, n: LookupCount) -> LookupCount;
    fn get_path(&self, inode: Inode) -> Option<EntryName>;
    fn resolve_from_parent(&self, parent: Inode, name: OsString) -> Option<EntryName> {
        let parent = self.get_folder_path(parent)?;
        Some(EntryName::new(parent, name))
    }
    fn get_folder_path(&self, inode: Inode) -> Option<FolderPath>;
    fn get_parent_inode(&self, ino: Inode) -> Option<Inode>;
    fn lookup(&mut self, inode: Inode);
    fn rename(
        &mut self,
        oldparent: Inode,
        oldname: &OsStr,
        newparent: Inode,
        newname: &OsStr,
    ) -> Option<()>;
    fn unlink(&mut self, parent: Inode, name: &OsStr);
}

mod child_key {
    use super::Inode;
    use std::borrow::Borrow;
    use std::ffi::{OsStr, OsString};
    use std::hash::{Hash, Hasher};

    /// Borrowed child key used for zero-allocation lookups.
    #[derive(Clone, Copy, Debug)]
    pub struct ChildKeyRef<'a> {
        parent: Inode,
        name: &'a OsStr,
    }

    impl<'a> ChildKeyRef<'a> {
        pub fn new(parent: Inode, name: &'a OsStr) -> Self {
            Self { parent, name }
        }

        pub fn name(&self) -> &OsStr {
            self.name
        }
    }

    impl<'a> Hash for ChildKeyRef<'a> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.parent.hash(state);
            self.name().hash(state);
        }
    }

    impl<'a> PartialEq for ChildKeyRef<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.parent == other.parent && self.name() == other.name()
        }
    }

    impl<'a> Eq for ChildKeyRef<'a> {}

    /// Owned child key stored in the live parent/name index.
    #[derive(Debug)]
    pub struct ChildKey {
        _name: OsString,
        // Although borrowed stores a 'static reference internally,
        // it is only exposed through &self via Borrow, so callers cannot obtain a
        // reference that outlives the ChildKey value
        borrowed: ChildKeyRef<'static>,
    }

    impl ChildKey {
        pub fn new(parent: Inode, name: OsString) -> Self {
            // Safety:
            // ref_name points into the heap buffer owned by _name.
            // Moving name into Self does not move that buffer.
            // After construction, _name must never be mutated or replaced, or borrowed
            // would dangle!
            let ref_name: &'static OsStr = unsafe { (*(&name as *const OsString)).as_os_str() };
            let borrowed = ChildKeyRef::new(parent, ref_name);
            Self {
                _name: name,
                borrowed,
            }
        }
    }

    impl<'a> Borrow<ChildKeyRef<'a>> for ChildKey {
        fn borrow(&self) -> &ChildKeyRef<'a> {
            &self.borrowed
        }
    }

    impl Hash for ChildKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.borrowed.hash(state);
        }
    }

    impl PartialEq for ChildKey {
        fn eq(&self, other: &Self) -> bool {
            self.borrowed == other.borrowed
        }
    }

    impl Eq for ChildKey {}
}

/// Cached path data for directory inodes.
#[derive(Debug)]
struct FolderEntry {
    parent_inode: Inode,
    path: Arc<PathBuf>,
    parent: Arc<PathBuf>,
    name: OsString,
}

impl FolderEntry {
    /// Builds a directory entry from a live parent path and child name.
    fn with_parent(parent_inode: Inode, parent: Arc<PathBuf>, name: OsString) -> Self {
        Self {
            path: Arc::new(parent.join(&name)),
            parent_inode,
            parent,
            name,
        }
    }

    /// Replaces cached directory path metadata after a rename.
    fn set_path(&mut self, full_path: Arc<PathBuf>) -> Option<()> {
        self.name = full_path.file_name()?.into();
        self.parent = Arc::new(full_path.parent()?.into());
        self.path = full_path;
        Some(())
    }

    fn path(&self) -> Arc<PathBuf> {
        self.path.clone()
    }

    fn name(&self) -> &OsStr {
        &self.name
    }
}

/// File-like entry whose full path is derived from its parent directory.
#[derive(Debug)]
struct LeafEntry {
    parent: Inode,
    name: OsString,
}

/// Occupancy state for a table slot.
#[derive(Debug)]
enum Entry {
    Vacant,
    Root,
    Folder(FolderEntry),
    Leaf(LeafEntry),
}

/// Metadata stored for one inode slot.
#[derive(Debug)]
struct InodeEntry {
    entry: Entry,
    linked: bool,
    child_count: usize,
    lookups: LookupCount,
    generation: Generation,
}

impl InodeEntry {
    /// Returns the parent path and name needed to build an entry path.
    fn path<'a>(&'a self, table: &'a [InodeEntry]) -> Option<(Arc<PathBuf>, &'a OsStr)> {
        match &self.entry {
            Entry::Vacant => None,
            Entry::Root => Some((Arc::new(PathBuf::from("/")), OsStr::new(""))),
            Entry::Folder(folder) => Some((folder.parent.clone(), folder.name())),
            Entry::Leaf(leaf) => {
                let parent_idx = leaf.parent as usize - 1;
                match &table.get(parent_idx)?.entry {
                    Entry::Root => Some((Arc::new(PathBuf::from("/")), &leaf.name)),
                    Entry::Folder(parent) => Some((parent.path(), &leaf.name)),
                    _ => None,
                }
            }
        }
    }
}

/// Tree-backed inode table with live child and folder-path indexes.
#[derive(Debug)]
pub struct InodeTable {
    // Inode n is stored at table[n - 1].
    table: Vec<InodeEntry>,
    // Vacant slots that can be reused with a bumped generation.
    free_list: VecDeque<usize>,
    // Live lookup index for parent inode and child name operations.
    children: HashMap<child_key::ChildKey, usize>,
    // Live folders sorted by path for subtree renames.
    folder_list: BTreeMap<Arc<PathBuf>, usize>,
}
impl InodeTable {
    /// Creates a table containing only the root inode.
    pub fn new() -> Self {
        let root = Arc::new(PathBuf::from("/"));
        let mut folder_list = BTreeMap::new();
        folder_list.insert(root, 0);

        Self {
            table: vec![InodeEntry {
                entry: Entry::Root,
                linked: true,
                child_count: 0,
                lookups: 0,
                generation: 0,
            }],
            free_list: VecDeque::new(),
            children: HashMap::new(),
            folder_list,
        }
    }

    /// Builds an owned key for storing in the child index.
    fn child_key(parent: Inode, name: OsString) -> child_key::ChildKey {
        child_key::ChildKey::new(parent, name)
    }

    /// Builds a borrowed key for looking up the child index.
    fn child_key_ref<'a>(parent: Inode, name: &'a OsStr) -> child_key::ChildKeyRef<'a> {
        child_key::ChildKeyRef::new(parent, name)
    }

    /// Converts a table index back to a public inode number.
    fn inode_for_idx(idx: usize) -> Inode {
        (idx + 1) as Inode
    }

    /// Returns a non-vacant inode entry.
    fn get_entry(&self, inode: Inode) -> Option<&InodeEntry> {
        let idx = inode.checked_sub(1)? as usize;
        let entry = self.table.get(idx)?;
        match entry.entry {
            Entry::Vacant => None,
            _ => Some(entry),
        }
    }

    /// Returns the path for a currently linked directory parent.
    fn live_dir_path(&self, inode: Inode) -> Option<Arc<PathBuf>> {
        let idx = (inode as usize).checked_sub(1)?;
        match &self.table[idx].entry {
            Entry::Root => Some(Arc::new(PathBuf::from("/"))),
            Entry::Folder(folder) if self.table[idx].linked => Some(folder.path()),
            _ => {
                error!("inode {inode} is not a live directory!");
                None
            }
        }
    }

    /// Allocates or reuses a slot and returns its inode number.
    fn get_inode_entry<'a>(
        free_list: &mut VecDeque<usize>,
        table: &'a mut Vec<InodeEntry>,
    ) -> (Inode, &'a mut InodeEntry) {
        let idx = match free_list.pop_front() {
            Some(idx) => {
                debug!("re-using inode {}", idx + 1);
                table[idx].generation += 1;
                idx
            }
            None => {
                table.push(InodeEntry {
                    entry: Entry::Vacant,
                    linked: false,
                    child_count: 0,
                    lookups: 0,
                    generation: 0,
                });
                table.len() - 1
            }
        };
        ((idx + 1) as Inode, &mut table[idx])
    }

    /// Shared insertion path for files and directories.
    fn add_child(
        &mut self,
        parent: Inode,
        name: &OsStr,
        is_dir: bool,
        initial_lookups: LookupCount,
        allow_existing: bool,
    ) -> Option<(Inode, Generation)> {
        let key = Self::child_key_ref(parent, name);

        if let Some(idx) = self.children.get(&key).copied() {
            let entry = &self.table[idx].entry;
            match (entry, is_dir) {
                (Entry::Folder(_), true) | (Entry::Leaf(_), false) => {}
                _ => {
                    error!(
                        "inode type mismatch for parent {parent} and child {:?}",
                        name
                    );
                    return None;
                }
            }

            if allow_existing {
                return Some((Self::inode_for_idx(idx), self.table[idx].generation));
            }
            error!(
                "attempted to insert duplicate child under inode {parent}: {:?}",
                name
            );
            return None;
        }

        let parent_path = self.live_dir_path(parent)?;
        // A child keeps its parent slot from being freed.
        self.table[parent as usize - 1].child_count += 1;

        let (inode, generation, folder_path) = {
            let (inode, entry) = Self::get_inode_entry(&mut self.free_list, &mut self.table);
            let child_name = name.to_os_string();
            entry.entry = if is_dir {
                Entry::Folder(FolderEntry::with_parent(
                    parent,
                    parent_path.clone(),
                    child_name,
                ))
            } else {
                Entry::Leaf(LeafEntry {
                    parent,
                    name: child_name,
                })
            };
            entry.linked = true;
            entry.child_count = 0;
            entry.lookups = initial_lookups;

            let folder_path = match &entry.entry {
                Entry::Folder(folder) => Some(folder.path()),
                _ => None,
            };
            (inode, entry.generation, folder_path)
        };

        let idx = inode as usize - 1;
        self.children
            .insert(Self::child_key(parent, name.into()), idx);
        if let Some(path) = folder_path {
            self.folder_list.insert(path, idx);
        }

        Some((inode, generation))
    }

    /// Drops live lookup indexes without freeing the inode slot.
    fn remove_live_indexes(&mut self, idx: usize) {
        if !self.table[idx].linked {
            return;
        }

        let (child_key, folder_path) = match &self.table[idx].entry {
            Entry::Folder(folder) => (
                Some(Self::child_key_ref(folder.parent_inode, folder.name())),
                Some(folder.path()),
            ),
            Entry::Leaf(leaf) => (Some(Self::child_key_ref(leaf.parent, &leaf.name)), None),
            _ => (None, None),
        };

        if let Some(key) = child_key {
            self.children.remove(&key);
        }
        if let Some(path) = folder_path {
            self.folder_list.remove(&path);
        }

        self.table[idx].linked = false;
    }

    /// Frees an inode once it has no lookups and no children.
    fn maybe_free_inode(&mut self, idx: usize) {
        if idx == 0 {
            return;
        }

        let (parent_inode, linked, child_key, folder_path) = {
            let entry = &self.table[idx];
            if entry.lookups != 0 || entry.child_count != 0 {
                return;
            }

            match &entry.entry {
                Entry::Vacant | Entry::Root => return,
                Entry::Folder(folder) => (
                    folder.parent_inode,
                    entry.linked,
                    Some(Self::child_key_ref(folder.parent_inode, folder.name())),
                    Some(folder.path()),
                ),
                Entry::Leaf(leaf) => (
                    leaf.parent,
                    entry.linked,
                    Some(Self::child_key_ref(leaf.parent, &leaf.name)),
                    None,
                ),
            }
        };

        if linked {
            if let Some(key) = child_key {
                self.children.remove(&key);
            }
            if let Some(path) = folder_path.as_ref() {
                self.folder_list.remove(path);
            }
        }

        self.table[idx].entry = Entry::Vacant;
        self.table[idx].linked = false;
        self.table[idx].child_count = 0;
        self.table[idx].lookups = 0;
        self.free_list.push_back(idx);

        let parent_idx = parent_inode as usize - 1;
        self.table[parent_idx].child_count -= 1;
        // Freeing a child may make an unlinked parent eligible too.
        self.maybe_free_inode(parent_idx);
    }

    /// Updates cached folder paths under a renamed directory.
    fn rename_folder_subtree(
        &mut self,
        old_path: Arc<PathBuf>,
        new_path: Arc<PathBuf>,
    ) -> Option<()> {
        // Detach everything at or after old_path, then attach back the right side.
        let mut suffix = self.folder_list.split_off(&old_path);
        if let Some(right_start) = suffix
            .keys()
            .find(|path| !path.as_path().starts_with(old_path.as_path()))
            .cloned()
        {
            let mut right = suffix.split_off(&right_start);
            self.folder_list.append(&mut right);
        }

        // Leaves are absent from folder_list; they follow their parent inode.
        for (path, idx) in suffix {
            let next_path = if path.as_path() == old_path.as_path() {
                new_path.clone()
            } else {
                let suffix = path.strip_prefix(old_path.as_path()).ok()?;
                Arc::new(new_path.join(suffix))
            };

            match &mut self.table[idx].entry {
                Entry::Folder(folder) => folder.set_path(next_path.clone())?,
                _ => {
                    error!("folder_list contains a non-folder entry!");
                    return None;
                }
            }

            self.folder_list.insert(next_path, idx);
        }
        Some(())
    }
}

impl InodeToPath for InodeTable {
    fn add_leaf(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)> {
        self.add_child(parent, name, false, 1, false)
    }

    fn add_dir(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)> {
        self.add_child(parent, name, true, 1, false)
    }

    fn add_or_get_leaf(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)> {
        self.add_child(parent, name, false, 0, true)
    }

    fn add_or_get_dir(&mut self, parent: Inode, name: &OsStr) -> Option<(Inode, Generation)> {
        self.add_child(parent, name, true, 0, true)
    }

    fn forget(&mut self, inode: Inode, n: LookupCount) -> LookupCount {
        if inode == 1 {
            return 1;
        }

        let idx = inode as usize - 1;
        let entry = &mut self.table[idx];
        assert!(!matches!(entry.entry, Entry::Vacant));
        assert!(n <= entry.lookups);
        entry.lookups -= n;
        let lookups = entry.lookups;
        self.maybe_free_inode(idx);
        lookups
    }

    fn get_path(&self, inode: Inode) -> Option<EntryName> {
        self.get_entry(inode)?
            .path(&self.table)
            .map(|val| EntryName::new(val.0.into(), val.1.into()))
    }

    fn get_folder_path(&self, inode: Inode) -> Option<FolderPath> {
        match &self.get_entry(inode)?.entry {
            Entry::Root => Some(Arc::new(PathBuf::from("/"))),
            Entry::Folder(folder) => Some(folder.path()),
            _ => None,
        }
        .map(|v| v.into())
    }

    fn get_parent_inode(&self, ino: Inode) -> Option<Inode> {
        match &self.get_entry(ino)?.entry {
            Entry::Vacant | Entry::Root => None,
            Entry::Folder(folder) => Some(folder.parent_inode),
            Entry::Leaf(leaf) => Some(leaf.parent),
        }
    }

    fn lookup(&mut self, inode: Inode) {
        if inode == 1 {
            return;
        }

        let entry = &mut self.table[inode as usize - 1];
        assert!(!matches!(entry.entry, Entry::Vacant));
        entry.lookups += 1;
    }

    fn rename(
        &mut self,
        oldparent: Inode,
        oldname: &OsStr,
        newparent: Inode,
        newname: &OsStr,
    ) -> Option<()> {
        if oldparent == newparent && oldname == newname {
            return Some(());
        }

        let old_key = Self::child_key_ref(oldparent, oldname);
        let idx = *self.children.get(&old_key)?;

        let new_parent_path = self.live_dir_path(newparent)?;
        let old_folder_path = match &self.table[idx].entry {
            Entry::Folder(folder) => Some(folder.path()),
            Entry::Leaf(_) => None,
            _ => return None, // exit function
        };

        // Remove any replaced live entry before inserting the moved one.
        let new_key = Self::child_key_ref(newparent, newname);
        let replaced_idx = self.children.get(&new_key).copied();
        if let Some(replaced_idx) = replaced_idx {
            self.remove_live_indexes(replaced_idx);
            self.maybe_free_inode(replaced_idx);
        }

        self.children.remove(&old_key);
        self.children
            .insert(Self::child_key(newparent, newname.into()), idx);

        if oldparent != newparent {
            self.table[oldparent as usize - 1].child_count -= 1;
            self.table[newparent as usize - 1].child_count += 1;
        }

        // Folder entries cache paths, so moving one may update its subtree.
        let mut moved_folder = None;
        match &mut self.table[idx].entry {
            Entry::Folder(folder) => {
                let new_path = Arc::new(new_parent_path.join(newname));
                folder.parent_inode = newparent;
                folder.set_path(new_path.clone());
                moved_folder = Some((old_folder_path?, new_path));
            }
            Entry::Leaf(leaf) => {
                leaf.parent = newparent;
                leaf.name = newname.to_os_string();
            }
            _ => return None, // exit function
        }

        if let Some((old_path, new_path)) = moved_folder {
            self.rename_folder_subtree(old_path, new_path)?;
        }

        self.table[idx].linked = true;
        Some(())
    }

    fn unlink(&mut self, parent: Inode, name: &OsStr) {
        // Remove the live name; open inodes are freed later.
        let key = Self::child_key_ref(parent, name);
        if let Some(idx) = self.children.remove(&key) {
            if let Entry::Folder(folder) = &self.table[idx].entry {
                self.folder_list.remove(&folder.path);
            }
            self.table[idx].linked = false;
            self.maybe_free_inode(idx);
        }
    }
}

#[cfg(test)]
// Tests for the tree-backed table
mod tests {
    use super::InodeToPath;
    use super::{Inode, InodeTable};
    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};

    fn name(value: &'static str) -> &'static OsStr {
        OsStr::new(value)
    }

    fn assert_path(table: &InodeTable, inode: Inode, expected: &str) {
        assert_eq!(
            PathBuf::from(expected),
            table.get_path(inode).unwrap().full_path()
        );
    }

    fn assert_folder_path(table: &InodeTable, inode: Inode, expected: &str) {
        let path = table.get_folder_path(inode).unwrap();
        assert_eq!(Path::new(expected), path.as_path());
    }

    #[test]
    fn inode_numbers_are_reused_after_forget() {
        let mut table = InodeTable::new();

        let (inode1, generation1) = table.add_leaf(1, name("a")).unwrap();
        assert_ne!(1, inode1);
        assert_eq!(0, generation1);
        assert_path(&table, inode1, "/a");

        let (inode2, _) = table.add_leaf(1, name("b")).unwrap();
        assert_ne!(inode1, inode2);
        assert_ne!(1, inode2);
        assert_path(&table, inode2, "/b");

        assert_eq!(0, table.forget(inode1, 1));
        assert!(table.get_path(inode1).is_none());

        let (inode3, generation3) = table.add_leaf(1, name("c")).unwrap();
        assert_eq!(inode1, inode3);
        assert_eq!(generation1 + 1, generation3);
        assert_path(&table, inode3, "/c");
    }

    #[test]
    fn add_or_get_returns_existing_inode_without_lookup() {
        let mut table = InodeTable::new();

        let (inode1, generation1) = table.add_or_get_leaf(1, name("a")).unwrap();
        assert_eq!(0, generation1);
        assert_path(&table, inode1, "/a");
        table.lookup(inode1);

        let (inode2, generation2) = table.add_leaf(1, name("b")).unwrap();
        assert_path(&table, inode2, "/b");

        let (inode2_again, generation2_again) = table.add_or_get_leaf(1, name("b")).unwrap();
        assert_eq!(inode2, inode2_again);
        assert_eq!(generation2, generation2_again);
        table.lookup(inode2);

        assert_eq!(0, table.forget(inode1, 1));
        assert_eq!(1, table.forget(inode2, 1));
    }

    #[test]
    fn rename_leaf_keeps_inode_and_updates_live_name() {
        let mut table = InodeTable::new();

        let (inode, generation) = table.add_leaf(1, name("a")).unwrap();
        assert_path(&table, inode, "/a");

        assert_eq!(Some(()), table.rename(1, name("a"), 1, name("b")));
        assert_path(&table, inode, "/b");
        assert_eq!(None, table.rename(1, name("a"), 1, name("c")));

        let (inode_again, generation_again) = table.add_or_get_leaf(1, name("b")).unwrap();
        assert_eq!(inode, inode_again);
        assert_eq!(generation, generation_again);
    }

    #[test]
    fn unlink_removes_live_name_but_keeps_open_inode_path() {
        let mut table = InodeTable::new();

        let (inode, _) = table.add_leaf(1, name("bar")).unwrap();
        table.unlink(1, name("bar"));

        assert_path(&table, inode, "/bar");
        let (new_inode, _) = table.add_or_get_leaf(1, name("bar")).unwrap();
        assert_ne!(inode, new_inode);

        assert_eq!(0, table.forget(inode, 1));
        assert!(table.get_path(inode).is_none());
        assert_path(&table, new_inode, "/bar");
    }

    #[test]
    fn folders_report_paths_and_parent_inodes() {
        let mut table = InodeTable::new();

        let (dir, _) = table.add_dir(1, name("dir")).unwrap();
        let (child, _) = table.add_leaf(dir, name("child")).unwrap();

        assert_path(&table, dir, "/dir");
        assert_folder_path(&table, dir, "/dir");
        assert_eq!(Some(1), table.get_parent_inode(dir));

        assert_path(&table, child, "/dir/child");
        assert!(table.get_folder_path(child).is_none());
        assert_eq!(Some(dir), table.get_parent_inode(child));
    }

    #[test]
    fn forget_keeps_parent_directory_until_children_are_gone() {
        let mut table = InodeTable::new();

        let (dir, _) = table.add_dir(1, name("dir")).unwrap();
        let (child, _) = table.add_leaf(dir, name("child")).unwrap();

        assert_eq!(0, table.forget(dir, 1));
        assert_path(&table, dir, "/dir");
        assert_path(&table, child, "/dir/child");

        assert_eq!(0, table.forget(child, 1));
        assert!(table.get_path(child).is_none());
        assert!(table.get_path(dir).is_none());
    }

    #[test]
    fn renaming_parent_directory_updates_descendant_paths() {
        let mut table = InodeTable::new();

        let (parent, parent_generation) = table.add_dir(1, name("parent")).unwrap();
        let (child_dir, _) = table.add_dir(parent, name("child")).unwrap();
        let (grandchild_dir, _) = table.add_dir(child_dir, name("grandchild")).unwrap();
        let (leaf, _) = table.add_leaf(grandchild_dir, name("leaf")).unwrap();

        assert_eq!(
            Some(()),
            table.rename(1, name("parent"), 1, name("renamed"))
        );

        assert_path(&table, parent, "/renamed");
        assert_folder_path(&table, parent, "/renamed");
        assert_path(&table, child_dir, "/renamed/child");
        assert_folder_path(&table, child_dir, "/renamed/child");
        assert_path(&table, grandchild_dir, "/renamed/child/grandchild");
        assert_folder_path(&table, grandchild_dir, "/renamed/child/grandchild");
        assert_path(&table, leaf, "/renamed/child/grandchild/leaf");
        assert_eq!(Some(parent), table.get_parent_inode(child_dir));
        assert_eq!(Some(grandchild_dir), table.get_parent_inode(leaf));
        assert_eq!(None, table.rename(1, name("parent"), 1, name("again")));

        let (parent_again, generation_again) = table.add_or_get_dir(1, name("renamed")).unwrap();
        assert_eq!(parent, parent_again);
        assert_eq!(parent_generation, generation_again);
    }

    #[test]
    fn moving_directory_to_another_parent_updates_descendant_paths() {
        let mut table = InodeTable::new();

        let (source, _) = table.add_dir(1, name("source")).unwrap();
        let (destination, _) = table.add_dir(1, name("destination")).unwrap();
        let (child_dir, child_generation) = table.add_dir(source, name("child")).unwrap();
        let (leaf, _) = table.add_leaf(child_dir, name("leaf")).unwrap();

        assert_eq!(
            Some(()),
            table.rename(source, name("child"), destination, name("moved"))
        );

        assert_path(&table, child_dir, "/destination/moved");
        assert_folder_path(&table, child_dir, "/destination/moved");
        assert_path(&table, leaf, "/destination/moved/leaf");
        assert_eq!(Some(destination), table.get_parent_inode(child_dir));
        assert_eq!(Some(child_dir), table.get_parent_inode(leaf));

        let (child_again, generation_again) =
            table.add_or_get_dir(destination, name("moved")).unwrap();
        assert_eq!(child_dir, child_again);
        assert_eq!(child_generation, generation_again);
    }

    #[test]
    fn renaming_directory_does_not_move_path_prefix_siblings() {
        let mut table = InodeTable::new();

        let (foo, _) = table.add_dir(1, name("foo")).unwrap();
        let (foo_child, _) = table.add_dir(foo, name("child")).unwrap();
        let (foo_file, _) = table.add_leaf(foo, name("file")).unwrap();
        let (fo, _) = table.add_dir(1, name("fo")).unwrap();
        let (foo2, foo2_generation) = table.add_dir(1, name("foo2")).unwrap();
        let (foo_bar, _) = table.add_dir(1, name("foo_bar")).unwrap();

        assert_eq!(Some(()), table.rename(1, name("foo"), 1, name("bar")));

        assert_path(&table, foo, "/bar");
        assert_path(&table, foo_child, "/bar/child");
        assert_path(&table, foo_file, "/bar/file");
        assert_path(&table, fo, "/fo");
        assert_path(&table, foo2, "/foo2");
        assert_folder_path(&table, foo2, "/foo2");
        assert_path(&table, foo_bar, "/foo_bar");
        assert_folder_path(&table, foo_bar, "/foo_bar");

        let (foo2_again, foo2_generation_again) = table.add_or_get_dir(1, name("foo2")).unwrap();
        assert_eq!(foo2, foo2_again);
        assert_eq!(foo2_generation, foo2_generation_again);
    }

    #[test]
    fn rename_over_existing_leaf_keeps_new_name_bound_to_moved_inode() {
        let mut table = InodeTable::new();

        let (moved, moved_generation) = table.add_leaf(1, name("a")).unwrap();
        let (replaced, _) = table.add_leaf(1, name("b")).unwrap();

        assert_eq!(Some(()), table.rename(1, name("a"), 1, name("b")));
        assert_path(&table, moved, "/b");
        assert_path(&table, replaced, "/b");

        let (live_inode, live_generation) = table.add_or_get_leaf(1, name("b")).unwrap();
        assert_eq!(moved, live_inode);
        assert_eq!(moved_generation, live_generation);

        assert_eq!(0, table.forget(replaced, 1));
        assert!(table.get_path(replaced).is_none());
    }
}
