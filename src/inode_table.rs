// InodeTable :: a bi-directional map of paths to inodes.
//
// Copyright (c) 2016-2022 by William R. Fraser
//

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::sync::Arc;

use crate::{EntryName, FolderPath, InodeToPath};

pub type Inode = u64;
pub type Generation = u64;
pub type LookupCount = u64;

mod child_key {
    use super::Inode;
    use std::borrow::Borrow;
    use std::ffi::{OsStr, OsString};
    use std::hash::{Hash, Hasher};

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

#[derive(Debug)]
struct FolderEntry {
    parent_inode: Inode,
    path: Arc<PathBuf>,
    parent: Arc<PathBuf>,
    name: OsString,
}

impl FolderEntry {
    fn with_parent(parent_inode: Inode, parent: Arc<PathBuf>, name: OsString) -> Self {
        Self {
            path: Arc::new(parent.join(&name)),
            parent_inode,
            parent,
            name,
        }
    }

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

#[derive(Debug)]
struct LeafEntry {
    parent: Inode,
    name: OsString,
}

#[derive(Debug)]
enum Entry {
    Vacant,
    Root,
    Folder(FolderEntry),
    Leaf(LeafEntry),
}

#[derive(Debug)]
struct InodeEntry {
    entry: Entry,
    linked: bool,
    child_count: usize,
    lookups: LookupCount,
    generation: Generation,
}

impl InodeEntry {
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

#[derive(Debug)]
pub struct InodeTable {
    table: Vec<InodeEntry>,
    free_list: VecDeque<usize>,
    children: HashMap<child_key::ChildKey, usize>,
    folder_list: BTreeMap<Arc<PathBuf>, usize>,
}
impl InodeTable {
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

    fn child_key(parent: Inode, name: OsString) -> child_key::ChildKey {
        child_key::ChildKey::new(parent, name)
    }

    fn child_key_ref<'a>(parent: Inode, name: &'a OsStr) -> child_key::ChildKeyRef<'a> {
        child_key::ChildKeyRef::new(parent, name)
    }

    fn inode_for_idx(idx: usize) -> Inode {
        (idx + 1) as Inode
    }

    fn get_entry(&self, inode: Inode) -> Option<&InodeEntry> {
        let idx = inode.checked_sub(1)? as usize;
        let entry = self.table.get(idx)?;
        match entry.entry {
            Entry::Vacant => None,
            _ => Some(entry),
        }
    }

    fn live_dir_path(&self, inode: Inode) -> Arc<PathBuf> {
        let idx = inode as usize - 1;
        match &self.table[idx].entry {
            Entry::Root => Arc::new(PathBuf::from("/")),
            Entry::Folder(folder) if self.table[idx].linked => folder.path(),
            Entry::Vacant | Entry::Folder(_) | Entry::Leaf(_) => {
                panic!("inode {} is not a live directory", inode)
            }
        }
    }

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

    fn add_child(
        &mut self,
        parent: Inode,
        name: &OsStr,
        is_dir: bool,
        initial_lookups: LookupCount,
        allow_existing: bool,
    ) -> (Inode, Generation) {
        let key = Self::child_key_ref(parent, name);

        if let Some(idx) = self.children.get(&key).copied() {
            let entry = &self.table[idx].entry;
            match (entry, is_dir) {
                (Entry::Folder(_), true) | (Entry::Leaf(_), false) => {}
                (Entry::Vacant, _)
                | (Entry::Root, _)
                | (Entry::Folder(_), false)
                | (Entry::Leaf(_), true) => {
                    panic!(
                        "inode type mismatch for parent {} and child {:?}",
                        parent, name
                    )
                }
            }

            if allow_existing {
                return (Self::inode_for_idx(idx), self.table[idx].generation);
            }

            panic!(
                "attempted to insert duplicate child under inode {}: {:?}",
                parent, name
            );
        }

        let parent_path = self.live_dir_path(parent);
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
                Entry::Vacant | Entry::Root | Entry::Leaf(_) => None,
            };
            (inode, entry.generation, folder_path)
        };

        let idx = inode as usize - 1;
        self.children
            .insert(Self::child_key(parent, name.into()), idx);
        if let Some(path) = folder_path {
            self.folder_list.insert(path, idx);
        }

        (inode, generation)
    }

    fn remove_live_indexes(&mut self, idx: usize) {
        if !self.table[idx].linked {
            return;
        }

        let (child_key, folder_path) = match &self.table[idx].entry {
            Entry::Vacant | Entry::Root => (None, None),
            Entry::Folder(folder) => (
                Some(Self::child_key_ref(folder.parent_inode, folder.name())),
                Some(folder.path()),
            ),
            Entry::Leaf(leaf) => (Some(Self::child_key_ref(leaf.parent, &leaf.name)), None),
        };

        if let Some(key) = child_key {
            self.children.remove(&key);
        }
        if let Some(path) = folder_path {
            self.folder_list.remove(&path);
        }

        self.table[idx].linked = false;
    }

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
        self.maybe_free_inode(parent_idx);
    }

    fn rename_folder_subtree(
        &mut self,
        old_path: Arc<PathBuf>,
        new_path: Arc<PathBuf>,
    ) -> Option<()> {
        let affected: Vec<(Arc<PathBuf>, usize)> = self
            .folder_list
            .range(old_path.clone()..)
            .take_while(|(path, _)| path.as_path().starts_with(old_path.as_path()))
            .map(|(path, idx)| (path.clone(), *idx))
            .collect();

        for (path, _) in &affected {
            self.folder_list.remove(path);
        }

        for (path, idx) in affected {
            let next_path = if path.as_path() == old_path.as_path() {
                new_path.clone()
            } else {
                let suffix = path.strip_prefix(old_path.as_path()).ok()?;
                Arc::new(new_path.join(suffix))
            };

            match &mut self.table[idx].entry {
                Entry::Folder(folder) => folder.set_path(next_path.clone())?,
                _ => {
                    //panic!("folder_list contains a non-folder entry")
                }
            }

            self.folder_list.insert(next_path, idx);
        }
        Some(())
    }
}

impl InodeToPath for InodeTable {
    fn add_leaf(&mut self, parent: Inode, name: &OsStr) -> (Inode, Generation) {
        self.add_child(parent, name, false, 1, false)
    }

    fn add_dir(&mut self, parent: Inode, name: &OsStr) -> (Inode, Generation) {
        self.add_child(parent, name, true, 1, false)
    }

    fn add_or_get_leaf(&mut self, parent: Inode, name: &OsStr) -> (Inode, Generation) {
        self.add_child(parent, name, false, 0, true)
    }

    fn add_or_get_dir(&mut self, parent: Inode, name: &OsStr) -> (Inode, Generation) {
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

    fn get_path(&self, inode: Inode) -> Option<EntryName<'_>> {
        self.get_entry(inode)?
            .path(&self.table)
            .map(|val| EntryName::new(val.0.into(), val.1))
    }

    fn get_folder_path(&self, inode: Inode) -> Option<FolderPath> {
        match &self.get_entry(inode)?.entry {
            Entry::Root => Some(Arc::new(PathBuf::from("/"))),
            Entry::Folder(folder) => Some(folder.path()),
            Entry::Vacant | Entry::Leaf(_) => None,
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

        let new_parent_path = self.live_dir_path(newparent);
        let old_folder_path = match &self.table[idx].entry {
            Entry::Folder(folder) => Some(folder.path()),
            Entry::Leaf(_) => None,
            _ => return None, // exit function
        };

        self.children.remove(&old_key);
        let replaced_idx = self
            .children
            .insert(Self::child_key(newparent, newname.into()), idx);

        if oldparent != newparent {
            self.table[oldparent as usize - 1].child_count -= 1;
            self.table[newparent as usize - 1].child_count += 1;
        }

        if let Some(replaced_idx) = replaced_idx {
            self.remove_live_indexes(replaced_idx);
            self.maybe_free_inode(replaced_idx);
        }

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
mod old {
    use super::Generation;
    use super::Inode;
    use super::LookupCount;
    use std::borrow::Borrow;
    use std::cmp::{Eq, PartialEq};
    use std::collections::hash_map::Entry::*;
    use std::collections::{HashMap, VecDeque};
    use std::hash::{Hash, Hasher};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    #[derive(Debug)]
    struct _InodeTableEntry {
        path: Option<Arc<PathBuf>>,
        lookups: LookupCount,
        generation: Generation,
    }

    /// A data structure for mapping paths to inodes and vice versa.
    #[derive(Debug)]
    pub struct _InodeTable {
        table: Vec<_InodeTableEntry>,
        free_list: VecDeque<usize>,
        by_path: HashMap<Arc<PathBuf>, usize>,
    }

    impl _InodeTable {
        /// Create a new inode table.
        ///
        /// inode table entries have a limited lifetime, controlled by a 'lookup count', which is
        /// manipulated with the `lookup` and `forget` functions.
        ///
        /// The table initially contains just the root directory ("/"), mapped to inode 1.
        /// inode 1 is special: it cannot be forgotten.
        pub fn new() -> _InodeTable {
            let mut inode_table = _InodeTable {
                table: Vec::new(),
                free_list: VecDeque::new(),
                by_path: HashMap::new(),
            };
            let root = Arc::new(PathBuf::from("/"));
            inode_table.table.push(_InodeTableEntry {
                path: Some(root.clone()),
                lookups: 0, // not used for this entry; root is always present.
                generation: 0,
            });
            inode_table.by_path.insert(root, 0);
            inode_table
        }

        /// Add a path to the inode table.
        ///
        /// Returns the inode number the path is now mapped to.
        /// The returned inode number may be a re-used number formerly assigned to a now-forgotten
        /// path.
        ///
        /// The path is added with an initial lookup count of 1.
        ///
        /// This operation runs in O(log n) time.
        pub fn add(&mut self, path: Arc<PathBuf>) -> (Inode, Generation) {
            let (inode, generation) = {
                let (inode, entry) = Self::get_inode_entry(&mut self.free_list, &mut self.table);
                entry.path = Some(path.clone());
                entry.lookups = 1;
                (inode, entry.generation)
            };
            debug!("explicitly adding {} -> {:?} with 1 lookups", inode, path);
            let previous = self.by_path.insert(path, inode as usize - 1);
            if let Some(previous) = previous {
                error!("inode table buggered: {:?}", self);
                panic!(
                    "attempted to insert duplicate path into inode table: {:?}",
                    previous
                );
            }
            (inode, generation)
        }

        /// Add a path to the inode table if it does not yet exist.
        ///
        /// Returns the inode number the path is now mapped to.
        ///
        /// If the path was not in the table, it is added with an initial lookup count of 0.
        ///
        /// This operation runs in O(log n) time.
        pub fn add_or_get(&mut self, path: Arc<PathBuf>) -> (Inode, Generation) {
            match self.by_path.entry(path.clone()) {
                Vacant(path_entry) => {
                    let (inode, entry) =
                        Self::get_inode_entry(&mut self.free_list, &mut self.table);
                    debug!("adding {} -> {:?} with 0 lookups", inode, path);
                    entry.path = Some(path);
                    path_entry.insert(inode as usize - 1);
                    (inode, entry.generation)
                }
                Occupied(path_entry) => {
                    let idx = path_entry.get();
                    ((idx + 1) as Inode, self.table[*idx].generation)
                }
            }
        }

        /// Get the path that corresponds to an inode, if there is one, or None, if it is not in the
        /// table.
        /// Note that the file could be unlinked but still open, in which case it's not actually
        /// reachable from the path returned.
        ///
        /// This operation runs in O(1) time.
        pub fn get_path(&self, inode: Inode) -> Option<Arc<PathBuf>> {
            self.table[inode as usize - 1].path.clone()
        }

        /// Get the inode that corresponds to a path, if there is one, or None, if it is not in the
        /// table.
        ///
        /// This operation runs in O(log n) time.
        pub fn get_inode(&mut self, path: &Path) -> Option<Inode> {
            self.by_path
                .get(Pathish::new(path))
                .map(|idx| (idx + 1) as Inode)
        }

        /// Increment the lookup count on a given inode.
        ///
        /// Calling this on an invalid inode will result in a panic.
        ///
        /// This operation runs in O(1) time.
        pub fn lookup(&mut self, inode: Inode) {
            if inode == 1 {
                return;
            }

            let entry = &mut self.table[inode as usize - 1];
            entry.lookups += 1;
            debug!(
                "lookups on {} -> {:?} now {}",
                inode, entry.path, entry.lookups
            );
        }

        /// Decrement the lookup count on a given inode by the given number.
        ///
        /// If the lookup count reaches 0, the path is removed from the table, and the inode number
        /// is eligible to be re-used.
        ///
        /// Returns the new lookup count of the inode. (If it returns 0, that means the inode was
        /// deleted.)
        ///
        /// Calling this on an invalid inode will result in a panic.
        ///
        /// This operation runs in O(1) time normally, or O(log n) time if the inode is deleted.
        pub fn forget(&mut self, inode: Inode, n: LookupCount) -> LookupCount {
            if inode == 1 {
                return 1;
            }

            let mut delete = false;
            let lookups: LookupCount;
            let idx = inode as usize - 1;

            {
                let entry = &mut self.table[idx];
                debug!("forget entry {:?}", entry);
                assert!(n <= entry.lookups);
                entry.lookups -= n;
                lookups = entry.lookups;
                if lookups == 0 {
                    delete = true;
                    self.by_path.remove(entry.path.as_ref().unwrap());
                }
            }

            if delete {
                self.table[idx].path = None;
                self.free_list.push_back(idx);
            }

            lookups
        }

        /// Change an inode's path to a different one, without changing the inode number.
        /// Lookup counts remain unchanged, even if this is replacing another file.
        pub fn rename(&mut self, oldpath: &Path, newpath: Arc<PathBuf>) {
            let idx = self.by_path.remove(Pathish::new(oldpath)).unwrap();
            self.table[idx].path = Some(newpath.clone());
            self.by_path.insert(newpath, idx); // this can replace a path with a new inode
        }

        /// Remove the path->inode mapping for a given path, but keep the inode around.
        pub fn unlink(&mut self, path: &Path) {
            self.by_path.remove(Pathish::new(path));
            // Note that the inode->path mapping remains.
        }

        /// Get a free indode table entry and its number, either by allocating a new one, or re-using
        /// one that had its lookup count previously go to zero.
        ///
        /// 1st arg should be `&mut self.free_list`; 2nd arg should be `&mut self.table`.
        /// This function's signature is like this instead of taking &mut self so that it can avoid
        /// mutably borrowing *all* fields of self when we only need those two.
        fn get_inode_entry<'a>(
            free_list: &mut VecDeque<usize>,
            table: &'a mut Vec<_InodeTableEntry>,
        ) -> (Inode, &'a mut _InodeTableEntry) {
            let idx = match free_list.pop_front() {
                Some(idx) => {
                    debug!("re-using inode {}", idx + 1);
                    table[idx].generation += 1;
                    idx
                }
                None => {
                    table.push(_InodeTableEntry {
                        path: None,
                        lookups: 0,
                        generation: 0,
                    });
                    table.len() - 1
                }
            };
            ((idx + 1) as Inode, &mut table[idx])
        }
    }

    /// Facilitates comparing Rc<PathBuf> and &Path
    #[derive(Debug)]
    struct Pathish {
        inner: Path,
    }

    impl Pathish {
        pub fn new(p: &Path) -> &Pathish {
            unsafe { &*(p as *const _ as *const Pathish) }
        }
    }

    impl Borrow<Pathish> for Arc<PathBuf> {
        fn borrow(&self) -> &Pathish {
            Pathish::new(self.as_path())
        }
    }

    impl Hash for Pathish {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }

    impl Eq for Pathish {}

    impl PartialEq for Pathish {
        fn eq(&self, other: &Pathish) -> bool {
            self.inner.eq(&other.inner)
        }
    }

    #[test]
    fn test_inode_reuse() {
        let mut table = _InodeTable::new();
        let path1 = Arc::new(PathBuf::from("/foo/a"));
        let path2 = Arc::new(PathBuf::from("/foo/b"));

        // Add a path.
        let inode1 = table.add(path1.clone()).0;
        assert!(inode1 != 1);
        assert_eq!(*path1, *table.get_path(inode1).unwrap());

        // Add a second path; verify that the inode number is different.
        let inode2 = table.add(path2.clone()).0;
        assert!(inode2 != inode1);
        assert!(inode2 != 1);
        assert_eq!(*path2, *table.get_path(inode2).unwrap());

        // Forget the first inode; verify that lookups on it fail.
        assert_eq!(0, table.forget(inode1, 1));
        assert!(table.get_path(inode1).is_none());

        // Add a third path; verify that the inode is reused.
        let (inode3, generation3) = table.add(Arc::new(PathBuf::from("/foo/c")));
        assert_eq!(inode1, inode3);
        assert_eq!(1, generation3);

        // Check that lookups on the third path succeed.
        assert_eq!(Path::new("/foo/c"), *table.get_path(inode3).unwrap());
    }

    #[test]
    fn test_add_or_get() {
        let mut table = _InodeTable::new();
        let path1 = Arc::new(PathBuf::from("/foo/a"));
        let path2 = Arc::new(PathBuf::from("/foo/b"));

        // add_or_get() a path and verify that get by inode works before lookup() is done.
        let inode1 = table.add_or_get(path1.clone()).0;
        assert_eq!(*path1, *table.get_path(inode1).unwrap());
        table.lookup(inode1);

        // add() a second path and verify that get by path and inode work.
        let inode2 = table.add(path2.clone()).0;
        assert_eq!(*path2, *table.get_path(inode2).unwrap());
        assert_eq!(inode2, table.add_or_get(path2).0);
        table.lookup(inode2);

        // Check the ref counts by doing a single forget.
        assert_eq!(0, table.forget(inode1, 1));
        assert_eq!(1, table.forget(inode2, 1));
    }

    #[test]
    fn test_inode_rename() {
        let mut table = _InodeTable::new();
        let path1 = Arc::new(PathBuf::from("/foo/a"));
        let path2 = Arc::new(PathBuf::from("/foo/b"));

        // Add a path; verify that get by path and inode work.
        let inode = table.add(path1.clone()).0;
        assert_eq!(*path1, *table.get_path(inode).unwrap());
        assert_eq!(inode, table.get_inode(&path1).unwrap());

        // Rename the inode; verify that get by the new path works and old path doesn't, and get by
        // inode still works.
        table.rename(&path1, path2.clone());
        assert!(table.get_inode(&path1).is_none());
        assert_eq!(inode, table.get_inode(&path2).unwrap());
        assert_eq!(*path2, *table.get_path(inode).unwrap());
    }

    #[test]
    fn test_unlink() {
        let mut table = _InodeTable::new();
        let path = Arc::new(PathBuf::from("/foo/bar"));

        // Add a path.
        let inode = table.add(path.clone()).0;

        // Unlink it and verify that get by path fails.
        table.unlink(&path);
        assert!(table.get_inode(&path).is_none());

        // Getting the path for the inode should still return the path.
        assert_eq!(*path, *table.get_path(inode).unwrap());

        // Verify that forgetting it once drops the refcount to zero and then lookups by inode fail.
        assert_eq!(0, table.forget(inode, 1));
        assert!(table.get_path(inode).is_none());
    }
}
