// DirectoryCache :: a cache for directory entries to simplify readdir calls.
//
// Copyright (c) 2017-2019 by William R. Fraser
//

use std::collections::HashMap;
use std::num::Wrapping;
use std::sync::Arc;

use fuser::Errno;

use super::{DirectoryEntry, DirectoryEntryPlus};

/// Directory entry cache.
///
/// The way FUSE does readdir() is it gives you a buffer and an offset and asks you to fill the
/// buffer. If you have more entries than fit in the buffer, FUSE will call you again with a higher
/// offset, until you return an empty buffer.
///
/// Implementing this in the filesystem is tedious and a little tricky, so instead fuse-mt has the
/// filesystem just return a Vec with *all* the directory entries, and it takes care of paginating
/// it for FUSE.
///
/// To do this, we need to cache the response from the filesystem, and we need to give FUSE our own
/// file handle (the cache entry key) instead of the one the filesystem returned from opendir(), so
/// we have to store that file handle as well.
#[derive(Debug)]
pub struct DirectoryCache {
    next_key: Wrapping<u64>,
    entries: HashMap<u64, DirectoryCacheEntry>,
}

impl DirectoryCache {
    pub fn new() -> DirectoryCache {
        DirectoryCache {
            next_key: Wrapping(1),
            entries: HashMap::new(),
        }
    }

    /// Add a new entry with the given file handle and an un-populated directory entry list.
    /// This is intended to be called on opendir().
    pub fn new_entry(&mut self, fh: u64) -> u64 {
        let key = self.next_key.0;
        self.entries.insert(key, DirectoryCacheEntry::new(fh));
        self.next_key += Wrapping(1);
        key
    }

    /// Get the real file handle (the one set by the filesystem) for a given cache entry key.
    /// Panics if there is no such key.
    pub fn real_fh(&self, key: u64) -> u64 {
        self.entries
            .get(&key)
            .unwrap_or_else(|| {
                panic!("no such directory cache key {}", key);
            })
            .fh
    }

    /// Get a mutable reference to the cache entry (file handle and entries) for the given key.
    /// Panics if there is no such key.
    pub fn get_mut(&mut self, key: u64) -> &mut DirectoryCacheEntry {
        self.entries.get_mut(&key).unwrap_or_else(|| {
            panic!("no such directory cache key {}", key);
        })
    }

    /// Delete the cache entry with the given key.
    /// This is intended to be called on releasedir().
    /// Panics if there is no such key.
    pub fn delete(&mut self, key: u64) {
        self.entries.remove(&key);
    }
}

#[derive(Debug)]
pub struct DirectoryCacheEntry {
    pub fh: u64,
    pub entries: Option<Vec<DirectoryEntry>>,
}

impl DirectoryCacheEntry {
    pub fn new(fh: u64) -> DirectoryCacheEntry {
        DirectoryCacheEntry { fh, entries: None }
    }
}

/// State retained while serving readdirplus requests for one directory handle.
pub struct ReaddirPlusState<P> {
    pub entries: Vec<DirectoryEntryPlus>,
    pub producer: Option<P>,
    pub pending_error: Option<Errno>,
}

impl<P> ReaddirPlusState<P> {
    /// Creates an empty history backed by a live producer.
    pub fn new(producer: P) -> Self {
        Self {
            entries: Vec::new(),
            producer: Some(producer),
            pending_error: None,
        }
    }
}

impl<P> std::fmt::Debug for ReaddirPlusState<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReaddirPlusState")
            .field("entries", &self.entries)
            .field("producer_active", &self.producer.is_some())
            .field("pending_error", &self.pending_error)
            .finish()
    }
}

/// Per-handle readdirplus state indexed by the synthetic FUSE handle.
#[derive(Debug)]
pub struct ReaddirPlusCache<S> {
    entries: HashMap<u64, Arc<S>>,
}

impl<S> ReaddirPlusCache<S> {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Associates a newly opened directory handle with an empty state slot.
    pub fn insert(&mut self, key: u64, slot: S) {
        self.entries.insert(key, Arc::new(slot));
    }

    /// Returns the state slot for an open directory handle.
    pub fn get(&self, key: u64) -> Option<Arc<S>> {
        self.entries.get(&key).cloned()
    }

    /// Removes state associated with a released directory handle.
    pub fn delete(&mut self, key: u64) {
        self.entries.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::DirectoryCache;

    #[test]
    fn duplicate_target_handles_receive_distinct_cache_keys() {
        let mut cache = DirectoryCache::new();

        let first = cache.new_entry(0);
        let second = cache.new_entry(0);

        assert_ne!(first, second);
        assert_eq!(cache.real_fh(first), 0);
        assert_eq!(cache.real_fh(second), 0);
    }
}
