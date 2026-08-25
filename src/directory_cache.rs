// DirectoryCache :: a cache for directory entries to simplify readdir calls.
//
// Copyright (c) 2017-2019 by William R. Fraser
//

use std::collections::HashMap;
use std::num::Wrapping;
use std::sync::Arc;

use fuser::Errno;

/// Directory entry cache.
///
/// The way FUSE does readdir() is it gives you a buffer and an offset and asks you to fill the
/// buffer. If you have more entries than fit in the buffer, FUSE will call you again with a higher
/// offset, until you return an empty buffer.
///
/// FuserNG consumes the target filesystem's directory stream as reply space becomes available and
/// retains the entries it has seen so FUSE can revisit earlier offsets.
///
/// To do this, we need to give FUSE our own file handle (the cache entry key) instead of the one
/// the filesystem returned from opendir(), so we have to store that file handle as well.
#[derive(Debug)]
pub(crate) struct DirectoryCache {
    next_key: Wrapping<u64>,
    entries: HashMap<u64, DirectoryCacheEntry>,
}

impl DirectoryCache {
    pub(crate) fn new() -> DirectoryCache {
        DirectoryCache {
            next_key: Wrapping(1),
            entries: HashMap::new(),
        }
    }

    /// Add a new entry with the given file handle and an un-populated directory entry list.
    /// This is intended to be called on opendir().
    pub(crate) fn new_entry(&mut self, fh: u64) -> u64 {
        let key = self.next_key.0;
        self.entries.insert(key, DirectoryCacheEntry::new(fh));
        self.next_key += Wrapping(1);
        key
    }

    /// Get the real file handle (the one set by the filesystem) for a given cache entry key.
    /// Panics if there is no such key.
    pub(crate) fn real_fh(&self, key: u64) -> u64 {
        self.entries
            .get(&key)
            .unwrap_or_else(|| {
                panic!("no such directory cache key {}", key);
            })
            .fh
    }

    /// Delete the cache entry with the given key.
    /// This is intended to be called on releasedir().
    /// Panics if there is no such key.
    pub(crate) fn delete(&mut self, key: u64) {
        self.entries.remove(&key);
    }
}

#[derive(Debug)]
pub(crate) struct DirectoryCacheEntry {
    pub(crate) fh: u64,
}

impl DirectoryCacheEntry {
    pub(crate) fn new(fh: u64) -> DirectoryCacheEntry {
        DirectoryCacheEntry { fh }
    }
}

/// State retained while serving directory requests for one directory handle.
pub(crate) struct ReaddirState<P, E> {
    pub(crate) entries: Vec<E>,
    pub(crate) producer: Option<P>,
    pub(crate) pending_error: Option<Errno>,
}

impl<P, E> ReaddirState<P, E> {
    /// Creates an empty history backed by a live producer.
    pub(crate) fn new(producer: P) -> Self {
        Self {
            entries: Vec::new(),
            producer: Some(producer),
            pending_error: None,
        }
    }
}

impl<P, E: std::fmt::Debug> std::fmt::Debug for ReaddirState<P, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReaddirState")
            .field("entries", &self.entries)
            .field("producer_active", &self.producer.is_some())
            .field("pending_error", &self.pending_error)
            .finish()
    }
}

/// Per-handle readdir state indexed by the synthetic FUSE handle.
#[derive(Debug)]
pub(crate) struct ReaddirCache<S> {
    entries: HashMap<u64, Arc<S>>,
}

impl<S> ReaddirCache<S> {
    /// Creates an empty cache.
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Associates a newly opened directory handle with an empty state slot.
    pub(crate) fn insert(&mut self, key: u64, slot: S) {
        self.entries.insert(key, Arc::new(slot));
    }

    /// Returns the state slot for an open directory handle.
    pub(crate) fn get(&self, key: u64) -> Option<Arc<S>> {
        self.entries.get(&key).cloned()
    }

    /// Removes state associated with a released directory handle.
    pub(crate) fn delete(&mut self, key: u64) {
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
