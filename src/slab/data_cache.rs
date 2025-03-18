use linked_hash_map::*;
use std::sync::Arc;

//------------------------------------------------

/// A cache for slab data that uses an LRU eviction policy.
///
/// This cache stores data blocks indexed by slab number and tracks cache hits and misses.
/// When the cache reaches capacity, the least recently used entries are evicted.
pub struct DataCache {
    max_entries: usize,
    data: LinkedHashMap<u32, Arc<Vec<u8>>>,
    pub hits: u64,
    pub misses: u64,
}

impl DataCache {
    /// Creates a new `DataCache` with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of entries to store in the cache
    pub fn new(max_entries: usize) -> Self {
        let data = LinkedHashMap::new();

        Self {
            max_entries,
            data,
            hits: 0,
            misses: 0,
        }
    }

    /// Looks up a slab in the cache.
    ///
    /// Updates hit/miss statistics based on whether the slab was found.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab index to look up
    ///
    /// # Returns
    ///
    /// * `Some(Arc<Vec<u8>>)` - The cached data if found
    /// * `None` - If the slab is not in the cache
    pub fn find(&mut self, slab: u32) -> Option<Arc<Vec<u8>>> {
        match self.data.remove(&slab) {
            Some(data) => {
                let r = data.clone();

                self.hits += 1;

                // Reinsert as the most recently accessed
                self.data.insert(slab, data);
                Some(r)
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    /// Inserts a slab into the cache.
    ///
    /// If the cache is at capacity, the least recently used entry will be evicted.
    /// If the slab is already in the cache, it will be marked as recently used.
    ///
    /// # Arguments
    ///
    /// * `slab` - The slab index to insert
    /// * `data` - The slab data to cache
    pub fn insert(&mut self, slab: u32, data: Arc<Vec<u8>>) {
        match self.data.remove(&slab) {
            Some(_) => {
                // There was already an entry for this slab.  We don't
                // need to ensure space.
            }
            None => {
                // Is there space for the new entry?
                if self.data.len() >= self.max_entries {
                    // We need to evict the oldest entry
                    let _ = self.data.pop_front();
                }
            }
        }
        self.data.insert(slab, data);
    }
}

//------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cache() {
        let cache = DataCache::new(10);
        assert_eq!(cache.max_entries, 10);
        assert_eq!(cache.hits, 0);
        assert_eq!(cache.misses, 0);
        assert!(cache.data.is_empty());
    }

    #[test]
    fn test_find_miss() {
        let mut cache = DataCache::new(10);
        assert!(cache.find(1).is_none());
        assert_eq!(cache.misses, 1);
        assert_eq!(cache.hits, 0);
    }

    #[test]
    fn test_insert_and_find() {
        let mut cache = DataCache::new(10);
        let data = Arc::new(vec![1, 2, 3]);

        cache.insert(1, data.clone());

        let result = cache.find(1);
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), *data);
        assert_eq!(cache.hits, 1);
        assert_eq!(cache.misses, 0);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = DataCache::new(2);

        cache.insert(1, Arc::new(vec![1]));
        cache.insert(2, Arc::new(vec![2]));

        // This should evict slab 1
        cache.insert(3, Arc::new(vec![3]));

        assert!(cache.find(1).is_none());
        assert!(cache.find(2).is_some());
        assert!(cache.find(3).is_some());

        assert_eq!(cache.hits, 2);
        assert_eq!(cache.misses, 1);
    }

    #[test]
    fn test_access_updates_lru_order() {
        let mut cache = DataCache::new(2);

        cache.insert(1, Arc::new(vec![1]));
        cache.insert(2, Arc::new(vec![2]));

        // Access slab 1, making it the most recently used
        assert!(cache.find(1).is_some());

        // This should now evict slab 2 instead of 1
        cache.insert(3, Arc::new(vec![3]));

        assert!(cache.find(1).is_some());
        assert!(cache.find(2).is_none());
        assert!(cache.find(3).is_some());
    }

    #[test]
    fn test_update_existing_entry() {
        let mut cache = DataCache::new(2);

        let data1 = Arc::new(vec![1]);
        let data1_new = Arc::new(vec![10]);

        cache.insert(1, data1);
        cache.insert(1, data1_new.clone());

        let result = cache.find(1);
        assert_eq!(*result.unwrap(), *data1_new);
    }
}

//------------------------------------------------
