use std::collections::BTreeMap;

#[derive(Debug)]
struct Entry {
    n: u32,
    prev: usize,
    next: usize,
}

pub struct LRU {
    capacity: usize,
    entries: Vec<Entry>,
    head: usize,
    tail: usize,
    tree: BTreeMap<u32, usize>,
}

impl LRU {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::with_capacity(capacity),
            head: 0,
            tail: 0,
            tree: BTreeMap::new(),
        }
    }

    fn lru_push_(&mut self, n: u32) {
        let index = self.entries.len();
        self.entries.push(Entry {n, prev: self.head, next: self.tail });

        self.entries[self.head].next = index;
        self.entries[self.tail].prev = index;

        self.head = index;
        if index == 0 {
            self.tail = index;
        }
    }

    fn lru_add_(&mut self, n: u32, index: usize) {
        let e = &mut self.entries[index];
        e.n = n;
        e.prev = self.head;
        e.next = self.tail;

        self.entries[self.head].next = index;
        self.entries[self.tail].prev = index;

        self.head = index;
    }

    fn lru_del_(&mut self, index: usize) {
        let e = &mut self.entries[index];
        let prev = e.prev;
        let next = e.next;
        drop(e);

        if self.tail == index {
            self.tail = next;
        }
        if self.head == index {
            self.head = prev;
        }
        self.entries[prev].next = next;
        self.entries[next].prev = prev;
    }

    // Makes sure n is in the LRU, optionally returns an entry
    // that was evicted
    pub fn push(&mut self, n: u32) -> Option<u32> {
        let r = if let Some(index) = self.tree.get(&n).cloned() {
            // relink
            self.lru_del_(index);
            self.lru_add_(n, index);
            None
        } else {
            if self.entries.len() < self.capacity {
                // insert
                self.lru_push_(n);
                self.tree.insert(n, self.entries.len() - 1);
                None
            } else {
                // evict and insert
                let index = self.tail;
                self.tail = self.entries[index].next;
                let evicted = self.entries[index].n;
                self.tree.remove(&evicted);
                self.lru_del_(index);
                self.lru_add_(n, index);
                self.tree.insert(n, index);
                Some(evicted)
            }
        };

        assert_eq!(self.entries.len(), self.tree.len());
        r
    }
}

#[cfg(test)]
mod lru_tests {
    use super::*;

    #[allow(dead_code)]
    fn print_entries(lru: &LRU) {
        for (i, e) in lru.entries.iter().enumerate() {
            eprintln!("entry[{}] = {:?}", i, e);
        }
        eprintln!("tree = {:?}", lru.tree);
        eprintln!("head = {}, tail = {}", lru.head, lru.tail);
    }

    #[test]
    fn same_item_repeatedly_added() {
        let mut lru = LRU::with_capacity(1);

        assert!(lru.push(54).is_none());
        for _ in 0..100 {
            assert!(lru.push(54).is_none());
        }
    }

    #[test]
    fn alternate_two_values() {
        let mut lru = LRU::with_capacity(2);

        assert!(lru.push(1).is_none());
        assert!(lru.push(2).is_none());
        for _ in 0..100 {
            assert!(lru.push(1).is_none());
            assert!(lru.push(2).is_none());
        }
    }

    #[test]
    fn alternate_three_values() {
        let mut lru = LRU::with_capacity(2);

        assert!(lru.push(0).is_none());
        assert!(lru.push(1).is_none());
        assert_eq!(lru.push(2), Some(0));
        for _ in 0..100 {
            for i in 0..3 {
                assert_eq!(lru.push(i), Some((i + 1) % 3));
            }
        }
    }

    #[test]
    fn relink() {
        let mut lru = LRU::with_capacity(3);

        assert!(lru.push(0).is_none());
        assert!(lru.push(1).is_none());
        assert!(lru.push(100).is_none());
        assert_eq!(lru.push(2), Some(0));
        for _ in 0..100 {
            for i in 0..3 {
                assert_eq!(lru.push(i), Some((i + 1) % 3));
                assert_eq!(lru.push(100), None);
            }
        }
    }
}
