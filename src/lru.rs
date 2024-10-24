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

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    AlreadyPresent,
    Added,
    AddAndEvict(u32),
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
        self.entries.push(Entry {
            n,
            prev: self.head,
            next: self.tail,
        });

        self.entries[self.head].next = index;
        self.entries[self.tail].prev = index;

        self.head = index;
    }

    fn lru_update_(&mut self, n: u32, index: usize) {
        let e = &mut self.entries[index];

        e.n = n;
        // There's nothing to do if the entry is already at the head
        if self.head != index {
            // When moving the entry from the tail to the head, only the
            // head and tail values need to change
            if self.tail == index {
                self.tail = e.next;
            } else {
                let prev = e.prev;
                let next = e.next;

                self.entries[prev].next = next;
                self.entries[next].prev = prev;

                self.entries[index].prev = self.head;
                self.entries[index].next = self.tail;
                self.entries[self.head].next = index;
                self.entries[self.tail].prev = index;
            }
            self.head = index;
        }
    }

    // Makes sure n is in the LRU, optionally returns an entry
    // that was evicted
    pub fn push(&mut self, n: u32) -> PushResult {
        use PushResult::*;

        let r = if let Some(index) = self.tree.get(&n).cloned() {
            // relink
            self.lru_update_(n, index);
            AlreadyPresent
        } else if self.entries.len() < self.capacity {
            // insert
            self.lru_push_(n);
            self.tree.insert(n, self.entries.len() - 1);
            Added
        } else {
            // evict and insert
            let index = self.tail;
            self.tail = self.entries[index].next;
            let evicted = self.entries[index].n;
            self.tree.remove(&evicted);
            self.lru_update_(n, index);
            self.tree.insert(n, index);
            AddAndEvict(evicted)
        };

        assert_eq!(self.entries.len(), self.tree.len());
        r
    }
}

#[cfg(test)]
mod lru_tests {
    use super::*;
    use PushResult::*;

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

        assert!(lru.push(54) == Added);
        for _ in 0..100 {
            assert!(lru.push(54) == AlreadyPresent);
        }
    }

    #[test]
    fn alternate_two_values() {
        let mut lru = LRU::with_capacity(2);

        assert_eq!(lru.push(1), Added);
        assert_eq!(lru.push(2), Added);
        for _ in 0..100 {
            assert_eq!(lru.push(1), AlreadyPresent);
            assert_eq!(lru.push(2), AlreadyPresent);
        }
    }

    #[test]
    fn alternate_three_values() {
        let mut lru = LRU::with_capacity(2);

        assert_eq!(lru.push(0), Added);
        assert_eq!(lru.push(1), Added);
        assert_eq!(lru.push(2), AddAndEvict(0));
        for _ in 0..100 {
            for i in 0..3 {
                assert_eq!(lru.push(i), AddAndEvict((i + 1) % 3));
            }
        }
    }

    #[test]
    fn relink() {
        let mut lru = LRU::with_capacity(3);

        assert_eq!(lru.push(0), Added);
        assert_eq!(lru.push(1), Added);
        assert_eq!(lru.push(100), Added);
        assert_eq!(lru.push(2), AddAndEvict(0));
        for _ in 0..100 {
            for i in 0..3 {
                assert_eq!(lru.push(i), AddAndEvict((i + 1) % 3));
                assert_eq!(lru.push(100), AlreadyPresent);
            }
        }
    }
}
