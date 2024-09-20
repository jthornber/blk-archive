use crate::stream::MapEntry;
use anyhow::Result;
use std::collections::BTreeMap;

// Creates order out of the unordered stream of map entries

pub struct Sentry {
    pub e: MapEntry,
    pub len: u64,
}

pub struct StreamOrder {
    seq_id: u64,
    next: u64,
    ready: BTreeMap<u64, Sentry>,
}

impl StreamOrder {
    pub fn new() -> Result<Self> {
        Ok(Self {
            seq_id: 0,
            next: 0,
            ready: BTreeMap::new(),
        })
    }

    pub fn entry_start(&mut self) -> u64 {
        let v = self.seq_id;
        self.seq_id += 1;
        v
    }

    pub fn entry_complete(&mut self, id: u64, e: MapEntry, len: u64) -> Result<()> {
        self.ready.insert(id, Sentry { e, len });
        Ok(())
    }

    pub fn remove(&mut self) -> Option<Sentry> {
        self.ready.remove(&self.next).map(|entry| {
            self.next += 1;
            entry
        })
    }

    pub fn is_complete(&self) -> bool {
        self.ready.is_empty() && (self.seq_id == self.next)
    }
}
