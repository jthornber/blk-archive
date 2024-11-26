use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};

use crate::stream::MapEntry;

// Creates order out of the unordered stream of map entries

#[derive(Debug)]
pub struct Sentry {
    pub e: MapEntry,
    pub len: Option<u64>,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug)]
struct Protected {
    seq_id: u64,
    next: u64,
    ready: BTreeMap<u64, Sentry>,
}

#[derive(Debug)]
pub struct StreamOrder {
    pair: Arc<(Mutex<Protected>, Condvar)>,
}

impl std::clone::Clone for StreamOrder {
    fn clone(&self) -> Self {
        Self {
            pair: self.pair.clone(),
        }
    }
}

impl Default for StreamOrder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamOrder {
    pub fn new() -> Self {
        let p = Mutex::new(Protected {
            seq_id: 0,
            next: 0,
            ready: BTreeMap::new(),
        });

        Self {
            pair: Arc::new((p, Condvar::new())),
        }
    }

    fn _next(p: &mut Protected) -> u64 {
        let v = p.seq_id;
        p.seq_id += 1;
        v
    }

    pub fn entry_add(&self, e: MapEntry, len: Option<u64>, data: Option<Vec<u8>>) {
        let (protected, cvar) = &*self.pair;
        let mut p = protected.lock().unwrap();
        let next = Self::_next(&mut p);
        p.ready.insert(next, Sentry { e, len, data });
        cvar.notify_all();
    }

    pub fn entry_start(&self) -> u64 {
        let (protected, _cvar) = &*self.pair;

        let mut p = protected.lock().unwrap();
        Self::_next(&mut p)
    }

    pub fn entry_complete(&self, id: u64, e: MapEntry, len: Option<u64>, data: Option<Vec<u8>>) {
        let (protected, cvar) = &*self.pair;
        let mut p = protected.lock().unwrap();
        p.ready.insert(id, Sentry { e, len, data });
        cvar.notify_all();
    }

    fn remove(p: &mut Protected) -> Option<Sentry> {
        p.ready.remove(&p.next).inspect(|_| {
            p.next += 1;
        })
    }

    fn _complete(p: &Protected) -> bool {
        p.ready.is_empty() && (p.seq_id == p.next) && p.seq_id > 0
    }

    pub fn drain(&mut self, wait: bool) -> (Vec<Sentry>, bool) {
        let (protected, cvar) = &*self.pair;
        let mut p = protected.lock().unwrap();

        if wait {
            while p.ready.is_empty() {
                p = cvar.wait(p).unwrap();
            }
        }

        let mut rc = Vec::new();
        while let Some(e) = Self::remove(&mut p) {
            rc.push(e);
        }
        (rc, Self::_complete(&p))
    }

    pub fn is_complete(&self) -> bool {
        let (protected, _cvar) = &*self.pair;
        let p = protected.lock().unwrap();
        Self::_complete(&p)
    }
}
