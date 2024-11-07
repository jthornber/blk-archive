use std::sync::{Arc, Condvar, Mutex};

use crate::wire;

struct Package {
    done: bool,
    contents: Option<wire::Rpc>,
}

#[derive(Clone)]
pub struct HandShake {
    pair: Arc<(Mutex<Package>, Condvar)>,
}

impl Default for HandShake {
    fn default() -> Self {
        Self::new()
    }
}

impl HandShake {
    pub fn new() -> Self {
        let p = Mutex::new(Package {
            done: false,
            contents: None,
        });
        Self {
            pair: Arc::new((p, Condvar::new())),
        }
    }

    pub fn done(&self, rpc: Option<wire::Rpc>) {
        let (lock, cvar) = &*self.pair;
        let mut started = lock.lock().unwrap();
        started.done = true;
        started.contents = rpc;
        cvar.notify_all();
    }

    pub fn wait(&self) -> Option<wire::Rpc> {
        let (lock, cvar) = &*self.pair;
        let mut started = lock.lock().unwrap();

        while !started.done {
            started = cvar.wait(started).unwrap();
        }
        started.contents.take()
    }
}
