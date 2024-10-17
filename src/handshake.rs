use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone)]
pub struct HandShake {
    pair: Arc<(Mutex<bool>, Condvar)>,
}

impl Default for HandShake {
    fn default() -> Self {
        Self::new()
    }
}

impl HandShake {
    pub fn new() -> Self {
        Self {
            pair: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    pub fn done(&self) {
        let (lock, cvar) = &*self.pair;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_all();
    }

    pub fn wait(&self) {
        let (lock, cvar) = &*self.pair;
        let mut started = lock.lock().unwrap();

        while !*started {
            started = cvar.wait(started).unwrap();
        }
    }
}
