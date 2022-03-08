use anyhow::{anyhow, Result};
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use std::cmp;
use std::iter::*;

const ENTRIES_PER_BUCKET: usize = 4;
const MAX_KICKS: usize = 500;

#[derive(Clone)]
struct Bucket {
    entries: [u8; ENTRIES_PER_BUCKET],
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            entries: [0; ENTRIES_PER_BUCKET],
        }
    }
}

pub struct CuckooFilter {
    rng: ChaCha20Rng,
    len: usize,
    scatter: Vec<usize>,
    bucket_counts: Vec<u8>,
    buckets: Vec<Bucket>,
    mask: usize,
}

impl CuckooFilter {
    pub fn with_capacity(n: usize) -> Self {
        let mut rng = ChaCha20Rng::seed_from_u64(1);
        let nr_buckets = cmp::max(n, 4096).next_power_of_two();
        let scatter: Vec<usize> = repeat_with(|| rng.gen()).take(256).collect();
        Self {
            rng,
            len: 0,
            scatter,
            bucket_counts: vec![0; nr_buckets],
            buckets: vec![Bucket::default(); nr_buckets],
            mask: nr_buckets - 1,
        }
    }

    fn present(&self, fp: u8, index: usize) -> bool {
        for entry in 0..self.bucket_counts[index] as usize {
            if self.buckets[index].entries[entry] == fp {
                return true;
            }
        }

        false
    }

    fn insert(&mut self, fp: u8,  index: usize) -> bool {
        let entry = self.bucket_counts[index] as usize;
        if entry >= ENTRIES_PER_BUCKET {
            false
        } else {
            self.buckets[index].entries[entry] = fp;
            self.bucket_counts[index] += 1;
            true
        }
    }

    pub fn contains(&self, h: u64) -> bool {
        let fingerprint: u8 = (h & 0b1111111) as u8;
        let index1: usize = ((h >> 8) as usize) & self.mask;

        if self.present(fingerprint, index1) {
            return true;
        }

        let index2: usize = ((index1 ^ self.scatter[fingerprint as usize]) as usize) & self.mask;
        if self.present(fingerprint, index2) {
            return true;
        }

        false
    }

    // h must be randomly distributed across u64
    fn test_and_set_(&mut self, h: u64) -> Result<bool> {
        let mut fingerprint: u8 = (h & 0b1111111) as u8;
        let index1: usize = ((h >> 8) as usize) & self.mask;

        if self.present(fingerprint, index1) {
            return Ok(false);
        }

        let index2: usize = ((index1 ^ self.scatter[fingerprint as usize]) as usize) & self.mask;
        if self.present(fingerprint, index2) {
            return Ok(false);
        }

        if self.insert(fingerprint, index1) {
            return Ok(true);
        }

        if self.insert(fingerprint, index2) {
            return Ok(true);
        }

        let mut i = if self.rng.gen() { index1 } else { index2 };

        for _ in 0..MAX_KICKS {
            // randomly select entry from bucket i
            let entry = self.rng.gen_range(0..self.bucket_counts[i]);

            // swap with fp
            std::mem::swap(&mut fingerprint, &mut self.buckets[i].entries[entry as usize]);

            // i = i ^ hash(new fp)
            i = (i ^ self.scatter[fingerprint as usize]) & self.mask;

            if self.bucket_counts[i] < (ENTRIES_PER_BUCKET as u8) {
                self.insert(fingerprint, i);
                return Ok(true);
            }
        }

        Err(anyhow!("cuckoo table full"))
    }

    pub fn test_and_set(&mut self, h: u64) -> Result<bool> {
        if self.test_and_set_(h)? {
            self.len += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod cuckoo_tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_create() {
        let _cf = CuckooFilter::with_capacity(1000_000);
    }

    #[test]
    fn test_insert() {
        let mut cf = CuckooFilter::with_capacity(2000);
        let mut rng = rand::thread_rng();
        let mut inserted = BTreeSet::new();
        for _ in 0..10_000 {
            let n = rng.gen_range(0..100_000);
            if cf.test_and_set(n).expect("test_and_set failed") {
                assert!(!inserted.contains(&n));
                inserted.insert(n);
            } else {
                // False positive means we can't check inserted
            }
        }

	assert_eq!(cf.len(), inserted.len());
        for n in inserted {
            assert!(cf.contains(n));
        }
    }
}
