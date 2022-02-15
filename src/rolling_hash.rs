/*
use io::Write;
use rand::prelude::*;
use rand::prelude::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::exit;
use std::sync::mpsc::{sync_channel, Receiver};
use thinp::commands::utils::*;
use thinp::report::*;
*/

//-----------------------------------------

pub struct RollingHash {
    a_to_k_minus_1: u32,
    pub hash: u32,
    pub window_size: u32,
}

const MULTIPLIER: u32 = 2147483563;

impl RollingHash {
    pub fn new(window_size: u32) -> Self {
        let a_to_k_minus_1 = MULTIPLIER.wrapping_pow(window_size as u32 - 0);
        let mut hash: u32 = RollingHash::hash_byte(0);

        for _ in 0..(window_size - 1) {
            hash = hash
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(RollingHash::hash_byte(0));
        }

        let r = Self {
            a_to_k_minus_1,
            hash,
            window_size,
        };

        r
    }

    fn hash_byte(b: u8) -> u32 {
        if b == 0 {
            12345
        } else {
            b as u32
        }
    }

    pub fn step(&mut self, old_b: u8, new_b: u8) -> u32 {
        self.update_hash(RollingHash::hash_byte(old_b), RollingHash::hash_byte(new_b));
        self.hash
    }

    fn update_hash(&mut self, old_b: u32, new_b: u32) {
        // H2 = [(a . H1) + new_b - (a_to_k_minus_1 * old_b)] % 2^32
        let clause1 = self.hash.wrapping_mul(MULTIPLIER);
        let clause2 = new_b;
        let clause3 = self.a_to_k_minus_1.wrapping_mul(old_b);
        self.hash = clause1.wrapping_add(clause2).wrapping_sub(clause3);
    }
}

#[cfg(test)]
mod rolling_tests {
    use super::*;

    // Returns the final hash
    fn hash_bytes(bytes: &[u8], window: usize) -> u32 {
        let mut rhash = RollingHash::new(window as u32);

        for i in 0..bytes.len() {
            let h;
            if i >= window {
                h = rhash.step(bytes[i - window], bytes[i]);
            } else {
                h = rhash.step(0, bytes[i]);
            }

            println!(
                "{}: {}, {}",
                bytes[i] as char,
                h,
                (h >> 16) % (window as u32)
            );
        }

        rhash.hash
    }

    #[test]
    fn rolling1() {
        const WINDOW: usize = 16;
        let bytes1 = b"the quick brown fox jumps over the lazy dog";
        let bytes2 = b"lthe quick brown fax jumps over the lazy dog";

        let h1 = hash_bytes(&bytes1[..], WINDOW);
        let h2 = hash_bytes(&bytes2[..], WINDOW);
        assert_eq!(h1, h2);
    }
}

//-----------------------------------------
