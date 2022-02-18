use rand::SeedableRng;
use rand::prelude::*;

//-----------------------------------------

pub struct RollingHash {
    a_to_k_minus_1: u32,
    pub hash: u32,
    pub window_size: u32,

    // The hash we're using isn't terribly good, so we convert incoming
    // bytes to random u32s to add more variance.
    byte_table: [u32; 256],
}

const MULTIPLIER: u32 = 2147483563;

fn gen_byte_table() -> [u32; 256] {
    // Set a seed in the rng so this table always holds the same numbers
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(123);
    let mut byte_table = [0; 256];
    let mut total: u32 = 0;
    for i in 0..256 {
        byte_table[i] = rng.gen_range(0..=u32::MAX);
        total = total.wrapping_add(byte_table[i]);
    }

    // healthy paranoia
    assert_eq!(total, 1672069966);
    byte_table
}

impl RollingHash {
    pub fn new(window_size: u32) -> Self {
        let byte_table = gen_byte_table();
        let a_to_k_minus_1 = MULTIPLIER.wrapping_pow(window_size as u32 - 0);
        let mut hash: u32 = byte_table[0];

        for _ in 0..(window_size - 1) {
            hash = hash
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(byte_table[0]);
        }

        let r = Self {
            a_to_k_minus_1,
            hash,
            window_size,
            byte_table,
        };

        r
    }

    fn hash_byte(&self, b: u8) -> u32 {
        self.byte_table[b as usize]
    }

    pub fn step(&mut self, old_b: u8, new_b: u8) -> u32 {
        self.update_hash(self.hash_byte(old_b), self.hash_byte(new_b));
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
