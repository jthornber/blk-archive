#[macro_export]
macro_rules! tpln {
    () => {
        print!("\n")
    };
    ($($arg:tt)*) => {{
        println!("{:?}[{}:{}] {}", ::std::thread::current().id(), file!(), line!(), format!($($arg)*));
    }};
}

pub fn round_pow2(i: u32) -> u64 {
    // Round up to the next power of 2
    // https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    let mut v = i as u64; // Using u64 to allow us to represent 2**32
    v += (v == 0) as u64;
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v += 1;
    v
}

pub fn is_pow2(v: usize) -> bool {
    // https://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
    v != 0 && ((v & (v - 1)) == 0)
}

#[cfg(test)]
mod util_tests {

    use super::*;

    #[test]
    fn test_roundup2pow2() {
        assert!(round_pow2(0) == 1);
        assert!(round_pow2(1) == 1);
        assert!(round_pow2(2) == 2);
        assert!(round_pow2(3) == 4);
        assert!(round_pow2(4) == 4);
        assert!(round_pow2(5) == 8);
        assert!(round_pow2(5) == 8);
        assert!(round_pow2(6) == 8);
        assert!(round_pow2(63) == 64);
        assert!(round_pow2(65) == 128);
        assert!(round_pow2(4096) == 4096);
        assert!(round_pow2(4096) == 4096);
        assert!(round_pow2(4097) == 8192);
        assert!(round_pow2(16385) == 32768);
        assert!(round_pow2(32769) == 65536);
        assert!(round_pow2(65537) == 131072);
        assert!(round_pow2(131072) == 131072);
        assert!(round_pow2(131073) == 262144);
        assert!(round_pow2(4294967295) == 4294967296);
    }

    #[test]
    fn test_is_pow2() {
        for i in 0..268435456 {
            let z = round_pow2(i);
            if i as u64 == z {
                assert!(is_pow2(i as usize) == true);
            } else {
                assert!(is_pow2(i as usize) == false);
            }
        }
    }
}
