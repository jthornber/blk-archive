use blake2::{Blake2b, Digest};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

use crate::iovec::*;

//-----------------------------------------

type Blake2b32 = Blake2b<generic_array::typenum::U4>;
type Blake2b64 = Blake2b<generic_array::typenum::U8>;
type Blake2b256 = Blake2b<generic_array::typenum::U32>;

pub type Hash32 = generic_array::GenericArray<u8, generic_array::typenum::U4>;
pub type Hash64 = generic_array::GenericArray<u8, generic_array::typenum::U8>;
pub type Hash256 = generic_array::GenericArray<u8, generic_array::typenum::U32>;

pub fn hash_256_iov(iov: &IoVec) -> Hash256 {
    let mut hasher = Blake2b256::new();
    for v in iov {
        hasher.update(&v[..]);
    }
    hasher.finalize()
}

pub fn hash_64_iov(iov: &IoVec) -> Hash64 {
    let mut hasher = Blake2b64::new();
    for v in iov {
        hasher.update(&v[..]);
    }
    hasher.finalize()
}

pub fn hash_32_iov(iov: &IoVec) -> Hash32 {
    let mut hasher = Blake2b32::new();
    for v in iov {
        hasher.update(&v[..]);
    }
    hasher.finalize()
}

pub fn hash_256(v: &[u8]) -> Hash256 {
    let mut hasher = Blake2b256::new();
    hasher.update(v);
    hasher.finalize()
}

pub fn hash_64(v: &[u8]) -> Hash64 {
    let mut hasher = Blake2b64::new();
    hasher.update(v);
    hasher.finalize()
}

pub fn hash_32(v: &[u8]) -> Hash32 {
    let mut hasher = Blake2b32::new();
    hasher.update(v);
    hasher.finalize()
}

pub fn hash_le_u64(h: &[u8]) -> u64 {
    let mini_hash = hash_64(h);
    let mut c = Cursor::new(&mini_hash);
    c.read_u64::<LittleEndian>().unwrap()
}

pub fn hash256_to_bytes(h: &Hash256) -> [u8; 32] {
    let mut rc = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        rc[i] = *v;
    }
    rc
}

pub fn bytes_to_hash256(v: &[u8; 32]) -> &Hash256 {
    Hash256::from_slice(&v[..])
}

//-----------------------------------------
#[test]
fn to_wire() {
    let bytes = [0, 128];

    // TODO: Maybe there is a better way to simply serialize/deserialize for the wire for a Hash256
    let reference = hash_256(&bytes);
    let as_bytes_rep = hash256_to_bytes(&reference);
    let converted = bytes_to_hash256(&as_bytes_rep);

    assert_eq!(reference, *converted);
}
