use anyhow::{anyhow, Result};
use blake2::{Blake2b, Digest};
use byteorder::{LittleEndian, WriteBytesExt};
use generic_array::typenum::U8;
use nom::IResult;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use std::cmp;
use std::iter::*;
use std::path::Path;

use crate::slab::*;
use crate::utils::is_pow2;

const ENTRIES_PER_BUCKET: usize = 4;
const MAX_KICKS: usize = 500;

#[derive(Clone)]
struct Bucket {
    entries: [u16; ENTRIES_PER_BUCKET],
    slabs: [u32; ENTRIES_PER_BUCKET],
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            entries: [0; ENTRIES_PER_BUCKET],
            slabs: [0; ENTRIES_PER_BUCKET],
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum InsertResult {
    PossiblyPresent(u32),
    Inserted,
}

pub struct CuckooFilter {
    rng: ChaCha20Rng,
    len: usize,
    scatter: Vec<usize>,
    bucket_counts: Vec<u8>,
    buckets: Vec<Bucket>,
    mask: usize,
}

fn parse_bucket(input: &[u8], nr: usize) -> IResult<&[u8], Bucket> {
    use nom::multi::*;
    use nom::number::complete::*;

    let (input, entries) = count(le_u16, nr)(input)?;
    let (input, slabs) = count(le_u32, nr)(input)?;
    let mut b = Bucket::default();
    for (i, (e, s)) in zip(entries, slabs).enumerate() {
        b.entries[i] = e;
        b.slabs[i] = s;
    }
    Ok((input, b))
}

fn parse_counts(input: &[u8], nr: usize) -> IResult<&[u8], Vec<u8>> {
    use nom::multi::*;
    use nom::number::complete::*;

    count(le_u8, nr)(input)
}

fn parse_buckets<'a>(mut input: &'a [u8], counts: &[u8]) -> IResult<&'a [u8], Vec<Bucket>> {
    let mut buckets: Vec<Bucket> = Vec::with_capacity(counts.len());

    for c in counts {
        let (inp, bucket) = parse_bucket(input, *c as usize)?;
        buckets.push(bucket);
        input = inp;
    }

    Ok((input, buckets))
}

fn parse_nr(input: &[u8]) -> IResult<&[u8], u32> {
    nom::number::complete::le_u32(input)
}

pub fn calculate_signature(values: &[usize]) -> u64 {
    let hash_bytes: Vec<u8> = values
        .iter()
        .flat_map(|&v| v.to_le_bytes().to_vec())
        .collect();

    let mut hasher = Blake2b::<U8>::new();
    hasher.update(&hash_bytes);

    u64::from_le_bytes(hasher.finalize().into())
}

impl CuckooFilter {
    fn make_scatter(rng: &mut ChaCha20Rng) -> Vec<usize> {
        let scatter: Vec<usize> = repeat_with(|| rng.gen())
            .take(u16::MAX as usize + 1)
            .collect();

        // Ensure that the scatter is identical every time it's constructed
        // We cannot use the DefaultHasher as it's documented to not be consistent across
        // versions/time
        assert!(4224213928824907068 == calculate_signature(scatter.as_slice()));

        scatter
    }

    pub fn with_capacity(mut n: usize) -> Self {
        n = (n * 5) / 4;
        n /= ENTRIES_PER_BUCKET;
        let mut rng = ChaCha20Rng::seed_from_u64(1);
        let nr_buckets = cmp::max(n, 4096).next_power_of_two();
        let scatter = Self::make_scatter(&mut rng);
        Self {
            rng,
            len: 0,
            scatter,
            bucket_counts: vec![0; nr_buckets],
            buckets: vec![Bucket::default(); nr_buckets],
            mask: nr_buckets - 1,
        }
    }

    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        // all the data goes in a single slab
        let mut file = SlabFileBuilder::open(path).build()?;
        let input = file.read(0)?;

        let mut rng = ChaCha20Rng::seed_from_u64(1);

        let (input, nr_buckets) = parse_nr(&input[..]).map_err(|_| anyhow!("couldn't parse nr"))?;
        let nr_buckets = nr_buckets as usize;
        let (input, bucket_counts) =
            parse_counts(input, nr_buckets).map_err(|_| anyhow!("couldn't parse counts"))?;
        let (input, buckets) =
            parse_buckets(input, &bucket_counts).map_err(|_| anyhow!("couldn't parse buckets"))?;

        if !input.is_empty() {
            // FIXME: throwing here causes a hang, presumably waiting for threads.
            return Err(anyhow!("extra bytes at end of index file"));
        }

        let scatter = Self::make_scatter(&mut rng);
        if !is_pow2(nr_buckets) {
            return Err(anyhow!("nr_buckets({nr_buckets}) is not a power of 2"));
        }
        let mask = nr_buckets - 1;
        let len = bucket_counts.iter().map(|n| *n as usize).sum();

        Ok(Self {
            rng,
            len,
            scatter,
            bucket_counts,
            buckets,
            mask,
        })
    }

    pub fn write<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut out: Vec<u8> = Vec::new();

        out.write_u32::<LittleEndian>(self.bucket_counts.len() as u32)?;
        for i in &self.bucket_counts {
            out.write_u8(*i)?;
        }

        for (b, count) in self.bucket_counts.iter().enumerate() {
            for e in 0..*count {
                out.write_u16::<LittleEndian>(self.buckets[b].entries[e as usize])?;
            }

            for s in 0..*count {
                out.write_u32::<LittleEndian>(self.buckets[b].slabs[s as usize])?;
            }
        }

        let mut file = SlabFileBuilder::create(path)
            .queue_depth(1)
            .compressed(false)
            .build()?;
        file.write_slab(&out)?;
        file.close()?;

        Ok(())
    }

    pub fn capacity(&self) -> usize {
        (self.buckets.len() * ENTRIES_PER_BUCKET * 4) / 5
    }

    fn present(&self, fp: u16, index: usize) -> Option<u32> {
        for entry in 0..self.bucket_counts[index] as usize {
            if self.buckets[index].entries[entry] == fp {
                return Some(self.buckets[index].slabs[entry]);
            }
        }

        None
    }

    fn insert(&mut self, fp: u16, slab: u32, index: usize) -> bool {
        let entry = self.bucket_counts[index] as usize;
        if entry >= ENTRIES_PER_BUCKET {
            false
        } else {
            self.buckets[index].entries[entry] = fp;
            self.buckets[index].slabs[entry] = slab;
            self.bucket_counts[index] += 1;
            true
        }
    }

    // h must be randomly distributed across u64. Does not overwrite
    // slab if there's already an entry.
    fn test_and_set_(&mut self, h: u64, mut slab: u32) -> Result<InsertResult> {
        use InsertResult::*;

        let mut fingerprint: u16 = (h & 0xffff) as u16;
        let index1: usize = ((h >> 16) as usize) & self.mask;

        if let Some(s) = self.present(fingerprint, index1) {
            return Ok(PossiblyPresent(s));
        }

        let index2: usize = (index1 ^ self.scatter[fingerprint as usize]) & self.mask;
        if let Some(s) = self.present(fingerprint, index2) {
            return Ok(PossiblyPresent(s));
        }

        if self.insert(fingerprint, slab, index1) {
            return Ok(Inserted);
        }

        if self.insert(fingerprint, slab, index2) {
            return Ok(Inserted);
        }

        let mut i = if self.rng.gen() { index1 } else { index2 };

        for _ in 0..MAX_KICKS {
            // randomly select entry from bucket i
            let entry = self.rng.gen_range(0..self.bucket_counts[i]);

            // swap with fp
            std::mem::swap(
                &mut fingerprint,
                &mut self.buckets[i].entries[entry as usize],
            );
            std::mem::swap(&mut slab, &mut self.buckets[i].slabs[entry as usize]);

            // i = i ^ hash(new fp)
            i = (i ^ self.scatter[fingerprint as usize]) & self.mask;

            if self.bucket_counts[i] < (ENTRIES_PER_BUCKET as u8) {
                self.insert(fingerprint, slab, i);
                return Ok(Inserted);
            }
        }

        Err(anyhow!("cuckoo table full"))
    }

    pub fn test(&mut self, h: u64) -> Result<InsertResult> {
        use InsertResult::*;

        let fingerprint: u16 = (h & 0xffff) as u16;
        let index1: usize = ((h >> 16) as usize) & self.mask;

        if let Some(s) = self.present(fingerprint, index1) {
            return Ok(PossiblyPresent(s));
        }

        let index2: usize = (index1 ^ self.scatter[fingerprint as usize]) & self.mask;
        if let Some(s) = self.present(fingerprint, index2) {
            return Ok(PossiblyPresent(s));
        }
        Ok(Inserted)
    }

    pub fn test_and_set(&mut self, h: u64, slab: u32) -> Result<InsertResult> {
        let r = self.test_and_set_(h, slab)?;
        if r == InsertResult::Inserted {
            self.len += 1;
        }
        Ok(r)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
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
        let mut cf = CuckooFilter::with_capacity(12_000);
        let mut rng = rand::thread_rng();
        let mut values = BTreeSet::new();

        // Generate a set of values to insert
        for _ in 0..10_000 {
            let n = rng.gen_range(0..u64::MAX);
            values.insert(n);
        }

        // Inserts
        for v in &values {
            match cf.test_and_set(*v, *v as u32).expect("test_and_set failed") {
                InsertResult::Inserted => {
                    // Expected
                }
                InsertResult::PossiblyPresent(n) => {
                    // Can happen due to false positives
                    eprintln!("already present {}", n);
                }
            }
        }

        // Lookups
        let mut hits = 0;
        let mut misses = 0;
        for v in &values {
            match cf.test_and_set(*v, *v as u32).expect("test_and_set failed") {
                InsertResult::Inserted => {
                    assert!(false);
                }
                InsertResult::PossiblyPresent(slab) => {
                    if slab == *v as u32 {
                        hits += 1;
                    } else {
                        misses += 1;
                    }
                }
            }
        }

        let false_positives = misses as f64 / hits as f64;
        eprintln!("false positives: {}", false_positives);
        assert!(false_positives < 0.001);
    }
}
