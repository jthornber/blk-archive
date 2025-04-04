use anyhow::Result;
use rand::Rng;

mod common;

use blk_archive::archive;
use common::blk_archive::PackResponse;
use common::fixture::{create_archive, create_input_file, BLOCK_SIZE};
use common::random::Pattern;
use common::test_dir::*;

//-----------------------------------------

#[test]
fn pack_one_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td, true)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed, Pattern::LCG)?;
    let stream = archive.pack(&input)?.stream_id;
    archive.verify(&input, &stream)
}

#[test]
fn pack_same_file_multiple_times() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td, true)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed, Pattern::LCG)?;
    let streams = (0..3)
        .map(|_| archive.pack(&input))
        .collect::<Result<Vec<_>>>()?;
    streams
        .iter()
        .try_for_each(|s| archive.verify(&input, &s.stream_id))
}

fn pack_common_verify_stats(file_size: u64, pattern: Pattern) -> Result<PackResponse> {
    let mut td = TestDir::new()?;
    let seed = 1;

    let archive = create_archive(&mut td, false)?;
    let input = create_input_file(&mut td, file_size, seed, pattern.clone())?;
    let response = archive.pack(&input)?;

    assert_eq!(response.stats.mapped_size, file_size);

    match pattern {
        Pattern::LCG => {
            assert_eq!(file_size, response.stats.data_written);
            assert_eq!(response.stats.fill_size, 0);
        }
        Pattern::SingleByte(_a) => {
            assert_eq!(response.stats.data_written, 0);
            assert_eq!(response.stats.fill_size, file_size);
        }
        Pattern::Repeating(pattern) => {
            assert_eq!(response.stats.data_written, pattern.len() as u64);
            assert_eq!(response.stats.fill_size, 0);
        }
    }

    archive.verify(&input, &response.stream_id)?;
    Ok(response)
}

#[test]
fn pack_zero_verify_stats() -> Result<()> {
    let file_size = 16 * 1024 * 1024;
    pack_common_verify_stats(file_size, Pattern::SingleByte(0))?;
    Ok(())
}

#[test]
fn pack_duplicate_verify_stats() -> Result<()> {
    let file_size = 16 * 1024 * 1024;
    let buffer: Vec<u8> = (0..BLOCK_SIZE).map(|x| x as u8).collect(); // Incrementing byte buffer
    pack_common_verify_stats(file_size, Pattern::Repeating(buffer))?;
    Ok(())
}

#[test]
fn pack_random_verify_stats() -> Result<()> {
    let file_size_start = 16 * 1024 * 1024 as u64;
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        let r_increase = rng.gen_range(1024..archive::SLAB_SIZE_TARGET);
        let size = file_size_start + r_increase as u64;

        pack_common_verify_stats(size, Pattern::LCG)?;
    }

    for s in vec![
        file_size_start,
        file_size_start - 10,
        file_size_start + archive::SLAB_SIZE_TARGET as u64 + 10,
    ] {
        pack_common_verify_stats(s, Pattern::LCG)?;
    }

    Ok(())
}
//-----------------------------------------
