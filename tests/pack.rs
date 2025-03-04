use anyhow::Result;

mod common;

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
    let input = create_input_file(&mut td, file_size, seed, pattern)?;

    let (data_start, hashes_start) = archive.data_hash_sizes()?;
    let response = archive.pack(&input)?;
    let (data_end, hashes_end) = archive.data_hash_sizes()?;

    let data_written = data_end - data_start;
    let hashes_written = hashes_end - hashes_start;

    assert_eq!(data_written, response.stats.data_written);
    assert_eq!(hashes_written, response.stats.hashes_written);

    archive.verify(&input, &response.stream_id)?;
    Ok(response)
}

#[test]
fn pack_zero_verify_stats() -> Result<()> {
    let file_size = 16 * 1024 * 1024;
    let response = pack_common_verify_stats(file_size, Pattern::SingleByte(0))?;
    assert_eq!(response.stats.fill_size, file_size);
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
    let file_size = 16 * 1024 * 1024;
    pack_common_verify_stats(file_size, Pattern::LCG)?;
    Ok(())
}

//-----------------------------------------
