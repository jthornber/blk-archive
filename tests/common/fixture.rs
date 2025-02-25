use anyhow::{anyhow, Result};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use thinp::file_utils::create_sized_file;

use crate::common::blk_archive::*;
use crate::common::block_visitor::*;
use crate::common::random::Pattern;
use crate::common::test_dir::*;

//-----------------------------------------

pub const BLOCK_SIZE: usize = 32768;

pub fn create_archive(td: &mut TestDir, data_compression: bool) -> Result<BlkArchive> {
    let archive_dir = td.mk_path("test_arch");
    BlkArchive::new_with(&archive_dir, 4096, data_compression)
}

pub fn create_input_file(
    td: &mut TestDir,
    size: u64,
    seed: u64,
    pattern: Pattern,
) -> Result<PathBuf> {
    let path = td.mk_path("input.bin");
    let file = create_sized_file(&path, size)?;
    let mut stamper = Stamper::new(file, seed, BLOCK_SIZE, pattern).len(size);
    stamper.stamp_file()?;
    Ok(path)
}

pub fn verify_file(path: &Path, size: u64, seed: u64, pattern: Pattern) -> Result<()> {
    let actual_size = std::fs::metadata(path)?.len();
    if actual_size != size {
        return Err(anyhow!(
            "unexpected file size {}, while expected {}",
            actual_size,
            size
        ));
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(path)?;
    let mut verifier = Verifier::new(file, seed, BLOCK_SIZE, pattern).len(size);
    verifier.verify()
}

#[test]
fn test_file_create_verify_repeating() -> Result<()> {
    let mut td = TestDir::new()?;
    let file_size = (BLOCK_SIZE * 10) as u64;
    let buffer: Vec<u8> = (0..BLOCK_SIZE).map(|x| x as u8).collect(); // Incrementing byte buffer
    let input = create_input_file(&mut td, file_size, 1, Pattern::Repeating(buffer.clone()))?;

    verify_file(&input, file_size, 1, Pattern::Repeating(buffer.clone()))?;

    Ok(())
}

#[test]
fn test_file_create_verify_fill() -> Result<()> {
    let mut td = TestDir::new()?;
    let file_size = (BLOCK_SIZE * 10) as u64;
    let input = create_input_file(&mut td, file_size, 1, Pattern::SingleByte(0xFF))?;

    verify_file(&input, file_size, 1, Pattern::SingleByte(0xFF))?;

    Ok(())
}

#[test]
fn test_file_create_verify_random() -> Result<()> {
    let mut td = TestDir::new()?;
    let file_size = (BLOCK_SIZE * 10) as u64;
    let input = create_input_file(&mut td, file_size, 1, Pattern::LCG)?;

    verify_file(&input, file_size, 1, Pattern::LCG)?;

    Ok(())
}
//-----------------------------------------
