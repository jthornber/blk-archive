use anyhow::Result;

mod common;

use common::fixture::{create_input_file, verify_file, BLOCK_SIZE};
use common::random::Pattern;
use common::test_dir::*;

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
