use anyhow::Result;

mod common;

use crate::common::random::Pattern;
use common::fixture::*;
use common::process::*;
use common::test_dir::*;

//-----------------------------------------

#[test]
fn verify_incompleted_archive_should_fail() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td, true)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed, Pattern::LCG)?;
    let input_short = create_input_file(&mut td, file_size - 1, seed, Pattern::LCG)?;
    let stream = archive.pack(&input_short)?.stream_id;
    run_fail(archive.verify_cmd(&input, &stream))?;
    Ok(())
}

#[test]
fn verify_incompleted_file_should_fail() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td, true)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed, Pattern::LCG)?;
    let input_short = create_input_file(&mut td, file_size - 1, seed, Pattern::LCG)?;
    let stream = archive.pack(&input)?.stream_id;
    run_fail(archive.verify_cmd(&input_short, &stream))?;
    Ok(())
}

//-----------------------------------------
