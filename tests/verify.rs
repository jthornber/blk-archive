use anyhow::Result;

mod common;

use common::fixture::*;
use common::process::*;
use common::test_dir::*;

//-----------------------------------------

#[test]
fn verify_incompleted_archive_should_fail() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed)?;
    let input_short = create_input_file(&mut td, file_size - 1, seed)?;
    let stream = archive.pack(&input_short)?;
    run_fail(archive.verify_cmd(&input, &stream))?;
    Ok(())
}

#[test]
fn verify_incompleted_file_should_fail() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed)?;
    let input_short = create_input_file(&mut td, file_size - 1, seed)?;
    let stream = archive.pack(&input)?;
    run_fail(archive.verify_cmd(&input_short, &stream))?;
    Ok(())
}

//-----------------------------------------
