use anyhow::Result;

mod common;

use common::fixture::*;
use common::test_dir::*;

//-----------------------------------------

#[test]
fn pack_one_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed)?;
    let stream = archive.pack(&input)?;
    archive.verify(&input, &stream)
}

#[test]
fn pack_same_file_multiple_times() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed)?;
    let streams = (0..3)
        .map(|_| archive.pack(&input))
        .collect::<Result<Vec<_>>>()?;
    streams.iter().try_for_each(|s| archive.verify(&input, s))
}

//-----------------------------------------
