use anyhow::Result;

mod common;

use crate::common::random::Pattern;
use common::fixture::*;
use common::test_dir::*;

//-----------------------------------------

#[test]
fn unpack_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td, true)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed, Pattern::LCG)?;
    let stream = archive.pack(&input)?.stream_id;

    let output = td.mk_path("output.bin");
    archive.unpack(&stream, &output, true)?;
    verify_file(&output, file_size, seed, Pattern::LCG)
}

//-----------------------------------------
