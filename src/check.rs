use std::env;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::ArgMatches;

use crate::output::Output;
use crate::paths;
use crate::slab::*;

pub fn run(matches: &ArgMatches, _output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let repair = matches.is_present("REPAIR");

    env::set_current_dir(archive_dir.clone())?;

    let data_path = paths::data_path();
    let hashes_path = paths::hashes_path();

    let num_data_slabs = SlabFile::verify(data_path.clone(), repair)?;
    let num_hash_slabs = SlabFile::verify(hashes_path.clone(), repair)?;

    if num_data_slabs != num_hash_slabs {
        return Err(anyhow!(
            "Number of slab entries in data slab {num_data_slabs} \
            != {num_hash_slabs} in hashes file!"
        ));
    }

    Ok(())
}
