use anyhow::Result;
use clap::ArgMatches;
use std::env;
use std::path::Path;
use std::sync::Arc;

use crate::output::Output;
use crate::stream::*;

//-----------------------------------------

pub fn run(matches: &ArgMatches, output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    let stream = matches.get_one::<String>("STREAM").unwrap();

    env::set_current_dir(archive_dir)?;

    let mut d = Dumper::new(stream)?;
    d.dump(output)
}

//-----------------------------------------
