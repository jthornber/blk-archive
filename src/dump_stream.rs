use anyhow::Result;
use clap::ArgMatches;
use std::env;
use std::path::Path;
use std::sync::Arc;
use thinp::report::*;

use crate::stream::*;

//-----------------------------------------

pub fn run(matches: &ArgMatches, _report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let stream = matches.value_of("STREAM").unwrap();

    env::set_current_dir(&archive_dir)?;

    let mut d = Dumper::new(&stream)?;
    d.dump()
}

//-----------------------------------------
