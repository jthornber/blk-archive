use anyhow::Result;
use chrono::prelude::*;
use clap::ArgMatches;
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use thinp::report::*;

use crate::config;

//-----------------------------------------

fn fmt_time(t: &chrono::DateTime<FixedOffset>) -> String {
    t.format("%b %d %y %H:%M").to_string()
}

pub fn run(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;

    env::set_current_dir(&archive_dir)?;

    let paths = fs::read_dir(&Path::new("./streams"))?;
    let stream_ids = paths
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str().map(String::from))
            })
        })
        .collect::<Vec<String>>();

    let mut streams = Vec::new();
    for id in stream_ids {
        let cfg = config::read_stream_config(&id)?;
        streams.push((id, config::to_date_time(&cfg.pack_time), cfg));
    }

    streams.sort_by(|l, r| l.1.partial_cmp(&r.1).unwrap());

    // calc size width
    let mut width = 0;
    for (_, _, cfg) in &streams {
        let txt = format!("{}", cfg.size);
        if txt.len() > width {
            width = txt.len();
        }
    }

    for (id, time, cfg) in streams {
        let source = cfg.name.unwrap();
        let size = cfg.size;
        report.info(&format!(
            "{} {:width$} {} {}",
            id,
            size,
            &fmt_time(&time),
            &source
        ));
    }

    Ok(())
}

//-----------------------------------------
