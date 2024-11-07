use serde_json::json;
use serde_json::to_string_pretty;

use anyhow::Result;
use clap::ArgMatches;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::client;
use crate::output::Output;
use crate::stream_meta;
use crate::wire;

//-----------------------------------------

fn fmt_time(t: &str) -> String {
    let t = stream_meta::to_date_time(t);
    t.format("%b %d %y %H:%M").to_string()
}

pub fn streams_get(dir: &Path) -> Result<Vec<(String, String, stream_meta::StreamConfig)>> {
    let paths = fs::read_dir(dir)?;
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
        let cfg = stream_meta::read_stream_config(&id)?;
        streams.push((id, cfg.pack_time.clone(), cfg));
    }

    streams.sort_by(|l, r| l.1.partial_cmp(&r.1).unwrap());
    Ok(streams)
}

pub fn run(matches: &ArgMatches, output: Arc<Output>) -> Result<()> {
    let streams = if matches.contains_id("LIST_ARCHIVE") {
        let archive_dir =
            Path::new(matches.get_one::<String>("LIST_ARCHIVE").unwrap()).canonicalize()?;
        env::set_current_dir(&archive_dir)?;
        streams_get(&PathBuf::from_str("./streams").unwrap())?
    } else {
        let server = matches.get_one::<String>("LIST_SERVER").unwrap();
        let response = client::one_rpc(server, wire::Rpc::ArchiveListReq(0))?.unwrap();
        if let wire::Rpc::ArchiveListResp(_id, streams) = response {
            streams
        } else {
            panic!("We are expecting a result from wire::Rpc::ArchiveListResp");
        }
    };

    if output.json {
        let mut j_output = Vec::new();
        for (id, time, cfg) in streams {
            let source = cfg.name.unwrap();
            let size = cfg.size;
            j_output.push(json!(
                {"stream_id": id, "size": size, "time": time, "source": source}
            ));
        }

        println!("{}", to_string_pretty(&j_output).unwrap());
    } else {
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
            output.report.to_stdout(&format!(
                "{} {:width$} {} {}",
                id,
                size,
                &fmt_time(&time),
                &source
            ));
        }
    }
    Ok(())
}

//-----------------------------------------
