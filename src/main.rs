use anyhow::Result;
use clap::{command, Arg, ArgAction, ArgMatches, Command};
use std::env;
use std::process::exit;
use std::sync::Arc;
use thinp::report::*;

use blk_archive::create;
use blk_archive::dump_stream;
use blk_archive::list;
use blk_archive::output::Output;
use blk_archive::pack;
use blk_archive::unpack;

//-----------------------

fn mk_report(matches: &ArgMatches) -> Arc<Report> {
    if matches.get_flag("JSON") {
        Arc::new(mk_quiet_report())
    } else if atty::is(atty::Stream::Stdout) {
        Arc::new(mk_progress_bar_report())
    } else {
        Arc::new(mk_simple_report())
    }
}

fn main_() -> Result<()> {
    let archive_arg = Arg::new("ARCHIVE")
        .help("Specify archive directory")
        .long("archive")
        .short('a')
        .value_name("ARCHIVE")
        .num_args(1)
        .env("BLK_ARCHIVE_DIR")
        .required(true);

    let stream_arg = Arg::new("STREAM")
        .help("Specify an archived stream to unpack")
        .required(true)
        .long("stream")
        .short('s')
        .value_name("STREAM")
        .num_args(1);

    let json: Arg = Arg::new("JSON")
        .help("Output JSON")
        .required(false)
        .long("json")
        .short('j')
        .action(ArgAction::SetTrue)
        .global(true);

    let data_cache_size: Arg = Arg::new("DATA_CACHE_SIZE_MEG")
        .help("Specify how much memory is used for caching data")
        .required(false)
        .long("data-cache-size-meg")
        .value_name("DATA_CACHE_SIZE_MEG")
        .num_args(1);

    let matches = command!()
        .arg(json)
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("create")
                .about("creates a new archive")
                // We don't want to take a default from the env var, so can't use
                // archive_arg
                .arg(
                    Arg::new("ARCHIVE")
                        .help("Specify archive directory")
                        .required(true)
                        .long("archive")
                        .short('a')
                        .value_name("ARCHIVE")
                        .num_args(1),
                )
                .arg(
                    Arg::new("BLOCK_SIZE")
                        .help("Specify the average block size used when deduplicating data")
                        .required(false)
                        .long("block-size")
                        .value_name("BLOCK_SIZE")
                        .num_args(1),
                )
                .arg(
                    Arg::new("HASH_CACHE_SIZE_MEG")
                        .help("Specify how much memory is used for caching hash entries")
                        .required(false)
                        .long("hash-cache-size-meg")
                        .value_name("HASH_CACHE_SIZE_MEG")
                        .num_args(1),
                )
                .arg(data_cache_size.clone())
                .arg(
                    Arg::new("DATA_COMPRESSION")
                        .long("data-compression")
                        .value_name("y|n")
                        .help("Enable or disable slab data compression")
                        .value_parser(["y", "n"]) // Restrict values
                        .default_value("y")
                        .action(ArgAction::Set),
                ),
        )
        .subcommand(
            Command::new("pack")
                .about("packs a stream into the archive")
                .arg(
                    Arg::new("INPUT")
                        .help("Specify a device or file to archive")
                        .required(true)
                        .value_name("INPUT")
                        .num_args(1),
                )
                .arg(archive_arg.clone())
                .arg(
                    Arg::new("DELTA_STREAM")
                        .help(
                            "Specify the stream that contains an older version of this thin device",
                        )
                        .required(false)
                        .long("delta-stream")
                        .value_name("DELTA_STREAM")
                        .num_args(1),
                )
                .arg(
                    Arg::new("DELTA_DEVICE")
                        .help(
                            "Specify the device that contains an older version of this thin device",
                        )
                        .required(false)
                        .long("delta-device")
                        .value_name("DELTA_DEVICE")
                        .num_args(1),
                )
                .arg(data_cache_size.clone()),
        )
        .subcommand(
            Command::new("unpack")
                .about("unpacks a stream from the archive")
                .arg(
                    Arg::new("OUTPUT")
                        .help("Specify a device or file as the destination")
                        .required(true)
                        .value_name("OUTPUT")
                        .index(1),
                )
                .arg(
                    Arg::new("CREATE")
                        .help("Create a new file rather than unpack to an existing device/file.")
                        .long("create")
                        .action(clap::ArgAction::SetTrue),
                )
                .arg(data_cache_size.clone())
                .arg(archive_arg.clone())
                .arg(stream_arg.clone()),
        )
        .subcommand(
            Command::new("verify")
                .about("verifies stream in the archive against the original file/dev")
                .arg(
                    Arg::new("INPUT")
                        .help("Specify a device or file containing the correct version of the data")
                        .required(true)
                        .value_name("INPUT")
                        .num_args(1),
                )
                .arg(data_cache_size.clone())
                .arg(archive_arg.clone())
                .arg(stream_arg.clone()),
        )
        .subcommand(
            Command::new("dump-stream")
                .about("dumps stream instructions (development tool)")
                .arg(archive_arg.clone())
                .arg(stream_arg.clone()),
        )
        .subcommand(
            Command::new("list")
                .about("lists the streams in the archive")
                .arg(archive_arg.clone()),
        )
        .get_matches();

    let report = mk_report(&matches);
    report.set_level(LogLevel::Info);
    let output = Arc::new(Output {
        report: report.clone(),
        json: matches.get_flag("JSON"),
    });
    match matches.subcommand() {
        Some(("create", sub_matches)) => {
            create::run(sub_matches, report)?;
        }
        Some(("pack", sub_matches)) => {
            pack::run(sub_matches, output)?;
        }
        Some(("unpack", sub_matches)) => {
            unpack::run_unpack(sub_matches, output)?;
        }
        Some(("verify", sub_matches)) => {
            unpack::run_verify(sub_matches, output)?;
        }
        Some(("list", sub_matches)) => {
            list::run(sub_matches, output)?;
        }
        Some(("dump-stream", sub_matches)) => {
            dump_stream::run(sub_matches, output)?;
        }
        _ => unreachable!("Exhausted list of subcommands and subcommand_required prevents 'None'"),
    }

    Ok(())
}

fn main() {
    let code = match main_() {
        Ok(()) => 0,
        Err(e) => {
            // FIXME: write to report
            eprintln!("{:?}", e);
            // We don't print out the error since -q may be set
            1
        }
    };

    exit(code)
}

//-----------------------
