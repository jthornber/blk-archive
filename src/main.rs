use anyhow::Result;
use clap::{command, Arg, ArgMatches, Command, SubCommand};
use std::env;
use std::process::exit;
use std::sync::Arc;
use thinp::report::*;

use blk_archive::check;
use blk_archive::create;
use blk_archive::dump_stream;
use blk_archive::list;
use blk_archive::output::Output;
use blk_archive::pack;
use blk_archive::unpack;

//-----------------------

fn mk_report(matches: &ArgMatches) -> Arc<Report> {
    if matches.is_present("JSON") {
        Arc::new(mk_quiet_report())
    } else if atty::is(atty::Stream::Stdout) {
        Arc::new(mk_progress_bar_report())
    } else {
        Arc::new(mk_simple_report())
    }
}

fn main_() -> Result<()> {
    let default_archive = match env::var("BLK_ARCHIVE_DIR") {
        Err(_) => String::new(),
        Ok(s) => s,
    };

    let archive_arg = if default_archive.is_empty() {
        Arg::new("ARCHIVE")
            .help("Specify archive directory")
            .required(true)
            .long("archive")
            .short('a')
            .value_name("ARCHIVE")
            .takes_value(true)
    } else {
        Arg::new("ARCHIVE")
            .help("Specify archive directory")
            .default_value(&default_archive)
            .long("archive")
            .short('a')
            .value_name("ARCHIVE")
            .takes_value(true)
    };

    let stream_arg = Arg::new("STREAM")
        .help("Specify an archived stream to unpack")
        .required(true)
        .long("stream")
        .short('s')
        .value_name("STREAM")
        .takes_value(true);

    let json: Arg = Arg::new("JSON")
        .help("Output JSON")
        .required(false)
        .long("json")
        .short('j')
        .value_name("JSON")
        .takes_value(false);

    let repair: Arg = Arg::new("REPAIR")
        .help("Attempts a repair of an archive")
        .required(false)
        .long("repair")
        .short('r')
        .value_name("REPAIR")
        .takes_value(false);

    let validate_operations = SubCommand::with_name("validate")
        .about("Validate operations")
        .arg_required_else_help(true)
        .arg(repair.clone())
        .subcommand(
            SubCommand::with_name("all")
                .help_template(
                    "Validates the archive and optionally repairs \n\
                    corruption that may have occurred during interrupted 'pack' operation.\n\
                    Note: Data for previous interrupted 'pack' operation will be lost. \n\
                    \nOPTIONS:\n{options}\n USAGE:\n\t{usage}",
                )
                .about("Validates an archive")
                .arg(archive_arg.clone()),
        )
        .subcommand(
            SubCommand::with_name("stream")
                .help("Validates an individual stream")
                .about("Validates an individual stream")
                .arg(stream_arg.clone())
                .arg(archive_arg.clone()),
        );

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
                        .takes_value(true),
                )
                .arg(
                    Arg::new("BLOCK_SIZE")
                        .help("Specify the average block size used when deduplicating data")
                        .required(false)
                        .long("block-size")
                        .value_name("BLOCK_SIZE")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("HASH_CACHE_SIZE_MEG")
                        .help("Specify how much memory is used for caching hash entries")
                        .required(false)
                        .long("hash-cache-size-meg")
                        .value_name("HASH_CACHE_SIZE_MEG")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("DATA_CACHE_SIZE_MEG")
                        .help("Specify how much memory is used for caching data")
                        .required(false)
                        .long("data-cache-size-meg")
                        .value_name("DATA_CACHE_SIZE_MEG")
                        .takes_value(true),
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
                        .takes_value(true),
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
                        .takes_value(true),
                )
                .arg(
                    Arg::new("DELTA_DEVICE")
                        .help(
                            "Specify the device that contains an older version of this thin device",
                        )
                        .required(false)
                        .long("delta-device")
                        .value_name("DELTA_DEVICE")
                        .takes_value(true),
                ),
        )
        .subcommand(
            Command::new("unpack")
                .about("unpacks a stream from the archive")
                .arg(
                    Arg::new("OUTPUT")
                        .help("Specify a device or file as the destination")
                        .required(true)
                        .value_name("OUTPUT")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("CREATE")
                        .help("Create a new file rather than unpack to an existing device/file.")
                        .long("create")
                        .value_name("CREATE")
                        .takes_value(false),
                )
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
                        .takes_value(true),
                )
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
        .subcommand(validate_operations)
        .get_matches();

    let report = mk_report(&matches);
    report.set_level(LogLevel::Info);
    let output = Arc::new(Output {
        report: report.clone(),
        json: matches.is_present("JSON"),
    });
    match matches.subcommand() {
        Some(("create", sub_matches)) => {
            create::run(sub_matches, report)?;
        }
        Some(("pack", sub_matches)) => {
            pack::run(sub_matches, output)?;
        }
        Some(("unpack", sub_matches)) => {
            unpack::run_unpack(sub_matches, report)?;
        }
        Some(("verify", sub_matches)) => {
            unpack::run_verify(sub_matches, report)?;
        }
        Some(("list", sub_matches)) => {
            list::run(sub_matches, output)?;
        }
        Some(("dump-stream", sub_matches)) => {
            dump_stream::run(sub_matches, output)?;
        }
        Some(("validate", sub_matches)) => match sub_matches.subcommand() {
            Some(("all", args)) => {
                // Repair is an argument on the validate commands, not the all.
                // Maybe we can figure out how to make it an option of all instead?
                check::run(args, output, sub_matches.is_present("REPAIR"))?;
            }
            Some(("stream", args)) => {
                unpack::run_verify(args, report)?;
            }
            _ => unreachable!("Exhauted list of validate sub commands"),
        },
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
