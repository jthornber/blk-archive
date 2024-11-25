use anyhow::Result;
use clap::ArgAction;
use clap::{command, Arg, ArgGroup, ArgMatches, Command};
use std::env;
use std::path::Path;
use std::process::exit;
use std::sync::Arc;
use thinp::report::*;

use blk_archive::create;
use blk_archive::dump_stream;
use blk_archive::list;
use blk_archive::output::Output;
use blk_archive::pack;
use blk_archive::server;
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
        .required(true)
        .long("archive")
        .short('a')
        .value_name("ARCHIVE")
        .env("BLK_ARCHIVE_DIR");

    let stream_arg = Arg::new("STREAM")
        .help("Specify an archived stream to unpack")
        .required(true)
        .long("stream")
        .short('s')
        .value_name("STREAM");

    let json: Arg = Arg::new("JSON")
        .help("Output JSON")
        .required(false)
        .long("json")
        .short('j')
        .action(ArgAction::SetTrue);

    let server = Arg::new("SERVER")
        .help("Server connection information")
        .required(true)
        .long("server")
        .short('s')
        .value_name("SERVER");

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
                        .value_name("ARCHIVE"),
                )
                .arg(
                    Arg::new("BLOCK_SIZE")
                        .help("Specify the average block size used when deduplicating data")
                        .required(false)
                        .long("block-size")
                        .value_name("BLOCK_SIZE"),
                )
                .arg(
                    Arg::new("HASH_CACHE_SIZE_MEG")
                        .help("Specify how much memory is used for caching hash entries")
                        .required(false)
                        .long("hash-cache-size-meg")
                        .value_name("HASH_CACHE_SIZE_MEG"),
                )
                .arg(
                    Arg::new("DATA_CACHE_SIZE_MEG")
                        .help("Specify how much memory is used for caching data")
                        .required(false)
                        .long("data-cache-size-meg")
                        .value_name("DATA_CACHE_SIZE_MEG"),
                ),
        )
        .subcommand(
            Command::new("pack")
                .about("packs a stream into the archive")
                .arg(
                    Arg::new("INPUT")
                        .help("Specify a device or file to archive")
                        .required(true)
                        .value_name("INPUT"),
                )
                .arg(archive_arg.clone())
                .arg(
                    Arg::new("DELTA_STREAM")
                        .help(
                            "Specify the stream that contains an older version of this thin device",
                        )
                        .required(false)
                        .long("delta-stream")
                        .value_name("DELTA_STREAM"),
                )
                .arg(
                    Arg::new("DELTA_DEVICE")
                        .help(
                            "Specify the device that contains an older version of this thin device",
                        )
                        .required(false)
                        .long("delta-device")
                        .value_name("DELTA_DEVICE"),
                ),
        )
        .subcommand(
            Command::new("send")
                .about("packs a stream into a remote archive")
                .arg(server.clone())
                .arg(
                    Arg::new("INPUT")
                        .help("Specify a device or file to archive")
                        .required(true)
                        .value_name("INPUT"),
                ),
        )
        .subcommand(
            Command::new("unpack")
                .about("unpacks a stream from the archive")
                .arg(
                    Arg::new("OUTPUT")
                        .help("Specify a device or file as the destination")
                        .required(true)
                        .value_name("OUTPUT"),
                )
                .arg(
                    Arg::new("CREATE")
                        .help("Create a new file rather than unpack to an existing device/file.")
                        .long("create")
                        .value_name("CREATE")
                        .action(ArgAction::SetTrue),
                )
                .arg(archive_arg.clone())
                .arg(stream_arg.clone()),
        )
        .subcommand(
            Command::new("receive")
                .about("unpacks a stream from a remote archive")
                .arg(
                    Arg::new("OUTPUT")
                        .help("Specify a device or file as the destination")
                        .required(true)
                        .value_name("OUTPUT"),
                )
                .arg(
                    Arg::new("CREATE")
                        .help("Create a new file rather than unpack to an existing device/file.")
                        .long("create")
                        .value_name("CREATE")
                        .action(ArgAction::SetTrue),
                )
                .arg(server.clone())
                .arg(stream_arg.clone()),
        )
        .subcommand(
            Command::new("verify")
                .about("verifies stream in the archive against the original file/dev")
                .arg(
                    Arg::new("INPUT")
                        .help("Specify a device or file containing the correct version of the data")
                        .required(true)
                        .value_name("INPUT"),
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
                // Note: We can't use the existing archive etc. as they CANNOT have required=true
                // in them, otherwise you panic at runtime!
                .about("lists the streams in the archive")
                .arg(
                    Arg::new("LIST_ARCHIVE")
                        .help("Specify archive directory")
                        .long("archive")
                        .short('a')
                        .value_name("ARCHIVE"),
                )
                .arg(
                    Arg::new("LIST_SERVER")
                        .help("Specify server connection information (hostname:port/ip:port)")
                        .long("server")
                        .short('s')
                        .value_name("SERVER"),
                )
                .group(
                    ArgGroup::new("list_exclusive_option")
                        .arg("LIST_SERVER")
                        .arg("LIST_ARCHIVE")
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("server")
                .about("run in daemon mode")
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
        Some(("send", sub_matches)) => {
            let server = Some(sub_matches.get_one::<String>("SERVER").unwrap().clone());
            pack::run(sub_matches, output, server)?;
        }
        Some(("pack", sub_matches)) => {
            pack::run(sub_matches, output, None)?;
        }
        Some(("unpack", sub_matches)) => {
            unpack::run_unpack(sub_matches, report)?;
        }
        Some(("receive", sub_matches)) => {
            unpack::run_receive(sub_matches, report)?;
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
        Some(("server", sub_matches)) => {
            let archive_dir =
                Path::new(sub_matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
            let mut s = server::Server::new(Some(&archive_dir), false)?;
            s.0.run()?;
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
