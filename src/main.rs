use anyhow::Result;
use clap::{command, Arg, Command};
use std::process::exit;

use dm_archive::create;
use dm_archive::pack;
use dm_archive::unpack;

//-----------------------

fn main_() -> Result<()> {
    let matches = command!()
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)

        .subcommand(
            Command::new("create")
                .about("creates a new archive")
                .arg(
                    Arg::new("DIR")
                        .help("Specify the top level directory for the archive")
                        .required(true)
                        .long("dir")
                        .value_name("DIR")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("BLOCK_SIZE")
                        .help("Specify the average block size used when deduplicating data")
                        .required(false)
                        .long("block-size")
                        .value_name("BLOCK_SIZE")
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
                .arg(
                    Arg::new("ARCHIVE")
                        .help("Specify archive directory")
                        .required(true)
                        .long("archive")
                        .short('a')
                        .value_name("ARCHIVE")
                        .takes_value(true),
                )
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
                    Arg::new("ARCHIVE")
                        .help("Specify archive directory")
                        .required(true)
                        .long("archive")
                        .short('a')
                        .value_name("ARCHIVE")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("STREAM")
                        .help("Specify an archived stream to unpack")
                        .required(true)
                        .long("stream")
                        .short('s')
                        .value_name("STREAM")
                        .takes_value(true),
                )
        )

        .get_matches();

    match matches.subcommand() {
        Some(("create", sub_matches)) => {
            create::run(sub_matches)?;
        }
        Some(("pack", sub_matches)) => {
            pack::run(sub_matches)?;
        }
        Some(("unpack", sub_matches)) => {
            unpack::run(sub_matches)?;
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
            eprintln!("{}", e);
            // We don't print out the error since -q may be set
            1
        }
    };

    exit(code)
}

//-----------------------
