use anyhow::Result;
use clap::{command, Arg, Command};
use std::process::exit;

use dm_archive::pack;

//-----------------------

fn main_() -> Result<()> {
    let matches = command!()
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("pack")
                .about("packs a stream into the archive")
                .arg(
                    Arg::new("INPUT")
                        .help("Specify a device or file to archive")
                        .required(true)
                        .short('i')
                        .value_name("DEV")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("OUTPUT")
                        .help("Specify packed output file")
                        .required(true)
                        .short('o')
                        .value_name("FILE")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("BLOCK_SIZE")
                        .help("Specify average block size")
                        .required(false)
                        .validator(|s| s.parse::<usize>())
                        .default_value("4096")
                        .short('b')
                        .value_name("BLOCK_SIZE")
                        .takes_value(true),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("pack", sub_matches)) => {
            pack::run(sub_matches)?;
        }
        _ => unreachable!("Exhausted list of subcommands and subcommand_required prevents 'None'"),
    }

    Ok(())
}

fn main() {
    let code = match main_() {
        Ok(()) => 0,
        Err(_) => {
            // We don't print out the error since -q may be set
            1
        }
    };

    exit(code)
}

//-----------------------
