use anyhow::{anyhow, ensure, Result};
use std::ffi::OsString;
use std::path::Path;
use std::process::exit;

use dm_archive::add;

//-----------------------

fn name_eq(name: &Path, cmd: &str) -> bool {
    name == Path::new(cmd)
}

fn main_() -> Result<()> {
    let mut args = std::env::args_os();
    ensure!(args.len() > 0);

    let mut os_name = args.next().unwrap();
    let mut os_name = args.next().unwrap();
    let mut name = Path::new(&os_name);

    let mut new_args = vec![OsString::from(&name)];
    for a in args.into_iter() {
        new_args.push(a);
    }

    if name_eq(name, "add") {
        add::run(&new_args);
    } else {
        eprintln!("unrecognised command");
        return Err(anyhow!("unrecognised command"));
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
