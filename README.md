# Introduction

blk-archive is a tool for archiving block devices.

Archived data is deduplicated, compressed and stored in a directory
within a file system.

blk-archive is not a complete backup solution.  But it may well make an
excellent first step in a backup process.  For instance you may wish to
sync the archive directory to cloud storage, or write it to tape.

It works particularly well with the thin provisioning device-mapper
target:

- Thin snapshots can be used to archive live data.
- it avoids reading unprovisioned areas of thin devices.
- it can calculate deltas between thin devices to minimise how much data is read and deduped.
- restoring to a thin device tries to maximise data sharing within the thin pool
  (a big win if you're restoring snapshots).

# Status

This project is in a alpha state.

See doc/TODO.md for more info.

# Documentation

At the moment the best introduction to the tool is the *Use Cases*
document in the doc directory.

The files in the doc/ directory are in *markdown* format so can be read
in any text viewer.  However, they were edited using the Obsidian tool
(https://obsidian.md), and you will get a prettier viewing experience if you
also use this tool.

# Building

blk-archive is written in Rust.  You will need to install a recent
tool chain.  I recommend using *rustup*.

### Build dependencies
Some of the rust libraries have build dependencies which need to be satisfied.

* Fedora/EL9(enable CRB repo): `$ sudo dnf cargo clang-devel device-mapper-devel`
* ubuntu: `sudo apt-get install cargo libclang-dev libdevmapper-dev libsystemd-dev pkg-config`

Build via the standard *cargo* tool.

> cargo build --release

Do not forget the --release flag, debug builds can be an order of
magnitude slower.  The executable will be in target/release/.
