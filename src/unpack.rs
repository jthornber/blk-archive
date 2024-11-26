use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use io::{Read, Seek, Write};
use std::env;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
//use std::time::Duration;
use chrono::prelude::*;
use size_display::Size;
use tempfile::TempDir;
use thinp::report::*;

use crate::chunkers::*;
use crate::client;
use crate::config;
use crate::db;
use crate::db::SLAB_SIZE_TARGET;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;
use crate::stream_meta;
use crate::stream_orderer::StreamOrder;
use crate::thin_metadata::*;
use crate::wire;

//-----------------------------------------

// Unpack and verify do different things with the data.
trait UnpackDest {
    #[allow(dead_code)]
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()>;
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()>;
    fn handle_unmapped(&mut self, len: u64) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

struct Remote {
    rq: Arc<Mutex<client::ClientRequests>>,
    socket_thread: Option<JoinHandle<std::result::Result<(), anyhow::Error>>>,
    entry_thread: Option<JoinHandle<std::result::Result<(), anyhow::Error>>>,
    _td: TempDir,
    so: StreamOrder,
}

struct Unpacker<D: UnpackDest> {
    stream_file: PathBuf,
    db: Option<db::Db>,

    dest: D,
    remote: Option<Remote>,
}

impl<D: UnpackDest> Unpacker<D> {
    // Assumes current directory is the root of the archive.
    fn new(stream: &Path, cache_nr_entries: u64, dest: D, remote: Option<Remote>) -> Result<Self> {
        let db = if remote.is_some() {
            None
        } else {
            Some(db::Db::new(Some(cache_nr_entries))?)
        };

        Ok(Self {
            stream_file: stream.to_path_buf(),
            db,
            dest,
            remote,
        })
    }

    fn unpack_entry(&mut self, e: &MapEntry, data: Option<Vec<u8>>) -> Result<u64> {
        use MapEntry::*;

        let amt_written: u64;

        match e {
            Fill { byte, len } => {
                // len may be very big, so we have to be prepared to write in chunks.
                // FIXME: if we're writing to a file would this be zeroes anyway?  fallocate?
                const MAX_BUFFER: u64 = 16 * 1024 * 1024;
                let mut written = 0;
                while written < *len {
                    let write_len = std::cmp::min(*len - written, MAX_BUFFER);
                    let bytes: Vec<u8> = vec![*byte; write_len as usize];
                    self.dest.handle_mapped(&bytes[..])?;
                    written += write_len;
                }
                amt_written = *len;
            }
            Unmapped { len } => {
                self.dest.handle_unmapped(*len)?;
                amt_written = *len;
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                if let Some(d) = data {
                    amt_written = d.len() as u64;
                    self.dest.handle_mapped(&d[..])?;
                } else {
                    // This is a bit ugly, but removes a data copy
                    let (data, start, end) =
                        self.db
                            .as_mut()
                            .unwrap()
                            .data_get(*slab, *offset, *nr_entries, None)?;

                    amt_written = (end - start) as u64;
                    self.dest.handle_mapped(&data[start..end])?;
                };
            }
            Partial {
                begin,
                end,
                slab,
                offset,
                nr_entries,
            } => {
                if let Some(d) = data {
                    amt_written = d.len() as u64;
                    self.dest.handle_mapped(&d[..])?;
                } else {
                    // This is a bit ugly, but removes a data copy
                    let partial = Some((*begin, *end));
                    let (data, start, end) =
                        self.db
                            .as_mut()
                            .unwrap()
                            .data_get(*slab, *offset, *nr_entries, partial)?;

                    amt_written = (end - start) as u64;
                    self.dest.handle_mapped(&data[start..end])?;
                };
            }
            Ref { .. } => {
                // Can't get here.
                return Err(anyhow!("unexpected MapEntry::Ref (shouldn't be possible)"));
            }
        }

        Ok(amt_written)
    }

    pub fn unpack(&mut self, report: &Arc<Report>, total: u64) -> Result<()> {
        report.progress(0);
        let mut socket_thread_alive: bool = true;
        let mut amt_written = 0;

        let start_time: DateTime<Utc> = Utc::now();

        if self.remote.is_some() {
            let mut remote = self.remote.take().unwrap();

            // Loop getting stream entries from stream order which is be filled via the thread
            // that is running the function `build_entries`
            loop {
                let (entries, complete) = remote.so.drain(true);
                if !entries.is_empty() {
                    for e in entries {
                        amt_written += self.unpack_entry(&e.e, e.data)?;
                    }
                    let percentage = ((amt_written as f64 / total as f64) * 100.0) as u8;
                    report.progress(percentage);
                }

                if complete {
                    break;
                }

                {
                    let rq = remote.rq.lock().unwrap();
                    if rq.dead_thread {
                        socket_thread_alive = false;
                        break;
                    }
                }
            }

            if socket_thread_alive {
                let end_time: DateTime<Utc> = Utc::now();
                let elapsed = end_time - start_time;
                let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;

                report.info(&format!(
                    "speed            : {:.2}/s",
                    Size((total as f64 / elapsed) as u64)
                ));
            } else {
                eprintln!("We encountered an error during communication, exiting!");
            }

            let entry_thread = remote.entry_thread.take().unwrap();

            let rc = entry_thread.join();
            if rc.is_err() {
                eprintln!("Entry thread exited with {:?}", rc);
            }

            // End the socket thread
            client::client_thread_end(&remote.rq);
            let socket_thread = remote.socket_thread.take().unwrap();
            let rc = socket_thread.join();
            if rc.is_err() {
                eprintln!("Socket client thread exited with {:?}", rc);
            }
        } else {
            let mut stream_file = SlabFileBuilder::open(&self.stream_file).build()?;
            let nr_slabs = stream_file.get_nr_slabs();
            let mut unpacker = stream::MappingUnpacker::default();
            for s in 0..nr_slabs {
                let stream_data = stream_file.read(s as u32)?;
                let (entries, _positions) = unpacker.unpack(&stream_data[..])?;
                let nr_entries = entries.len();

                for (i, e) in entries.iter().enumerate() {
                    self.unpack_entry(e, None)?;

                    if i % 1024 == 0 {
                        // update progress bar
                        let entry_fraction = i as f64 / nr_entries as f64;
                        let slab_fraction = s as f64 / nr_slabs as f64;
                        let percent =
                            ((slab_fraction + (entry_fraction / nr_slabs as f64)) * 100.0) as u8;
                        report.progress(percent);
                    }
                }
            }

            let end_time: DateTime<Utc> = Utc::now();
            let elapsed = end_time - start_time;
            let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;

            report.info(&format!(
                "speed            : {:.2}/s",
                Size((total as f64 / elapsed) as u64)
            ));
        }

        self.dest.complete()?;
        report.progress(100);

        Ok(())
    }
}

/// Walks though the encoded stream file and placing each of the entries into a stream orderer.
/// For each MapEntry that requires data we'll queue up that requests to be sent out.
///
/// This function does not block on any entries or for waiting for data from server.
///
/// TODO: We need to place limits on how much data we have outstanding to the server as we could
/// use too much memory during this process.
fn build_entries(
    rq: Arc<Mutex<client::ClientRequests>>,
    stream: PathBuf,
    so: StreamOrder,
) -> Result<()> {
    use MapEntry::*;
    let mut stream_file = SlabFileBuilder::open(&stream).build()?;
    let nr_slabs = stream_file.get_nr_slabs();
    let mut unpacker = stream::MappingUnpacker::default();

    for s in 0..nr_slabs {
        let stream_data = stream_file.read(s as u32)?;
        let (entries, _positions) = unpacker.unpack(&stream_data[..])?;

        for e in entries.iter() {
            match e {
                Data {
                    slab,
                    offset,
                    nr_entries,
                } => {
                    let id = so.entry_start();
                    // Get a seq number and enqueue request

                    let slab_info = client::SlabInfo {
                        slab: *slab,
                        offset: *offset,
                        nr_entries: *nr_entries,
                        partial: None,
                    };

                    let data = client::Data {
                        id,
                        t: client::IdType::Unpack(slab_info),
                        data: None,
                        entry: Some(*e),
                    };
                    let mut rq = rq.lock().unwrap();
                    rq.handle_data(data);
                }
                Partial {
                    begin,
                    end,
                    slab,
                    offset,
                    nr_entries,
                } => {
                    let slab_info = client::SlabInfo {
                        slab: *slab,
                        offset: *offset,
                        nr_entries: *nr_entries,
                        partial: Some(client::Partial {
                            begin: *begin,
                            end: *end,
                        }),
                    };

                    let id = so.entry_start();
                    // Get a seq number and enqueue request
                    let data = client::Data {
                        id,
                        t: client::IdType::Unpack(slab_info),
                        data: None,
                        entry: Some(*e),
                    };
                    let mut rq = rq.lock().unwrap();
                    rq.handle_data(data);
                }
                _ => so.entry_add(*e, None, None),
            }
        }
    }

    Ok(())
}
//-----------------------------------------

struct ThickDest<W: Write> {
    output: W,
}

fn write_bytes<W: Write>(w: &mut W, byte: u8, len: u64) -> Result<()> {
    let buf_size = std::cmp::min(len, 64 * 1024 * 1024);
    let buf = vec![byte; buf_size as usize];

    let mut remaining = len;
    while remaining > 0 {
        let w_len = std::cmp::min(buf_size, remaining);
        w.write_all(&buf[0..(w_len as usize)])?;
        remaining -= w_len;
    }

    Ok(())
}

impl<W: Write> UnpackDest for ThickDest<W> {
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()> {
        write_bytes(&mut self.output, byte, len)
    }

    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        self.output.write_all(data)?;
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        write_bytes(&mut self.output, 0, len)
    }

    fn complete(&mut self) -> Result<()> {
        Ok(())
    }
}

//-----------------------------------------

// defined in include/uapi/linux/fs.h
const BLK_IOC_CODE: u8 = 0x12;
const BLKDISCARD_SEQ: u8 = 119;
nix::ioctl_write_ptr_bad!(
    ioctl_blkdiscard,
    nix::request_code_none!(BLK_IOC_CODE, BLKDISCARD_SEQ),
    [u64; 2]
);

struct ThinDest {
    block_size: u64,
    output: File,
    pos: u64,
    provisioned: RunIter,

    // (provisioned, len bytes)
    run: Option<(bool, u64)>,
    writes_avoided: u64,
}

impl ThinDest {
    fn issue_discard(&mut self, len: u64) -> Result<()> {
        let begin = self.pos;
        let end = begin + len;

        // Discards should always be block aligned
        assert_eq!(begin % self.block_size, 0);
        assert_eq!(end % self.block_size, 0);

        unsafe {
            ioctl_blkdiscard(self.output.as_raw_fd(), &[begin, len])?;
        }

        Ok(())
    }

    //------------------

    // These low level io functions update the position.
    fn forward(&mut self, len: u64) -> Result<()> {
        self.output.seek(std::io::SeekFrom::Current(len as i64))?;
        self.pos += len;
        Ok(())
    }

    fn rewind(&mut self, len: u64) -> Result<()> {
        self.output
            .seek(std::io::SeekFrom::Current(-(len as i64)))?;
        self.pos -= len;
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.output.write_all(data)?;
        self.pos += data.len() as u64;
        Ok(())
    }

    fn discard(&mut self, len: u64) -> Result<()> {
        self.issue_discard(len)?;
        self.forward(len)?;
        Ok(())
    }

    fn fill(&mut self, byte: u8, len: u64) -> Result<()> {
        write_bytes(&mut self.output, byte, len)?;
        self.pos += len;
        Ok(())
    }

    fn read(&mut self, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0; len as usize];
        self.output.read_exact(&mut buf[..])?;
        self.pos += len;
        Ok(buf)
    }

    //------------------

    //Why is this dead code?
    #[allow(dead_code)]
    fn handle_fill_unprovisioned(&mut self, byte: u8, len: u64) -> Result<()> {
        if byte == 0 {
            // FIXME: what if we fill a partial block, then subsequently trigger a provision and
            // block zeroing is turned off?
            self.forward(len)
        } else {
            self.fill(byte, len)
        }
    }

    //Why is this dead code?
    #[allow(dead_code)]
    fn handle_fill_provisioned(&mut self, byte: u8, len: u64) -> Result<()> {
        self.fill(byte, len)
    }

    fn handle_mapped_unprovisioned(&mut self, data: &[u8]) -> Result<()> {
        self.write(data)
    }

    fn handle_mapped_provisioned(&mut self, data: &[u8]) -> Result<()> {
        let actual = self.read(data.len() as u64)?;
        if actual == data {
            self.writes_avoided += data.len() as u64;
        } else {
            self.rewind(data.len() as u64)?;
            self.write(data)?;
        }

        Ok(())
    }

    fn handle_unmapped_unprovisioned(&mut self, len: u64) -> Result<()> {
        self.forward(len)
    }

    fn handle_unmapped_provisioned(&mut self, len: u64) -> Result<()> {
        self.discard(len)
    }

    fn ensure_run(&mut self) -> Result<()> {
        if self.run.is_none() {
            match self.provisioned.next() {
                Some((provisioned, run)) => {
                    self.run = Some((provisioned, (run.end - run.start) as u64 * self.block_size));
                }
                None => {
                    return Err(anyhow!("internal error: out of runs"));
                }
            }
        }

        Ok(())
    }

    fn next_run(&mut self, max_len: u64) -> Result<(bool, u64)> {
        self.ensure_run()?;
        let (provisioned, run_len) = self.run.take().unwrap();
        if run_len <= max_len {
            Ok((provisioned, run_len))
        } else {
            self.run = Some((provisioned, run_len - max_len));
            Ok((provisioned, max_len))
        }
    }
}

impl UnpackDest for ThinDest {
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let (provisioned, c_len) = self.next_run(remaining)?;

            if provisioned {
                self.handle_fill_provisioned(byte, c_len)?;
            } else {
                self.handle_fill_unprovisioned(byte, c_len)?;
            }

            remaining -= c_len;
        }

        Ok(())
    }

    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        let mut remaining = data.len() as u64;
        let mut offset = 0;
        while remaining > 0 {
            let (provisioned, c_len) = self.next_run(remaining)?;

            if provisioned {
                self.handle_mapped_provisioned(&data[offset as usize..(offset + c_len) as usize])?;
            } else {
                self.handle_mapped_unprovisioned(
                    &data[offset as usize..(offset + c_len) as usize],
                )?;
            }

            remaining -= c_len;
            offset += c_len;
        }

        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let (provisioned, c_len) = self.next_run(remaining)?;

            if provisioned {
                self.handle_unmapped_provisioned(c_len)?;
            } else {
                self.handle_unmapped_unprovisioned(c_len)?;
            }

            remaining -= c_len;
        }

        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        assert!(self.run.is_none());
        assert!(self.provisioned.next().is_none());
        Ok(())
    }
}

fn _retrieve_stream(
    rq: &Mutex<client::ClientRequests>,
    stream_id: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let rc = client::rpc_invoke(rq, wire::Rpc::StreamRetrieve(0, stream_id.to_string()))?
        .ok_or(anyhow!("expecting a result to StreamRetrieve!"))?;

    if let wire::Rpc::StreamRetrieveResp(_id, data) = rc {
        if data.is_none() {
            return Err(anyhow!(format!("stream id = {} not found!", stream_id)));
        }
        let t = data.unwrap();
        Ok((t.stream, t.offsets))
    } else {
        panic!("We received the wrong response {:?} !", rc);
    }
}

fn _retrieve_stream_config(
    rq: &Mutex<client::ClientRequests>,
    stream_id: &str,
) -> Result<stream_meta::StreamConfig> {
    let rc = client::rpc_invoke(rq, wire::Rpc::StreamConfig(0, stream_id.to_string()))?
        .ok_or(anyhow!("expecting a result to StreamConfig!"))?;

    if let wire::Rpc::StreamConfigResp(_id, config) = rc {
        Ok(*config)
    } else {
        panic!("We received the wrong response {:?} !", rc);
    }
}

fn create_open_output(create: bool, output_file: &Path) -> Result<File> {
    let output = if create {
        fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(output_file)
            .context(format!("Couldn't create output file: {:?}", output_file))?
    } else {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(output_file)
            .context(format!("Couldn't open output file: {:?}", output_file))?
    };
    Ok(output)
}

fn unpack_common(
    report: Arc<Report>,
    create: bool,
    output_file: &Path,
    stream_file: &Path,
    remote: Option<Remote>,
    cache_nr_entries: u64,
    output_size: u64,
) -> Result<()> {
    let output = create_open_output(create, output_file)?;

    if create {
        report.set_title(&format!("Unpacking {} ...", output_file.display()));
        let dest = ThickDest { output };
        let mut u = Unpacker::new(stream_file, cache_nr_entries, dest, remote)?;
        u.unpack(&report, output_size)
    } else {
        report.set_title(&format!("Unpacking {} ...", output_file.display()));
        if is_thin_device(output_file)? {
            let mappings = read_thin_mappings(output_file)?;
            let block_size = mappings.data_block_size as u64 * 512;
            let provisioned = RunIter::new(
                mappings.provisioned_blocks,
                (output_size / block_size) as u32,
            );

            let dest = ThinDest {
                block_size,
                output,
                pos: 0,
                provisioned,
                run: None,
                writes_avoided: 0,
            };
            let mut u = Unpacker::new(stream_file, cache_nr_entries, dest, remote)?;
            u.unpack(&report, output_size)
        } else {
            let dest = ThickDest { output };
            let mut u = Unpacker::new(stream_file, cache_nr_entries, dest, remote)?;
            u.unpack(&report, output_size)
        }
    }
}

//-----------------------------------------
pub fn run_receive(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());
    let stream_id = matches.get_one::<String>("STREAM").unwrap();
    let create = matches.get_one::<bool>("CREATE").unwrap();
    let server = matches.get_one::<String>("SERVER").unwrap();

    let so = StreamOrder::new();
    let mut client = client::Client::new(server.to_string(), so.clone())?;
    let rq = client.get_request_queue();
    // Start a thread to handle client communication
    let h = thread::Builder::new()
        .name("client unpack socket handler".to_string())
        .spawn(move || client.run())?;

    let td = stream_meta::create_temp_stream_dir(true)?;
    let stream_dir = td.path().join(stream_id);
    let stream_file = stream_dir.join("stream");
    let stream_file_offsets = stream_dir.join("stream.offsets");
    let e_msg = format!(
        "Error creating the local tmp. stream directory {:?}",
        stream_dir
    );
    fs::create_dir_all(stream_dir).context(e_msg)?;

    // Get archive config from server side to retrieve the memory cache size
    let cache_nr_entries = if let wire::Rpc::ArchiveConfigResp(_id, archive_config) =
        client::rpc_invoke(&rq, wire::Rpc::ArchiveConfig(0))?.ok_or(anyhow!(format!(
            "Unable to retrieve remote archive configuration"
        )))? {
        (1024 * 1024 * archive_config.data_cache_size_meg) / SLAB_SIZE_TARGET
    } else {
        return Err(anyhow!(
            "Unknown error while trying to retrieve archive config"
        ));
    };

    // Fetch stream file from server
    let (stream_data, offset_data) = _retrieve_stream(&rq, stream_id)?;

    // TODO: Move this stuff to a function in stream_meta, or instead of doing these files manually
    // provide the functionality to grab everything in the stream directory and copy it to here,
    // similar to making a tar file for a directory structure.
    stream_meta::write_file(&stream_file, stream_data)?;
    stream_meta::write_file(&stream_file_offsets, offset_data)?;

    // We spin up a thread which handles retrieving the MapEntries from the stream and retrieving
    // data from the server, does not block waiting for data from server.
    let thread_rq = rq.clone();
    let thread_so = so.clone();
    let thread_stream_file = stream_file.clone();
    let thread_handle = thread::Builder::new()
        .name("entry builder".to_string())
        .spawn(move || build_entries(thread_rq, thread_stream_file, thread_so))?;

    let remote = Remote {
        rq,
        socket_thread: Some(h),
        entry_thread: Some(thread_handle),
        _td: td,
        so,
    };

    // Check the size matches the stream size.
    let stream_cfg = _retrieve_stream_config(&remote.rq, stream_id)?;
    let stream_size = stream_cfg.size;

    if !*create && stream_size != thinp::file_utils::file_size(output_file)? {
        return Err(anyhow!("Destination size doesn't not match stream size"));
    }

    unpack_common(
        report,
        *create,
        output_file,
        &stream_file,
        Some(remote),
        cache_nr_entries,
        stream_size,
    )
}

pub fn run_unpack(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap())
        .canonicalize()
        .context("Bad archive dir")?;
    let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());
    let stream = matches.get_one::<String>("STREAM").unwrap();
    let create = matches.get_one::<bool>("CREATE").unwrap();
    let stream_path = stream_path(stream);

    env::set_current_dir(archive_dir)?;

    let config = config::read_config(".")?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    let stream_cfg = stream_meta::read_stream_config(stream)?;
    let stream_size = stream_cfg.size;

    if !*create && stream_size != thinp::file_utils::file_size(output_file)? {
        return Err(anyhow!("Destination size doesn't not match stream size"));
    }

    unpack_common(
        report,
        *create,
        output_file,
        &stream_path,
        None,
        cache_nr_entries,
        stream_size,
    )
}

//-----------------------------------------

struct VerifyDest {
    input_it: Box<dyn Iterator<Item = Result<Chunk>>>,
    chunk: Option<Chunk>,
    chunk_offset: u64,
    total_verified: u64,
}

impl VerifyDest {
    fn new(input_it: Box<dyn Iterator<Item = Result<Chunk>>>) -> Self {
        Self {
            input_it,
            chunk: None,
            chunk_offset: 0,
            total_verified: 0,
        }
    }
}

impl VerifyDest {
    fn fail(&self, msg: &str) -> anyhow::Error {
        anyhow!(format!(
            "verify failed at offset ~{}: {}",
            self.total_verified, msg
        ))
    }

    fn ensure_chunk(&mut self) -> Result<()> {
        if self.chunk.is_none() {
            match self.input_it.next() {
                Some(rc) => {
                    self.chunk = Some(rc?);
                    self.chunk_offset = 0;
                    Ok(())
                }
                None => Err(self.fail("archived stream is longer than expected")),
            }
        } else {
            Ok(())
        }
    }

    fn peek_data(&mut self, max_len: u64) -> Result<&[u8]> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let len = std::cmp::min(bytes.len() - self.chunk_offset as usize, max_len as usize);
                Ok(&bytes[self.chunk_offset as usize..(self.chunk_offset + len as u64) as usize])
            }
            Some(Chunk::Unmapped(_)) => Err(self.fail("expected data, got unmapped")),
            Some(Chunk::Ref(_)) => Err(self.fail("expected data, got ref")),
            None => Err(self.fail("ensure_chunk() failed")),
        }
    }

    fn consume_data(&mut self, len: u64) -> Result<()> {
        use std::cmp::Ordering::*;
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let c_len = bytes.len() as u64 - self.chunk_offset;
                match c_len.cmp(&len) {
                    Less => {
                        return Err(self.fail("bad consume, chunk too short"));
                    }
                    Greater => {
                        self.chunk_offset += len;
                    }
                    Equal => {
                        self.chunk = None;
                    }
                }
            }
            Some(Chunk::Unmapped(_)) => {
                return Err(self.fail("bad consume, unexpected unmapped chunk"));
            }
            Some(Chunk::Ref(_)) => {
                return Err(self.fail("bad consume, unexpected ref chunk"));
            }
            None => {
                return Err(self.fail("archived stream longer than input"));
            }
        }
        Ok(())
    }

    fn get_unmapped(&mut self, max_len: u64) -> Result<u64> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(_)) => Err(self.fail("expected unmapped, got data")),
            Some(Chunk::Unmapped(len)) => {
                let len = *len;
                if len <= max_len {
                    self.chunk = None;
                    Ok(len)
                } else {
                    self.chunk = Some(Chunk::Unmapped(len - max_len));
                    Ok(max_len)
                }
            }
            Some(Chunk::Ref(_)) => Err(self.fail("unexpected Ref")),
            None => Err(self.fail("archived stream longer than input")),
        }
    }

    fn more_data(&self) -> bool {
        self.chunk.is_some()
    }
}

impl UnpackDest for VerifyDest {
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let actual = self.peek_data(remaining)?;
            let actual_len = actual.len() as u64;

            for b in actual {
                if *b != byte {
                    return Err(self.fail("fill mismatch"));
                }
            }
            self.consume_data(actual_len)?;
            remaining -= actual_len;
        }
        self.total_verified += len;
        Ok(())
    }

    fn handle_mapped(&mut self, expected: &[u8]) -> Result<()> {
        let mut remaining = expected.len() as u64;
        let mut offset = 0;
        while remaining > 0 {
            let actual = self.peek_data(remaining)?;
            let actual_len = actual.len() as u64;
            if actual != &expected[offset as usize..(offset + actual_len) as usize] {
                return Err(self.fail("data mismatch"));
            }
            self.consume_data(actual_len)?;
            remaining -= actual_len;
            offset += actual_len;
        }
        self.total_verified += expected.len() as u64;
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let len = self.get_unmapped(remaining)?;
            remaining -= len;
        }
        self.total_verified += len;
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        if self.more_data() {
            return Err(anyhow!("archived stream is too short"));
        }
        Ok(())
    }
}

fn thick_verifier(input_file: &Path) -> Result<VerifyDest> {
    let input_it = Box::new(ThickChunker::new(input_file, 16 * 1024 * 1024)?);
    Ok(VerifyDest::new(input_it))
}

fn thin_verifier(input_file: &Path) -> Result<VerifyDest> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(input_file)?;
    let mappings = read_thin_mappings(input_file)?;

    // FIXME: what if input_size is not a multiple of the block size?
    let run_iter = RunIter::new(
        mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_it = Box::new(ThinChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));

    Ok(VerifyDest::new(input_it))
}

pub fn run_verify(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap()).canonicalize()?;
    let stream = matches.get_one::<String>("STREAM").unwrap();

    env::set_current_dir(archive_dir)?;

    let config = config::read_config(".")?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    report.set_title(&format!(
        "Verifying {} and {} match ...",
        input_file.display(),
        &stream
    ));

    let dest = if is_thin_device(&input_file)? {
        thin_verifier(&input_file)?
    } else {
        thick_verifier(&input_file)?
    };

    let stream_path = stream_path(stream);
    let size = thinp::file_utils::file_size(input_file)?;

    let mut u = Unpacker::new(&stream_path, cache_nr_entries, dest, None)?;
    u.unpack(&report, size)
}

//-----------------------------------------
