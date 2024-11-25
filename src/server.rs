use anyhow::{anyhow, Result};

use crate::hash::bytes_to_hash256;
use crate::wire::{self, IOResult};
use crate::{config, db::*};
use nix::sys::signal;
use nix::sys::signal::SigSet;
use nix::sys::signalfd::{SfdFlags, SignalFd};
use rkyv::rancor::Error;
use std::collections::HashMap;
use std::env;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

use crate::client;
use crate::iovec::*;
use crate::ipc;
use crate::ipc::*;
use crate::list::streams_get;
use crate::stream_meta;

pub struct Server {
    db: Db,
    listener: Box<dyn ipc::Listening>,
    signalfd: SignalFd,
    pub exit: Arc<AtomicBool>,
    _ipc_dir: Option<TempDir>,
}

struct Client<'a> {
    c: Box<dyn ReadAndWrite>,
    bm: wire::BufferMeta,
    w: wire::OutstandingWrites<'a>,
}

impl<'a> Client<'a> {
    fn new(c: Box<dyn ReadAndWrite>) -> Self {
        Self {
            c,
            bm: wire::BufferMeta::new(),
            w: wire::OutstandingWrites::new(),
        }
    }
}

impl Server {
    pub fn new(archive_path: Option<&Path>, one_system: bool) -> Result<(Self, Option<String>)> {
        // If we don't have an archive_dir, we'll assume caller already set the current working
        // directory
        if let Some(archive_dir) = archive_path {
            let archive_dir = archive_dir.canonicalize()?;
            env::set_current_dir(&archive_dir)?;
        }

        let sfd = {
            let mut mask = SigSet::empty();
            mask.add(signal::SIGINT);
            mask.thread_block()?;
            SignalFd::with_flags(&mask, SfdFlags::SFD_NONBLOCK)?
        };

        let db = Db::new(None)?;

        let ipc_dir = if one_system {
            Some(ipc::create_unix_ipc_dir()?)
        } else {
            None
        };

        let server_addr = if let Some(d) = &ipc_dir {
            let f = d.path().join("ipc_socket");
            f.into_os_string().into_string().unwrap()
        } else {
            "0.0.0.0:9876".to_string()
        };

        let listener = create_listening_socket(&server_addr)?;

        let c_path = if one_system { Some(server_addr) } else { None };

        Ok((
            Self {
                db,
                listener,
                signalfd: sfd,
                exit: Arc::new(AtomicBool::new(false)),
                _ipc_dir: ipc_dir,
            },
            c_path,
        ))
    }

    fn process_read(&mut self, c: &mut Client) -> Result<wire::IOResult> {
        let mut found_it = Vec::new();
        let mut data_needed: Vec<u64> = Vec::new();

        // This reads a single request from the wire, we may need to change it to handle
        // all the requests to keep the incoming data as empty as possible.
        let mut rc = wire::read_request(&mut c.c, &mut c.bm)?;

        if rc == wire::IOResult::Ok {
            // We have a complete RPC request

            let r = rkyv::access::<wire::ArchivedRpc, Error>(
                &c.bm.buff[c.bm.meta_start..c.bm.meta_end],
            )
            .unwrap();
            let d = rkyv::deserialize::<wire::Rpc, Error>(r).unwrap();
            match d {
                wire::Rpc::HaveDataReq(data_req) => {
                    for i in data_req {
                        let hash256 = bytes_to_hash256(&i.hash);
                        if let Some(location) = self.db.is_known(hash256)? {
                            // We know about this one
                            let l = wire::DataRespYes {
                                id: i.id,
                                slab: location.0,
                                offset: location.1,
                            };
                            found_it.push(l);
                        } else {
                            // We need to add the data
                            data_needed.push(i.id);
                        }
                    }
                }
                wire::Rpc::PackReq(id, hash) => {
                    let mut iov = IoVec::new();
                    let hash256 = bytes_to_hash256(&hash);
                    iov.push(&c.bm.buff[c.bm.data_start..c.bm.data_end]);
                    let (entry, data_written, hash_written) =
                        self.db.add_data_entry(*hash256, &iov, c.bm.data_len())?;
                    let new_entry = wire::PackResp {
                        slab: entry.0,
                        offset: entry.1,
                        data_written,
                        hash_written,
                    };

                    if wire::write_request(
                        &mut c.c,
                        &wire::Rpc::PackResp(id, Box::new(new_entry)),
                        wire::WriteChunk::None,
                        &mut c.w,
                    )? {
                        rc = wire::IOResult::WriteWouldBlock;
                    }
                }
                wire::Rpc::StreamSend(id, sm, stream_files_bytes) => {
                    let packed_path = sm.source_path.clone();
                    let sf = wire::bytes_to_stream_files(&stream_files_bytes);
                    let write_rc =
                        stream_meta::package_unwrap(&sm, sf.stream.to_vec(), sf.offsets.to_vec());
                    // We have been sent a stream file, lets sync the data slab
                    self.db.complete_slab().unwrap();
                    match write_rc {
                        Ok(_) => {
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::StreamSendComplete(id),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                        Err(e) => {
                            let message = format!(
                                "During stream write id={} for stream = {} we encountered {}",
                                id, packed_path, e
                            );
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::Error(id, message),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                    }
                }
                wire::Rpc::ArchiveListReq(id) => {
                    let streams = streams_get(&PathBuf::from("./streams"));
                    match streams {
                        Ok(streams) => {
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::ArchiveListResp(id, streams),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                        Err(e) => {
                            let message = format!("During list::streams_get we encountered {}", e);
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::Error(id, message),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                    }
                }
                wire::Rpc::ArchiveConfig(id) => {
                    let config = config::read_config(".");
                    match config {
                        Ok(config) => {
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::ArchiveConfigResp(id, config),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                        Err(e) => {
                            let message =
                                format!("During wire::Rpc::ArchiveConfig we encountered {}", e);
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::Error(id, message),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                    }
                }
                wire::Rpc::StreamConfig(id, stream_id) => {
                    // Read up the stream config
                    let stream_config = stream_meta::read_stream_config(&stream_id);

                    match stream_config {
                        Ok(stream_config) => {
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::StreamConfigResp(id, Box::new(stream_config)),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                        Err(e) => {
                            let message =   //println!("process_read entry!");
                                format!("During wire::Rpc::StreamConfig we encountered {}", e);
                            if wire::write_request(
                                &mut c.c,
                                &wire::Rpc::Error(id, message),
                                wire::WriteChunk::None,
                                &mut c.w,
                            )? {
                                rc = wire::IOResult::WriteWouldBlock;
                            }
                        }
                    }
                }
                wire::Rpc::StreamRetrieve(id, stream_id) => {
                    let stream_files = stream_meta::stream_id_to_stream_files(&stream_id);

                    if let Err(e) = stream_files {
                        let message =
                            format!("During wire::Rpc::StreamRetrieve we encountered {}", e);
                        if wire::write_request(
                            &mut c.c,
                            &wire::Rpc::Error(id, message),
                            wire::WriteChunk::None,
                            &mut c.w,
                        )? {
                            rc = wire::IOResult::WriteWouldBlock;
                        }
                    } else {
                        let stream_files = stream_files.unwrap();

                        if wire::write_request(
                            &mut c.c,
                            &wire::Rpc::StreamRetrieveResp(id, stream_files),
                            wire::WriteChunk::None,
                            &mut c.w,
                        )? {
                            rc = wire::IOResult::WriteWouldBlock;
                        }
                    }
                }
                wire::Rpc::RetrieveChunkReq(id, op_type) => {
                    if let client::IdType::Unpack(s) = op_type {
                        let p = s.partial.map(|partial| (partial.begin, partial.end));

                        // How could this fail in normal operation?
                        let (data, start, end) =
                            self.db.data_get(s.slab, s.offset, s.nr_entries, p).unwrap();

                        if wire::write_request(
                            &mut c.c,
                            &wire::Rpc::RetrieveChunkResp(id),
                            wire::WriteChunk::ArcData(data, start, end),
                            &mut c.w,
                        )? {
                            rc = wire::IOResult::WriteWouldBlock;
                        }
                    }
                }
                _ => {
                    eprint!("What are we not handling! {:?}", d);
                }
            }
            c.bm.rezero();
        } else if rc == IOResult::PeerGone {
            return Err(anyhow!("Client closed connection"));
        }

        if !found_it.is_empty()
            && wire::write_request(
                &mut c.c,
                &wire::Rpc::HaveDataRespYes(found_it),
                wire::WriteChunk::None,
                &mut c.w,
            )?
        {
            rc = wire::IOResult::WriteWouldBlock;
        }

        if !data_needed.is_empty()
            && wire::write_request(
                &mut c.c,
                &wire::Rpc::HaveDataRespNo(data_needed),
                wire::WriteChunk::None,
                &mut c.w,
            )?
        {
            rc = wire::IOResult::WriteWouldBlock;
        }

        Ok(rc)
    }

    pub fn run(&mut self) -> Result<()> {
        println!("running as a service!");
        let event_fd = epoll::create(true)?;
        let mut clients = HashMap::<i32, Client>::new();

        let listen_fd = self.listener.as_raw_fd();

        let mut event: epoll::Event = epoll::Event {
            events: epoll::Events::EPOLLIN.bits(),
            data: listen_fd as u64,
        };

        epoll::ctl(
            event_fd,
            epoll::ControlOptions::EPOLL_CTL_ADD,
            listen_fd,
            event,
        )?;

        let sfd_fd = self.signalfd.as_raw_fd();
        event = ipc::read_event(sfd_fd);
        event.data = sfd_fd as u64;

        epoll::ctl(
            event_fd,
            epoll::ControlOptions::EPOLL_CTL_ADD,
            sfd_fd,
            event,
        )?;

        loop {
            let mut events = [epoll::Event::new(epoll::Events::empty(), 0); 10];
            let rdy = epoll::wait(event_fd, 100, &mut events)?;
            let mut end = false;

            for item_rdy in events.iter().take(rdy) {
                if item_rdy.events == epoll::Events::EPOLLIN.bits()
                    && item_rdy.data == listen_fd as u64
                {
                    let (new_client, addr) = self.listener.accept()?;
                    println!("We accepted a connection from {}", addr);
                    let fd = new_client.as_raw_fd();

                    event = ipc::read_event(fd);
                    epoll::ctl(event_fd, epoll::ControlOptions::EPOLL_CTL_ADD, fd, event)?;
                    clients.insert(fd, Client::new(new_client));
                }

                //if events[i].events == epoll::Events::EPOLLIN.bits() && events[i].data == sfd_fd as u64
                if item_rdy.data == sfd_fd as u64 {
                    eprintln!("SIGINT, exiting!");
                    end = true;
                    break;
                }

                if item_rdy.events & epoll::Events::EPOLLERR.bits()
                    == epoll::Events::EPOLLERR.bits()
                    || item_rdy.events & epoll::Events::EPOLLHUP.bits()
                        == epoll::Events::EPOLLHUP.bits()
                {
                    eprintln!("POLLERR");

                    let fd_to_remove = item_rdy.data as i32;
                    event.data = fd_to_remove as u64;
                    epoll::ctl(
                        event_fd,
                        epoll::ControlOptions::EPOLL_CTL_DEL,
                        fd_to_remove,
                        event,
                    )?;

                    clients.remove(&fd_to_remove.clone());
                    eprintln!("Removed client due to error!");
                } else if item_rdy.events & epoll::Events::EPOLLIN.bits()
                    == epoll::Events::EPOLLIN.bits()
                    && item_rdy.data != listen_fd as u64
                {
                    let fd: i32 = item_rdy.data as i32;
                    let s = clients.get_mut(&fd).unwrap();

                    let result = self.process_read(s);
                    match result {
                        Err(e) => {
                            println!("Client removed: {}", e);
                            clients.remove(&fd);
                        }
                        Ok(r) => {
                            if r == wire::IOResult::WriteWouldBlock {
                                event.events |= epoll::Events::EPOLLOUT.bits();
                                event.data = fd as u64;
                                epoll::ctl(
                                    event_fd,
                                    epoll::ControlOptions::EPOLL_CTL_MOD,
                                    fd,
                                    event,
                                )?;
                            }
                        }
                    }
                }

                if item_rdy.events & epoll::Events::EPOLLOUT.bits()
                    == epoll::Events::EPOLLOUT.bits()
                {
                    eprintln!("POLLOUT!");

                    // We only get here if we got an would block on a write before
                    let fd: i32 = item_rdy.data as i32;
                    let c = clients.get_mut(&fd).unwrap();

                    if c.w.write(&mut c.c)? == IOResult::Ok {
                        // We have no more pending data to write.
                        event = ipc::read_event(fd);
                        epoll::ctl(event_fd, epoll::ControlOptions::EPOLL_CTL_MOD, fd, event)?;
                    }
                }
            }

            if end {
                break;
            }

            if self.exit.load(Ordering::Relaxed) {
                eprintln!("Server asked to exit!");
                break;
            }
        }
        Ok(())
    }
}
