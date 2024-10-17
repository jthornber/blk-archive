use anyhow::Result;

use crate::db::*;
use crate::hash::bytes_to_hash256;
use crate::wire;
use nix::sys::signal;
use nix::sys::signal::SigSet;
use nix::sys::signalfd::{SfdFlags, SignalFd};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::env;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

use crate::iovec::*;
use crate::ipc;
use crate::ipc::*;
use crate::stream_meta;

pub struct Server {
    db: Db,
    listener: Box<dyn ipc::Listening>,
    signalfd: SignalFd,
    pub exit: Arc<AtomicBool>,
    _ipc_dir: Option<TempDir>,
}

struct Client {
    c: Box<dyn ReadAndWrite>,
    buff: VecDeque<u8>,
    wb: VecDeque<u8>,
}

impl Client {
    fn new(c: Box<dyn ReadAndWrite>) -> Self {
        Self {
            c,
            buff: VecDeque::new(),
            wb: VecDeque::new(),
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

        let db = Db::new()?;

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

    fn process_read(&mut self, c: &mut Client) -> Result<wire::IORequest> {
        let mut found_it: Vec<(u64, (u32, u32))> = Vec::new();
        let mut data_needed: Vec<u64> = Vec::new();
        let mut rc = wire::IORequest::Ok;

        //println!("process_read entry!");

        if let Some(rpcs) = wire::read_using_buffer(&mut c.c, &mut c.buff)? {
            //println!("Our read returned {} rpc messages", rpcs.len());
            for p in rpcs {
                match p {
                    wire::Rpc::HaveDataReq(hd) => {
                        //println!("wire::Rpc::HaveDataReq: {}", hd.len());
                        for (id, hash) in hd {
                            let hash256 = bytes_to_hash256(&hash);
                            if let Some(location) = self.db.is_known(hash256)? {
                                // We know about this one
                                found_it.push((id, location));
                            } else {
                                // We need to add the data
                                data_needed.push(id);
                            }
                        }
                    }
                    wire::Rpc::PackReq(id, hash, data) => {
                        let hash256 = bytes_to_hash256(&hash);
                        let iov: IoVec = vec![&data[..]];
                        //println!("packing data from client {} {:?} {}", id, hash, data.len());
                        let new_entry =
                            self.db.add_data_entry(*hash256, &iov, data.len() as u64)?;
                        if wire::write(&mut c.c, wire::Rpc::PackResp(id, new_entry), &mut c.wb)? {
                            rc = wire::IORequest::WouldBlock;
                        }
                    }
                    wire::Rpc::StreamSend(id, sm, stream_bytes, stream_offsets) => {
                        let packed_path = sm.source_path.clone();
                        let write_rc =
                            stream_meta::package_unwrap(sm, stream_bytes, stream_offsets);
                        match write_rc {
                            Ok(_) => {
                                if wire::write(
                                    &mut c.c,
                                    wire::Rpc::StreamSendComplete(id),
                                    &mut c.wb,
                                )? {
                                    rc = wire::IORequest::WouldBlock;
                                }
                            }
                            Err(e) => {
                                let message = format!(
                                    "During stream write id={} for stream = {} we encountered {}",
                                    id, packed_path, e
                                );
                                if wire::write(&mut c.c, wire::Rpc::Error(id, message), &mut c.wb)?
                                {
                                    rc = wire::IORequest::WouldBlock;
                                }
                            }
                        }
                    }
                    _ => {
                        eprint!("What are we not handling! {:?}", p);
                    }
                }
            }
        } else {
            // No more data to read, did we process anything?
        }

        if !found_it.is_empty()
            && wire::write(&mut c.c, wire::Rpc::HaveDataRespYes(found_it), &mut c.wb)?
        {
            rc = wire::IORequest::WouldBlock;
        }

        if !data_needed.is_empty()
            && wire::write(&mut c.c, wire::Rpc::HaveDataRespNo(data_needed), &mut c.wb)?
        {
            rc = wire::IORequest::WouldBlock;
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

        epoll::ctl(
            event_fd,
            epoll::ControlOptions::EPOLL_CTL_ADD,
            sfd_fd,
            event,
        )?;

        loop {
            let mut events = [epoll::Event::new(epoll::Events::empty(), 0); 10];
            let rdy = epoll::wait(event_fd, 2, &mut events)?;
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
                    println!("We lost a client!");
                    let fd_to_remove = item_rdy.data as i32;
                    event.data = fd_to_remove as u64;
                    epoll::ctl(
                        event_fd,
                        epoll::ControlOptions::EPOLL_CTL_DEL,
                        fd_to_remove,
                        event,
                    )?;

                    clients.remove(&fd_to_remove.clone());
                } else if item_rdy.events & epoll::Events::EPOLLIN.bits()
                    == epoll::Events::EPOLLIN.bits()
                    && item_rdy.data != listen_fd as u64
                {
                    let fd: i32 = item_rdy.data as i32;
                    let s = clients.get_mut(&fd).unwrap();

                    let result = self.process_read(s);
                    match result {
                        Err(e) => {
                            println!("Client ended in error: {}", e);
                            clients.remove(&fd);
                        }
                        Ok(r) => {
                            if r == wire::IORequest::WouldBlock {
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
                    // We only get here if we got an would block on a write before
                    let fd: i32 = item_rdy.data as i32;
                    let c = clients.get_mut(&fd).unwrap();

                    if !wire::write_buffer(&mut c.c, &mut c.wb)? && c.wb.is_empty() {
                        // Buffer is empty, we'll turn off pollout
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
        println!("exiting server.run()");
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        println!("Calling server.drop!");
    }
}
