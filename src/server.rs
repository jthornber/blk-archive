use anyhow::{Context, Result};

use crate::config::*;
use crate::db::*;
use crate::hash::bytes_to_hash256;
use crate::hash::Hash256;
use crate::paths::*;
use crate::slab::*;
use crate::wire;
use nix::sys::signal;
use nix::sys::signal::SigSet;
use nix::sys::signalfd::{SfdFlags, SignalFd};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::env;
use std::net::TcpListener;
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::path::Path;

use crate::iovec::*;
use crate::ipc;

pub struct Server {
    db: Db,
    listener: TcpListener,
    signalfd: SignalFd,
}

#[derive(Debug)]
struct Client {
    c: TcpStream,
    buff: VecDeque<u8>,
}

impl Client {
    fn new(c: TcpStream) -> Self {
        Self {
            c,
            buff: VecDeque::new(),
        }
    }
}

impl Server {
    pub fn new(archive_path: &Path, one_system: bool) -> Result<Self> {
        let archive_dir = archive_path.canonicalize()?;
        env::set_current_dir(&archive_dir)?;

        let sfd = {
            let mut mask = SigSet::empty();
            mask.add(signal::SIGINT);
            mask.thread_block()?;
            SignalFd::with_flags(&mask, SfdFlags::SFD_NONBLOCK)?
        };

        let data_file = SlabFileBuilder::open(data_path())
            .write(true)
            .queue_depth(128)
            .build()
            .context("couldn't open data slab file")?;

        let config = read_config(".")?;

        let hashes_per_slab = std::cmp::max(SLAB_SIZE_TARGET / config.block_size, 1);
        let slab_capacity = ((config.hash_cache_size_meg * 1024 * 1024)
            / std::mem::size_of::<Hash256>())
            / hashes_per_slab;

        let db = Db::new(data_file, slab_capacity)?;

        let listener = if one_system {
            TcpListener::bind("127.0.0.1:9876")?
        } else {
            TcpListener::bind("0.0.0.0:9876")?
        };

        listener.set_nonblocking(true)?;

        Ok(Self {
            db,
            listener,
            signalfd: sfd,
        })
    }

    fn process_read(&mut self, c: &mut Client) -> Result<bool> {
        let mut found_it: Vec<(u64, (u32, u32))> = Vec::new();
        let mut data_needed: Vec<u64> = Vec::new();

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
                        let mut iov = IoVec::new();
                        iov.push(&data[..]);
                        //println!("packing data from client {} {:?} {}", id, hash, data.len());
                        let new_entry =
                            self.db.add_data_entry(*hash256, &iov, data.len() as u64)?;
                        wire::write_rpc_panic(&mut c.c, wire::Rpc::PackResp(id, new_entry));
                    }
                    _ => {
                        eprint!("What are we not handling! {:?}", p);
                    }
                }
            }
        } else {
            // No more data to read, did we process anything?
            println!("Nothing to read ... why?");
        }

        if !found_it.is_empty() {
            //println!("We are writing back found for {}", found_it.len());
            wire::write_rpc_panic(&mut c.c, wire::Rpc::HaveDataRespYes(found_it));
        }

        if !data_needed.is_empty() {
            //println!("We are writing back data needed for {}", data_needed.len());
            wire::write_rpc_panic(&mut c.c, wire::Rpc::HaveDataRespNo(data_needed));
        }

        Ok(false)
    }

    pub fn run(&mut self) -> Result<()> {
        println!("running as a service!");
        let event_fd = epoll::create(true)?;
        let mut clients = HashMap::<i32, Client>::new();

        let listen_fd = self.listener.as_raw_fd();
        println!("listening fd = {}", listen_fd);

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
            let rdy = epoll::wait(event_fd, -1, &mut events)?;
            let mut end = false;

            for i in 0..rdy {
                if events[i].events == epoll::Events::EPOLLIN.bits()
                    && events[i].data == listen_fd as u64
                {
                    let (new_client, addr) = self.listener.accept()?;
                    println!("We accepted a connection from {}", addr);
                    new_client.set_nonblocking(true)?;
                    let fd = new_client.as_raw_fd();
                    new_client.set_nodelay(true).unwrap();

                    event = ipc::read_event(fd);
                    epoll::ctl(event_fd, epoll::ControlOptions::EPOLL_CTL_ADD, fd, event)?;
                    clients.insert(fd, Client::new(new_client));
                }

                //if events[i].events == epoll::Events::EPOLLIN.bits() && events[i].data == sfd_fd as u64
                if events[i].data == sfd_fd as u64 {
                    eprintln!("SIGINT, exiting!");
                    end = true;
                    break;
                }

                if events[i].events & epoll::Events::EPOLLERR.bits()
                    == epoll::Events::EPOLLERR.bits()
                    || events[i].events & epoll::Events::EPOLLHUP.bits()
                        == epoll::Events::EPOLLHUP.bits()
                {
                    println!("We lost a client!");
                    let fd_to_remove = events[i].data as i32;
                    event.data = fd_to_remove as u64;
                    epoll::ctl(
                        event_fd,
                        epoll::ControlOptions::EPOLL_CTL_DEL,
                        fd_to_remove,
                        event,
                    )?;

                    clients.remove(&fd_to_remove.clone());
                } else if events[i].events & epoll::Events::EPOLLIN.bits()
                    == epoll::Events::EPOLLIN.bits()
                    && events[i].data != listen_fd as u64
                {
                    let fd: i32 = events[i].data as i32;
                    let s = clients.get_mut(&fd).unwrap();

                    let result = self.process_read(s);
                    match result {
                        Err(e) => {
                            println!("Client ended in error: {}", e);
                            clients.remove(&fd);
                        }
                        Ok(rc) => {
                            if rc {
                                println!("Client completed cleanly");
                                clients.remove(&fd);
                            }
                        }
                    }
                }

                /*
                if events[i].events & epoll::Events::EPOLLOUT.bits() == epoll::Events::EPOLLOUT.bits() {
                    // We only get here if we got an would block on a write before
                    let fd: i32 = events[i].data as i32;
                    let c = clients.get_mut(&fd).unwrap();
                    c.write_all(&data_read[0..amt_read]).unwrap();
                    amt_read = 0;

                    // We will remove the epollout
                    event = read_event(fd);
                    event.data = fd as u64;
                    epoll::ctl(event_fd, epoll::ControlOptions::EPOLL_CTL_MOD, fd, event)?;
                }
                */
            }

            if end {
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
