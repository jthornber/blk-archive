use anyhow::Result;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::ipc;
use crate::ipc::*;
use crate::stream::*;
use crate::stream_orderer::*;
use crate::wire;

pub struct Client {
    s: Box<dyn ReadAndWrite>,
    inflight: HashMap<u64, Data>, // Items waiting to complete
    so: Arc<Mutex<StreamOrder>>,
    req_q: Arc<Mutex<ClientRequests>>,
}

pub struct ClientRequests {
    requests: VecDeque<Data>,
    pub dead_thread: bool,
}

pub struct Data {
    pub id: u64,
    pub h: [u8; 32],
    pub len: u64,
    pub d: Option<Vec<u8>>,
}

impl Debug for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let data = if self.d.is_some() { "Some(d)" } else { "None" };

        f.debug_struct("Data")
            .field("id", &self.id)
            .field("h", &self.h)
            .field("len", &self.len)
            .field("d", &data)
            .finish()
    }
}

// TODO Change this to an enum which can handle multiple different messages
pub const END: Data = Data {
    id: u64::MAX,
    h: [0; 32],
    len: 0,
    d: None,
};

impl ClientRequests {
    fn new() -> Result<Self> {
        Ok(Self {
            requests: VecDeque::new(),
            dead_thread: false,
        })
    }

    pub fn handle_data(&mut self, d: Data) {
        self.requests.push_back(d);
    }

    pub fn remove(&mut self) -> Option<Data> {
        self.requests.pop_front()
    }
}

impl Client {
    pub fn new(server: String, so: Arc<Mutex<StreamOrder>>) -> Result<Self> {
        let s = create_connected_socket(server)?;

        Ok(Self {
            s,
            inflight: HashMap::new(),
            so,
            req_q: Arc::new(Mutex::new(ClientRequests::new()?)),
        })
    }

    fn process_request_queue(&mut self, w_b: &mut VecDeque<u8>) -> Result<wire::IORequest> {
        // Empty the request queue and send as one packet!
        let mut rc = wire::IORequest::Ok;
        let mut req = self.req_q.lock().unwrap();
        let mut entries: Vec<(u64, [u8; 32])> = Vec::new();
        let mut should_exit = false;
        while let Some(e) = req.remove() {
            if e.id == u64::MAX {
                println!("We have been told that we are done processing entries for stream");
                should_exit = true;
                rc = wire::IORequest::Exit;
            } else {
                //println!("entry add: {} - {:?} - {:?}", e.id, e.h, e);
                entries.push((e.id, e.h));
                self.inflight.insert(e.id, e);
            }
        }
        if !entries.is_empty() {
            let rpc_request = wire::Rpc::HaveDataReq(entries);
            if wire::write(&mut self.s, rpc_request, w_b)? {
                rc = wire::IORequest::WouldBlock;
            }

            if should_exit {
                self.s.flush()?;
            }
        }

        Ok(rc)
    }

    fn process_read(
        &mut self,
        buff: &mut VecDeque<u8>,
        w_b: &mut VecDeque<u8>,
    ) -> Result<wire::IORequest> {
        let mut rc = wire::IORequest::Ok;

        if let Some(cmds) = wire::read_using_buffer(&mut self.s, buff)? {
            for p in cmds {
                match p {
                    wire::Rpc::HaveDataRespYes(y) => {
                        // Server already had data, build the stream
                        let mut stream = self.so.lock().unwrap();
                        for (id, location) in y {
                            //println!("server already has {} - {}:{}", id, location.0, location.1);

                            let e = MapEntry::Data {
                                slab: location.0,
                                offset: location.1,
                                nr_entries: 1,
                            };
                            let removed = self.inflight.remove(&id).unwrap();
                            let len = removed.len;
                            stream.entry_complete(id, e, len)?;
                        }
                    }
                    wire::Rpc::HaveDataRespNo(to_send) => {
                        // Server does not have data, lets send it
                        for id in to_send {
                            if let Some(data) = self.inflight.get_mut(&id) {
                                // We remove the data from the structure to hopefully save some
                                // data sitting in memory
                                //println!("in HaveDataRespNo: {:?}", data);
                                let d = data.d.take().unwrap();
                                let pack_req = wire::Rpc::PackReq(data.id, data.h, d);
                                if wire::write(&mut self.s, pack_req, w_b)? {
                                    rc = wire::IORequest::WouldBlock;
                                }
                            } else {
                                panic!("How does this happen? {}", id);
                            }
                        }
                    }
                    wire::Rpc::PackResp(id, (slab, offset)) => {
                        //println!("wire::Rpc::PackResp: {} - {}:{}", id, slab, offset);
                        let mut stream = self.so.lock().unwrap();
                        let e = MapEntry::Data {
                            slab,
                            offset,
                            nr_entries: 1,
                        };
                        let removed = self.inflight.remove(&id).unwrap();
                        let len = removed.len;
                        stream.entry_complete(id, e, len)?;
                    }
                    _ => {
                        eprint!("What are we not handling! {:?}", p);
                    }
                }
            }
        }

        Ok(rc)
    }

    fn _run(&mut self) -> Result<()> {
        let event_fd = epoll::create(true)?;
        let fd = self.s.as_raw_fd();
        let mut event = ipc::read_event(fd);

        epoll::ctl(event_fd, epoll::ControlOptions::EPOLL_CTL_ADD, fd, event)?;

        let mut read_buffer: VecDeque<u8> = VecDeque::new();
        let mut wb: VecDeque<u8> = VecDeque::new();

        loop {
            let mut events = [epoll::Event::new(epoll::Events::empty(), 0); 10];
            let rdy = epoll::wait(event_fd, 2, &mut events)?;
            let mut end = false;

            for i in 0..rdy {
                if events[i].events & epoll::Events::EPOLLERR.bits()
                    == epoll::Events::EPOLLERR.bits()
                    || events[i].events & epoll::Events::EPOLLHUP.bits()
                        == epoll::Events::EPOLLHUP.bits()
                {
                    epoll::ctl(
                        event_fd,
                        epoll::ControlOptions::EPOLL_CTL_DEL,
                        event.data as i32,
                        event,
                    )?;
                    eprintln!("We got socket errors, exiting!");
                    end = true;
                    break;
                }

                if events[i].events & epoll::Events::EPOLLIN.bits() == epoll::Events::EPOLLIN.bits()
                {
                    let read_result = self.process_read(&mut read_buffer, &mut wb);
                    match read_result {
                        Err(e) => {
                            println!("The process_read returned an error, we are exiting {:?}", e);
                            return Err(e);
                        }
                        Ok(r) => {
                            if r == wire::IORequest::WouldBlock {
                                //println!("Write would block with {} bytes to send", wb.len());
                                event.events |= epoll::Events::EPOLLOUT.bits();
                                epoll::ctl(
                                    event_fd,
                                    epoll::ControlOptions::EPOLL_CTL_MOD,
                                    event.data as i32,
                                    event,
                                )?;
                            }
                        }
                    }
                }

                if events[i].events & epoll::Events::EPOLLOUT.bits()
                    == epoll::Events::EPOLLOUT.bits()
                {
                    if !wire::write_buffer(&mut self.s, &mut wb)? && wb.is_empty() {
                        event = ipc::read_event(event.data as i32);
                        epoll::ctl(
                            event_fd,
                            epoll::ControlOptions::EPOLL_CTL_MOD,
                            event.data as i32,
                            event,
                        )?;
                    }
                }
            }

            if end {
                break;
            }

            if self.process_request_queue(&mut wb)? == wire::IORequest::Exit {
                println!("request queue just got an end marker!");
                break;
            }
        }
        println!("Client thread is exiting!");
        Ok(())
    }

    pub fn run(&mut self) -> Result<()> {
        let result = self._run();

        {
            // Indicate that we don't have anything servicing the request queue.
            let mut req = self.req_q.lock().unwrap();
            req.dead_thread = true;
        }

        if let Err(e) = result {
            eprintln!("Client runner errored {}", e);
            return Err(e);
        }

        Ok(())
    }

    pub fn get_request_queue(&self) -> Arc<Mutex<ClientRequests>> {
        self.req_q.clone()
    }
}
