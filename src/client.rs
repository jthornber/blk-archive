use anyhow::Result;
use bincode::{Decode, Encode};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::handshake;
use crate::handshake::HandShake;
use crate::ipc;
use crate::ipc::*;
use crate::stream::*;
use crate::stream_orderer::*;
use crate::wire;

pub struct Client {
    s: Box<dyn ReadAndWrite>,
    data_inflight: HashMap<u64, Data>, // Data items waiting to complete
    cmds_inflight: HashMap<u64, HandShake>,
    so: Arc<Mutex<StreamOrder>>,
    req_q: Arc<Mutex<ClientRequests>>,
}

pub struct ClientRequests {
    data: VecDeque<Data>,
    control: VecDeque<SyncCommand>,
    pub dead_thread: bool,
    pub data_written: u64,
    pub hashes_written: u64,
}

#[derive(PartialEq, Encode, Decode, Clone)]
pub enum IdType {
    Pack([u8; 32], u64),
    Unpack(u32, u32, u32, Option<(u32, u32)>), // Slab, offset, number entries, (partial_begin, partial_end)
}

#[derive(PartialEq, Clone)]
pub struct Data {
    pub id: u64,
    pub t: IdType,
    pub data: Option<Vec<u8>>,
    pub entry: Option<MapEntry>,
}

pub enum Command {
    Cmd(Box<wire::Rpc>),
    Exit,
}

pub struct SyncCommand {
    pub c: Command,
    pub h: HandShake,
}

impl SyncCommand {
    pub fn new(c: Command) -> Self {
        SyncCommand {
            c,
            h: HandShake::new(),
        }
    }
}

/*
impl Debug for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {

        let rc = match self.t {
            IdType::Pack(h, len, data) => {

                f.debug_struct("Data")
                .field("id", &self.id)
                .field("t", &self.h)
                .field("len", &self.len)
                .field("d", &data)
                .finish()
            }
            IdType::Unpack(slab,offset, nr_entries,partial) => {
                f.debug_struct("Data")
                .field("id", &self.id)
                .field("h", &self.h)
                .field("len", &self.len)
                .field("d", &data)
                .finish()
            }
        }

        Ok(rc)

        let data = if self.d.is_some() { "Some(d)" } else { "None" };

        /*
        f.debug_struct("Data")
            .field("id", &self.id)
            .field("h", &self.h)
            .field("len", &self.len)
            .field("d", &data)
            .finish()
        */
    }
}
*/

impl ClientRequests {
    fn new() -> Result<Self> {
        Ok(Self {
            data: VecDeque::new(),
            control: VecDeque::new(),
            dead_thread: false,
            data_written: 0,
            hashes_written: 0,
        })
    }

    pub fn handle_data(&mut self, d: Data) {
        self.data.push_back(d);
    }

    pub fn remove(&mut self) -> Option<Data> {
        self.data.pop_front()
    }

    pub fn handle_control(&mut self, c: SyncCommand) {
        self.control.push_back(c);
    }

    pub fn remove_control(&mut self) -> Option<SyncCommand> {
        self.control.pop_front()
    }
}

pub const END: Data = Data {
    id: u64::MAX,
    t: IdType::Pack([0; 32], 0),
    data: None,
    entry: None,
};

pub fn client_thread_end(client_req: &Mutex<ClientRequests>) {
    let mut rq = client_req.lock().unwrap();
    rq.handle_data(END);
}

impl Client {
    pub fn new(server: String, so: Arc<Mutex<StreamOrder>>) -> Result<Self> {
        let s = create_connected_socket(server)?;

        Ok(Self {
            s,
            data_inflight: HashMap::new(),
            cmds_inflight: HashMap::new(),
            so,
            req_q: Arc::new(Mutex::new(ClientRequests::new()?)),
        })
    }

    fn process_request_queue(&mut self, w_b: &mut VecDeque<u8>) -> Result<wire::IORequest> {
        // Empty the request queue and send as one packet!
        let mut rc = wire::IORequest::Ok;
        let mut req = self.req_q.lock().unwrap();
        let mut pack_entries: Vec<(u64, [u8; 32])> = Vec::new();
        let mut should_exit = false;
        while let Some(ref e) = req.remove() {
            if e.id == u64::MAX {
                should_exit = true;
                rc = wire::IORequest::Exit;
            } else {
                // We need to
                let t = &e.t;
                match t {
                    IdType::Pack(hash, _len) => {
                        let id = e.id;
                        pack_entries.push((id, *hash));
                        self.data_inflight.insert(id, e.clone());
                    }
                    IdType::Unpack(_slab, _offset, _nr, _partial) => {
                        let id = e.id;
                        let t = e.t.clone();
                        self.data_inflight.insert(e.id, e.clone());
                        let rpc_request = wire::Rpc::RetrieveChunkReq(id, t);
                        if wire::write(&mut self.s, rpc_request, w_b)? {
                            rc = wire::IORequest::WouldBlock;
                        }
                    }
                }
            }
        }
        if !pack_entries.is_empty() {
            let rpc_request = wire::Rpc::HaveDataReq(pack_entries);
            if wire::write(&mut self.s, rpc_request, w_b)? {
                rc = wire::IORequest::WouldBlock;
            }

            if should_exit {
                self.s.flush()?;
            }
        }

        while let Some(e) = req.remove_control() {
            match e.c {
                Command::Cmd(rpc) => {
                    self.cmds_inflight.insert(wire::id_get(&rpc), e.h.clone());
                    if wire::write(&mut self.s, *rpc, w_b)? {
                        rc = wire::IORequest::WouldBlock;
                    }
                }
                Command::Exit => {
                    rc = wire::IORequest::Exit;
                    e.h.done(None);
                }
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
                    wire::Rpc::RetrieveChunkResp(id, data) => {
                        let removed = self.data_inflight.remove(&id).unwrap();
                        let mut stream = self.so.lock().unwrap();
                        if let IdType::Unpack(_slab, _offset, _nr, _partial) = removed.t {
                            stream.entry_complete(id, removed.entry.unwrap(), None, Some(data));
                        }
                    }
                    wire::Rpc::HaveDataRespYes(y) => {
                        // Server already had data, build the stream
                        let mut stream = self.so.lock().unwrap();
                        for (id, location) in y {
                            let e = MapEntry::Data {
                                slab: location.0,
                                offset: location.1,
                                nr_entries: 1,
                            };
                            let removed = self.data_inflight.remove(&id).unwrap();

                            match removed.t {
                                IdType::Pack(_hash, len) => {
                                    stream.entry_complete(id, e, Some(len), None);
                                }
                                _ => {
                                    panic!("We are expecting only Pack type!");
                                }
                            }
                        }
                    }
                    wire::Rpc::HaveDataRespNo(to_send) => {
                        // Server does not have data, lets send it
                        for id in to_send {
                            if let Some(data) = self.data_inflight.get_mut(&id) {
                                // We remove the data from the structure to hopefully save some
                                // data sitting in memory
                                if let IdType::Pack(hash, _len) = data.t {
                                    let d = data.data.take().unwrap();
                                    let pack_req = wire::Rpc::PackReq(data.id, hash, d);
                                    if wire::write(&mut self.s, pack_req, w_b)? {
                                        rc = wire::IORequest::WouldBlock;
                                    }
                                }
                            } else {
                                panic!("How does this happen? {}", id);
                            }
                        }
                    }
                    wire::Rpc::PackResp(id, ((slab, offset), wrote, hashes_written)) => {
                        {
                            let mut rq = self.req_q.lock().unwrap();
                            rq.data_written += wrote;
                            rq.hashes_written += hashes_written;
                        }
                        let mut stream = self.so.lock().unwrap();
                        let e = MapEntry::Data {
                            slab,
                            offset,
                            nr_entries: 1,
                        };
                        let removed = self.data_inflight.remove(&id).unwrap();

                        if let IdType::Pack(_hash, len) = removed.t {
                            stream.entry_complete(id, e, Some(len), None);
                        }
                    }
                    wire::Rpc::StreamSendComplete(id) => {
                        self.cmds_inflight.remove(&id).unwrap().done(None);
                    }
                    wire::Rpc::ArchiveListResp(id, archive_list) => {
                        // This seems wrong, to have a Archive Resp and craft another to return?
                        self.cmds_inflight
                            .remove(&id)
                            .unwrap()
                            .done(Some(wire::Rpc::ArchiveListResp(id, archive_list)));
                    }
                    wire::Rpc::ArchiveConfigResp(id, config) => {
                        self.cmds_inflight
                            .remove(&id)
                            .unwrap()
                            .done(Some(wire::Rpc::ArchiveConfigResp(id, config)));
                    }
                    wire::Rpc::StreamRetrieveResp(id, data) => {
                        self.cmds_inflight
                            .remove(&id)
                            .unwrap()
                            .done(Some(wire::Rpc::StreamRetrieveResp(id, data)));
                    }
                    wire::Rpc::StreamConfigResp(id, config) => {
                        self.cmds_inflight
                            .remove(&id)
                            .unwrap()
                            .done(Some(wire::Rpc::StreamConfigResp(id, config)));
                    }
                    _ => {
                        eprint!("What are we not handling! {:?}", p);
                    }
                }
            }
        }

        Ok(rc)
    }

    fn _enable_poll_out(event_fd: i32, fd: i32, current_setting: &mut bool) -> Result<()> {
        if *current_setting {
            return Ok(());
        }

        *current_setting = true;
        let mut event = ipc::read_event(fd);
        event.events |= epoll::Events::EPOLLOUT.bits();
        event.data = fd as u64;

        Ok(epoll::ctl(
            event_fd,
            epoll::ControlOptions::EPOLL_CTL_MOD,
            fd,
            event,
        )?)
    }

    fn _run(&mut self) -> Result<()> {
        let event_fd = epoll::create(true)?;
        let fd = self.s.as_raw_fd();
        let mut event = ipc::read_event(fd);
        let mut poll_out = false;

        epoll::ctl(event_fd, epoll::ControlOptions::EPOLL_CTL_ADD, fd, event)?;

        let mut read_buffer: VecDeque<u8> = VecDeque::new();
        let mut wb: VecDeque<u8> = VecDeque::new();

        loop {
            let mut events = [epoll::Event::new(epoll::Events::empty(), 0); 10];
            let rdy = epoll::wait(event_fd, 2, &mut events)?;
            let mut end = false;

            for item_rdy in events.iter().take(rdy) {
                if item_rdy.events & epoll::Events::EPOLLERR.bits()
                    == epoll::Events::EPOLLERR.bits()
                    || item_rdy.events & epoll::Events::EPOLLHUP.bits()
                        == epoll::Events::EPOLLHUP.bits()
                {
                    epoll::ctl(
                        event_fd,
                        epoll::ControlOptions::EPOLL_CTL_DEL,
                        event.data as i32,
                        event,
                    )?;
                    eprintln!("Socket errors, exiting!");
                    end = true;
                    break;
                }

                if item_rdy.events & epoll::Events::EPOLLIN.bits() == epoll::Events::EPOLLIN.bits()
                {
                    let read_result = self.process_read(&mut read_buffer, &mut wb);
                    match read_result {
                        Err(e) => {
                            eprintln!("Error on read, exiting! {:?}", e);
                            return Err(e);
                        }
                        Ok(r) => {
                            if r == wire::IORequest::WouldBlock {
                                Client::_enable_poll_out(event_fd, fd, &mut poll_out)?;
                            }
                        }
                    }
                }

                if item_rdy.events & epoll::Events::EPOLLOUT.bits()
                    == epoll::Events::EPOLLOUT.bits()
                    && !wire::write_buffer(&mut self.s, &mut wb)?
                    && wb.is_empty()
                {
                    poll_out = false;
                    event = ipc::read_event(event.data as i32);
                    event.data = fd as u64;
                    epoll::ctl(
                        event_fd,
                        epoll::ControlOptions::EPOLL_CTL_MOD,
                        event.data as i32,
                        event,
                    )?;
                }
            }

            if end {
                break;
            }

            let rc = self.process_request_queue(&mut wb)?;
            if rc == wire::IORequest::Exit {
                break;
            } else if rc == wire::IORequest::WouldBlock {
                Client::_enable_poll_out(event_fd, fd, &mut poll_out)?;
            }
        }
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
            eprintln!("Client runner errored: {}", e);
            return Err(e);
        }

        Ok(())
    }

    pub fn get_request_queue(&self) -> Arc<Mutex<ClientRequests>> {
        self.req_q.clone()
    }
}

pub fn rpc_invoke(rq: &Mutex<ClientRequests>, rpc: wire::Rpc) -> Result<Option<wire::Rpc>> {
    let h: handshake::HandShake;
    {
        let cmd = SyncCommand::new(Command::Cmd(Box::new(rpc)));
        h = cmd.h.clone();
        let mut req = rq.lock().unwrap();
        req.handle_control(cmd);
    }

    // Wait for this to be done
    Ok(h.wait())
}

pub fn one_rpc(server: &str, rpc: wire::Rpc) -> Result<Option<wire::Rpc>> {
    let so = Arc::new(Mutex::new(StreamOrder::new()?));

    let mut client = Client::new(server.to_string(), so.clone())?;
    let rq = client.get_request_queue();
    // Start a thread to handle client communication
    let thread_handle = thread::Builder::new()
        .name("one_rpc".to_string())
        .spawn(move || client.run())?;

    let response = rpc_invoke(&rq, rpc)?;
    client_thread_end(&rq);

    let rc = thread_handle.join();
    if rc.is_err() {
        println!("client worker thread ended with {:?}", rc);
    }

    Ok(response)
}
