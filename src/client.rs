use anyhow::Result;
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::handshake;
use crate::handshake::HandShake;
use crate::ipc;
use crate::ipc::*;
use crate::stream::*;
use crate::stream_orderer::*;
use crate::wire::{self, IOResult};

pub struct Client {
    s: Box<dyn ReadAndWrite>,
    data_inflight: HashMap<u64, Data>, // Data items waiting to complete
    cmds_inflight: HashMap<u64, HandShake>,
    so: StreamOrder,
    req_q: Arc<Mutex<ClientRequests>>,
}

pub struct ClientRequests {
    data: VecDeque<Data>,
    control: VecDeque<SyncCommand>,
    pub dead_thread: bool,
    pub data_written: u64,
    pub hashes_written: u64,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct Partial {
    pub begin: u32,
    pub end: u32,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct SlabInfo {
    pub slab: u32,
    pub offset: u32,
    pub nr_entries: u32,
    pub partial: Option<Partial>,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum IdType {
    Pack([u8; 32], u64),
    Unpack(SlabInfo), // Slab, offset, number entries, (partial_begin, partial_end)
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
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
    pub fn new(server: String, so: StreamOrder) -> Result<Self> {
        let s = create_connected_socket(server)?;

        Ok(Self {
            s,
            data_inflight: HashMap::new(),
            cmds_inflight: HashMap::new(),
            so,
            req_q: Arc::new(Mutex::new(ClientRequests::new()?)),
        })
    }

    fn process_request_queue(
        &mut self,
        w_b: &mut wire::OutstandingWrites,
    ) -> Result<wire::IOResult> {
        // Empty the request queue and send as one packet!
        let mut rc = wire::IOResult::Ok;
        let mut req = self.req_q.lock().unwrap();
        let mut pack_entries = Vec::new();
        let mut should_exit = false;
        while let Some(e) = req.remove() {
            if e.id == u64::MAX {
                should_exit = true;
                rc = wire::IOResult::Exit;
            } else {
                // We need to
                let t = &e.t;
                match t {
                    IdType::Pack(hash, _len) => {
                        let id = e.id;
                        pack_entries.push(wire::DataReq { id, hash: *hash });
                        self.data_inflight.insert(id, e);
                    }
                    IdType::Unpack(_s) => {
                        let id = e.id;
                        let t = e.t.clone();
                        self.data_inflight.insert(e.id, e);
                        let rpc_request = wire::Rpc::RetrieveChunkReq(id, t);
                        if wire::write_request(
                            &mut self.s,
                            &rpc_request,
                            wire::WriteChunk::None,
                            w_b,
                        )? {
                            rc = wire::IOResult::WriteWouldBlock;
                        }
                    }
                }
            }
        }
        if !pack_entries.is_empty() {
            let rpc_request = wire::Rpc::HaveDataReq(pack_entries);
            if wire::write_request(&mut self.s, &rpc_request, wire::WriteChunk::None, w_b)? {
                rc = wire::IOResult::WriteWouldBlock;
            }

            if should_exit {
                self.s.flush()?;
            }
        }

        while let Some(e) = req.remove_control() {
            match e.c {
                Command::Cmd(rpc) => {
                    self.cmds_inflight.insert(wire::id_get(&rpc), e.h.clone());
                    if wire::write_request(&mut self.s, &rpc, wire::WriteChunk::None, w_b)? {
                        rc = wire::IOResult::WriteWouldBlock;
                    }
                }
                Command::Exit => {
                    rc = wire::IOResult::Exit;
                    e.h.done(None);
                }
            }
        }

        Ok(rc)
    }

    fn process_read(
        &mut self,
        r: &mut wire::BufferMeta,
        w: &mut wire::OutstandingWrites,
    ) -> Result<wire::IOResult> {
        let mut rc = wire::read_request(&mut self.s, r)?;

        if rc == wire::IOResult::Ok {
            let arpc = rkyv::access::<wire::ArchivedRpc, Error>(&r.buff[r.meta_start..r.meta_end])
                .unwrap();
            let d = rkyv::deserialize::<wire::Rpc, Error>(arpc).unwrap();

            match d {
                wire::Rpc::RetrieveChunkResp(id) => {
                    let removed = self.data_inflight.remove(&id).unwrap();
                    if let IdType::Unpack(_s) = removed.t {
                        // The code can handle responses out of order, but the current implementation
                        // handles things in order.  Thus, when we receive this chunk of data for
                        // the unpack operation if we are caught up with processing the entries in
                        // the stream order, we could simply write this data to the output file/
                        // device which would prevent us from having to allocate memory here.
                        let mut data = Vec::with_capacity(r.data_len() as usize);
                        data.extend_from_slice(&r.buff[r.data_start..r.data_end]);
                        self.so
                            .entry_complete(id, removed.entry.unwrap(), None, Some(data));
                    }
                }
                wire::Rpc::HaveDataRespYes(y) => {
                    // Server already had data, build the stream
                    for s in y {
                        let e = MapEntry::Data {
                            slab: s.slab,
                            offset: s.offset,
                            nr_entries: 1,
                        };
                        let removed = self.data_inflight.remove(&s.id).unwrap();

                        match removed.t {
                            IdType::Pack(_hash, len) => {
                                self.so.entry_complete(s.id, e, Some(len), None);
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
                            if let IdType::Pack(hash, _len) = data.t {
                                let d = data.data.take().unwrap();
                                let pack_req = wire::Rpc::PackReq(data.id, hash);
                                if wire::write_request(
                                    &mut self.s,
                                    &pack_req,
                                    wire::WriteChunk::Data(d),
                                    w,
                                )? {
                                    rc = wire::IOResult::WriteWouldBlock;
                                }
                            }
                        } else {
                            panic!("How does this happen? {}", id);
                        }
                    }
                }
                wire::Rpc::PackResp(id, p) => {
                    {
                        let mut rq = self.req_q.lock().unwrap();
                        rq.data_written += p.data_written;
                        rq.hashes_written += p.hash_written;
                    }

                    let e = MapEntry::Data {
                        slab: p.slab,
                        offset: p.offset,
                        nr_entries: 1,
                    };
                    let removed = self.data_inflight.remove(&id).unwrap();

                    if let IdType::Pack(_hash, len) = removed.t {
                        self.so.entry_complete(id, e, Some(len), None);
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
                wire::Rpc::Error(_id, msg) => {
                    eprintln!("Unexpected error, server reported: {}", msg);
                    process::exit(2);
                }
                _ => {
                    eprint!("What are we not handling! {:?}", d);
                }
            }
            r.rezero();
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

        let mut r = wire::BufferMeta::new();
        let mut w = wire::OutstandingWrites::new();

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
                    let read_result = self.process_read(&mut r, &mut w);
                    match read_result {
                        Err(e) => {
                            eprintln!("Error on read, exiting! {:?}", e);
                            return Err(e);
                        }
                        Ok(r) => {
                            if r == wire::IOResult::WriteWouldBlock {
                                Client::_enable_poll_out(event_fd, fd, &mut poll_out)?;
                            }
                        }
                    }
                }

                if item_rdy.events & epoll::Events::EPOLLOUT.bits()
                    == epoll::Events::EPOLLOUT.bits()
                    && w.write(&mut self.s)? == IOResult::Ok
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

            let rc = self.process_request_queue(&mut w)?;
            if rc == wire::IOResult::Exit {
                break;
            } else if rc == wire::IOResult::WriteWouldBlock {
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
    let so = StreamOrder::new();

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
