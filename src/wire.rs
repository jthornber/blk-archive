use anyhow::{anyhow, Result};
use rkyv::util::AlignedVec;
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};
use std::io;
use std::io::IoSlice;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use crate::client;
use crate::config;
use crate::ipc::*;
use crate::stream_meta;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
struct Header {
    length: u64,
    size_meta: u64,
    magic: u64,
    version: u32,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct PackResp {
    pub slab: u32,
    pub offset: u32,
    pub data_written: u64,
    pub hash_written: u64,
}

pub const HEADER_MAGIC: u64 = 0x4D454F474D454F47;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct ArchiveEntry {
    pub id: String,
    pub pack_time: String,
    pub cfg: stream_meta::StreamConfig,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct DataRespYes {
    pub id: u64,
    pub slab: u32,
    pub offset: u32,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct DataReq {
    pub id: u64,
    pub hash: [u8; 32],
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum Rpc {
    Error(u64, String),

    ArchiveConfig(u64),
    ArchiveConfigResp(u64, config::Config),

    // Do we have the data?
    HaveDataReq(Vec<DataReq>), // One of more tuples of (request id, hash signature bytes)  // 24
    HaveDataRespYes(Vec<DataRespYes>), // One or more tuples of (request id, (slab #, slab offset) ) //24
    HaveDataRespNo(Vec<u64>),          // We don't have the data for the following requests // 24

    // Send the data we don't have
    PackReq(u64, [u8; 32]), // Request id, stream sequence number, hash signature, data 64
    PackResp(u64, Box<PackResp>), // The response to the Pack is the request id and the (slab #, slab offset) 40

    StreamSend(u64, Box<stream_meta::StreamMetaInfo>, Vec<u8>),
    StreamSendComplete(u64),

    StreamConfig(u64, String),
    StreamConfigResp(u64, Box<stream_meta::StreamConfig>),

    StreamRetrieve(u64, String),
    StreamRetrieveResp(u64, Option<StreamFiles>),

    RetrieveChunkReq(u64, client::IdType),
    RetrieveChunkResp(u64),

    ArchiveListReq(u64),
    ArchiveListResp(u64, Vec<ArchiveEntry>), // This may not scale well enough
}

pub fn id_get(rpc: &Rpc) -> u64 {
    match rpc {
        Rpc::PackReq(id, _hash) => *id,
        Rpc::PackResp(id, _location) => *id,
        Rpc::StreamSend(id, _sm, _stream_data) => *id,
        Rpc::StreamSendComplete(id) => *id,
        Rpc::ArchiveListReq(id) => *id,
        Rpc::ArchiveListResp(id, _entries) => *id,
        _ => 0,
    }
}

/*
impl fmt::Debug for Rpc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Rpc::Error(e, msg) => write!(f, "Rpc::Error({}:{}", e, msg),

            Rpc::ArchiveConfig(id) => write!(f, "Rpc::ArchiveConfig({})", id),
            Rpc::ArchiveConfigResp(id, _config) => write!(f, "Rpc::ArchiveConfigResp({})", id),

            Rpc::HaveDataRespYes(i) => write!(f, "Rpc::HaveDataReq({})", i.len()),
            Rpc::HaveDataRespNo(i) => write!(f, "Rpc::HaveDataYes({})", i.len()),

            Rpc::PackReq(i, hash, data) => {
                write!(f, "Rpc::PackReq({}:[{:?}]:{})", i, hash, data.len())
            }
            Rpc::PackResp(id, _t) => write!(f, "Rpc::PackResp({})", id),
            _ => write!(f, "Not implemented!"),
        }
    }
}*/

fn to_8(b: &[u8]) -> &[u8; 8] {
    b.try_into().expect("we need 8 bytes!")
}

fn get_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes(*to_8(b))
}

pub struct BufferMeta {
    pub buff: Vec<u8>,
    in_use: usize,
    pub meta_start: usize,
    pub meta_end: usize,
    pub data_start: usize,
    pub data_end: usize,
    packet_size: usize,
}

impl Default for BufferMeta {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferMeta {
    pub fn new() -> Self {
        BufferMeta {
            buff: vec![0; 1024 * 1024 * 64 + 1024],
            in_use: 0,
            meta_start: 0,
            meta_end: 0,
            data_start: 0,
            data_end: 0,
            packet_size: 0,
        }
    }

    pub fn rezero(&mut self) {
        self.in_use = 0;
        self.meta_start = 0;
        self.meta_end = 0;
        self.data_start = 0;
        self.data_end = 0;
        self.packet_size = 0;
    }

    pub fn data_len(&self) -> u64 {
        (self.data_end - self.data_start) as u64
    }
}

#[derive(PartialEq, Debug)]
pub enum IOResult {
    Ok,
    More,
    ReadWouldBlock,
    WriteWouldBlock,
    PeerGone,
    Exit,
}

pub fn read_request(s: &mut Box<dyn ReadAndWrite>, b: &mut BufferMeta) -> Result<IOResult> {
    let hdr_size = std::mem::size_of::<Header>();
    let mut buffer_size_checked = false;

    if b.packet_size == 0 {
        b.packet_size = 32;
    }

    loop {
        let amt_read = s.read(&mut b.buff[b.in_use..b.packet_size]);

        if let Err(e) = amt_read {
            match e.kind() {
                io::ErrorKind::WouldBlock => {
                    //eprintln!("Should we be getting a WOULD_BLOCK on the read {} {}?", b.in_use, b.packet_size);
                    return Ok(IOResult::ReadWouldBlock);
                }
                _ => return Err(e.into()),
            }
        }

        let amt_read = amt_read.unwrap();

        if amt_read == 0 {
            return Ok(IOResult::PeerGone);
        }

        b.in_use += amt_read;

        if !buffer_size_checked && !b.in_use >= hdr_size {
            assert_eq!(get_u64(&b.buff[16..24]), HEADER_MAGIC);
            b.packet_size = get_u64(&b.buff[0..8]) as usize;
            {
                if b.packet_size > b.buff.capacity() {
                    b.buff.resize(b.packet_size, 0);
                }
            }
            buffer_size_checked = true;
        }

        if b.in_use == b.packet_size {
            break;
        } else {
            assert!(b.in_use < b.packet_size);
        }
    }

    let meta_size = get_u64(&b.buff[8..16]) as usize;

    b.meta_start = hdr_size;
    b.meta_end = b.meta_start + meta_size;

    let data_size = b.packet_size - hdr_size - meta_size;
    if data_size > 0 {
        b.data_start = hdr_size + meta_size;
        b.data_end = b.packet_size;
    }

    Ok(IOResult::Ok)
}

enum EntryType<'a> {
    Slice(IoSlice<'a>),
    Data(Vec<u8>),
    AlignedVec(AlignedVec),
    WriteChunk(WriteChunk),
}

impl<'a> EntryType<'a> {
    fn len(&self) -> usize {
        match self {
            EntryType::Data(d) => d.len(),
            EntryType::Slice(d) => d.len(),
            EntryType::AlignedVec(d) => d.len(),
            EntryType::WriteChunk(d) => d.len(),
        }
    }

    fn drain(&mut self, amount: usize) -> Self {
        match self {
            EntryType::Data(d) => {
                d.drain(0..amount);
                EntryType::Data(d.to_vec())
            }
            EntryType::Slice(d) => {
                IoSlice::advance(d, amount);
                EntryType::Slice(*d)
            }
            EntryType::AlignedVec(d) => {
                let t = d[amount..].to_vec();
                EntryType::Data(t)
            }
            EntryType::WriteChunk(d) => match d {
                WriteChunk::ArcData(d, start, end) => {
                    let n_start = if *start + amount < *end {
                        *start + amount
                    } else {
                        *end
                    };
                    EntryType::WriteChunk(WriteChunk::ArcData(Arc::clone(d), n_start, *end))
                }
                WriteChunk::Data(d) => {
                    d.drain(0..amount);
                    EntryType::WriteChunk(WriteChunk::Data(d.to_vec()))
                }
                WriteChunk::None => EntryType::WriteChunk(WriteChunk::None),
            },
        }
    }
}

pub struct OutstandingWrites<'a> {
    e: Vec<EntryType<'a>>,
}

impl<'a> Default for OutstandingWrites<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> OutstandingWrites<'a> {
    pub fn new() -> Self {
        OutstandingWrites { e: Vec::new() }
    }

    pub fn add_owned_vec(&mut self, data: Vec<u8>) {
        self.e.push(EntryType::Data(data));
    }

    pub fn add_owned_write_chunk(&mut self, data: WriteChunk) {
        self.e.push(EntryType::WriteChunk(data))
    }

    // Add a new IoSlice to the collection
    pub fn add_slice(&mut self, data: &'a [u8]) {
        self.e.push(EntryType::Slice(IoSlice::new(data)));
    }

    pub fn add_owned_aligned(&mut self, data: AlignedVec) {
        self.e.push(EntryType::AlignedVec(data));
    }

    // Write as much data as we can, returns true when we're empty, false implies we got
    // an error that we would block
    pub fn write(&mut self, c: &mut Box<dyn ReadAndWrite>) -> Result<IOResult> {
        let mut slices: Vec<IoSlice<'_>> = Vec::new();

        // Why should we need this, spurious pollout?
        if self.e.is_empty() {
            return Ok(IOResult::Ok);
        }

        for i in &self.e {
            match i {
                EntryType::Slice(ios) => {
                    slices.push(*ios);
                }
                EntryType::Data(d) => {
                    slices.push(IoSlice::new(d));
                }
                EntryType::AlignedVec(av) => {
                    slices.push(IoSlice::new(av));
                }
                EntryType::WriteChunk(wc) => match wc {
                    WriteChunk::ArcData(d, start, end) => {
                        slices.push(IoSlice::new(&d[*start..*end]));
                    }
                    WriteChunk::Data(d) => {
                        slices.push(IoSlice::new(d));
                    }
                    WriteChunk::None => {}
                },
            }
        }

        let mut t = 0;
        for s in &slices {
            t += s.len();
        }

        assert!(t != 0);

        let amt_written = c.write_vectored(&slices[..]);
        if let Err(amt_written) = amt_written {
            match amt_written.kind() {
                io::ErrorKind::WouldBlock => return Ok(IOResult::WriteWouldBlock),
                _ => return Err(amt_written.into()),
            }
        }

        let mut amt_written = amt_written.unwrap();

        let mut indexs_to_remove = 0;
        for i in &mut self.e {
            if amt_written >= i.len() {
                indexs_to_remove += 1;
                amt_written -= i.len();
                if amt_written == 0 {
                    break;
                }
            } else {
                *i = i.drain(amt_written);
                break;
            }
        }
        self.e.drain(0..indexs_to_remove);

        if self.e.is_empty() {
            Ok(IOResult::Ok)
        } else {
            Ok(IOResult::More)
        }
    }
}

pub enum WriteChunk {
    None,
    ArcData(Arc<Vec<u8>>, usize, usize),
    Data(Vec<u8>),
}

impl WriteChunk {
    pub fn len(&self) -> usize {
        match self {
            WriteChunk::None => 0,
            WriteChunk::ArcData(_d, start, end) => end - start,
            WriteChunk::Data(d) => d.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn write_request(
    c: &mut Box<dyn ReadAndWrite>,
    m: &Rpc,
    d: WriteChunk,
    pending: &mut OutstandingWrites,
) -> Result<bool> {
    let m_bytes = rkyv::to_bytes::<Error>(m).unwrap();

    let p = Header {
        length: (std::mem::size_of::<Header>() + m_bytes.len() + d.len()) as u64,
        size_meta: m_bytes.len() as u64,
        magic: HEADER_MAGIC,
        version: 0,
    };

    let p_bytes = rkyv::to_bytes::<Error>(&p).unwrap();
    pending.add_owned_aligned(p_bytes);
    pending.add_owned_aligned(m_bytes);

    if !d.is_empty() {
        pending.add_owned_write_chunk(d);
    }

    // We basically want to write until we are empty or get a Would block.
    loop {
        let write_result = pending.write(c)?;

        match write_result {
            IOResult::WriteWouldBlock => return Ok(true),
            IOResult::More => continue,
            IOResult::Ok => return Ok(false),
            _ => {
                return Err(anyhow!(format!(
                    "Write to socket returned {:?} returning error!",
                    write_result
                )))
            }
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct StreamFiles {
    pub stream: Vec<u8>,
    pub offsets: Vec<u8>,
}

pub fn stream_files(stream: Vec<u8>, offsets: Vec<u8>) -> StreamFiles {
    StreamFiles { stream, offsets }
}

pub fn stream_files_bytes(sf: &StreamFiles) -> Vec<u8> {
    let p_bytes = rkyv::to_bytes::<Error>(sf).unwrap();
    p_bytes.to_vec()
}

pub fn bytes_to_stream_files(b: &[u8]) -> &ArchivedStreamFiles {
    let r: &ArchivedStreamFiles = rkyv::access::<ArchivedStreamFiles, Error>(b).unwrap();
    r
}

#[test]
fn test_ser_des() {}
