use anyhow::Result;
use bincode::{config, Decode, Encode};
use std::collections::VecDeque;
use std::io;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;

use crate::ipc::*;
use crate::stream_meta;

const CONFIG: config::Configuration<config::BigEndian, config::Fixint> = config::standard()
    .with_fixed_int_encoding()
    .with_big_endian(); // We can certainly remove this to get little endian as most of the arches
                        // are little endian today, historically on-wire has been big endian

#[derive(Encode, Decode, PartialEq, Debug)]
struct Packet {
    length: u64,
    version: u32,
    payload: Vec<u8>, // The payload will be compressed
    payload_crc: u32,
}

#[derive(Encode, Decode, Debug, PartialEq)]
pub enum Rpc {
    Error(u64, String),

    // Do we have the data?
    HaveDataReq(Vec<(u64, [u8; 32])>), // One of more tuples of (request id, hash signature bytes)
    HaveDataRespYes(Vec<(u64, (u32, u32))>), // One or more tuples of (request id, (slab #, slab offset) )
    HaveDataRespNo(Vec<u64>),                // We don't have the data for the following requests

    // Send the data we don't have
    PackReq(u64, [u8; 32], Vec<u8>), // Request id, stream sequence number, hash signature, data
    PackResp(u64, ((u32, u32), u64)), // The response to the Pack is the request id and the (slab #, slab offset)

    StreamSend(u64, stream_meta::StreamMetaInfo, Vec<u8>, Vec<u8>),
    StreamSendComplete(u64),

    UnPackReq(u64, String), // stream id, Request id (returned from server, so client can correlate)
    UnPackResp(u64, Vec<u8>), // Request id, data
}

#[derive(PartialEq, Debug)]
pub enum IORequest {
    Ok,
    WouldBlock,
    PeerGone,
    Exit,
}

fn payload_to_rpc(packet: Packet) -> Rpc {
    let (result, _size): (Rpc, usize) =
        bincode::decode_from_slice(&packet.payload, CONFIG).unwrap();
    result
}

pub fn read(s: &mut TcpStream) -> Result<Option<Rpc>, io::Error> {
    let mut packet_length = [0; 8];

    let r = s.read_exact(&mut packet_length);
    if let Err(r) = r {
        match r.kind() {
            io::ErrorKind::WouldBlock => return Ok(None),
            _ => return Err(r),
        }
    }

    let bytes_to_read = u64::from_be_bytes(packet_length);
    let mut buffer = vec![0u8; bytes_to_read as usize];

    // Place the length in the main buffer that we already read from the stream and read the
    // rest
    buffer[..8].clone_from_slice(&packet_length);
    s.read_exact(&mut buffer[8..])?;

    Ok(Some(bytes_to_rpc(&buffer)))
}

fn to_8(b: &[u8]) -> &[u8; 8] {
    b.try_into().expect("we need 8 bytes!")
}

fn get_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes(*to_8(b))
}

fn get_hdr_len(b: &mut VecDeque<u8>) -> usize {
    let slice = b.make_contiguous();
    get_u64(&slice[0..8]) as usize
}

fn _read_into_rpc(b: &mut VecDeque<u8>) -> Result<Option<Vec<Rpc>>, io::Error> {
    let mut rc = Vec::new();

    while b.len() > 8 {
        let hdr_len = get_hdr_len(b);

        assert!(hdr_len < 1024 * 1024 * 5);

        if b.len() >= hdr_len {
            let mut packet = b.drain(0..hdr_len).collect::<VecDeque<_>>();
            let packet_slice = packet.make_contiguous();
            rc.push(bytes_to_rpc(packet_slice));
        } else {
            break;
        }
    }

    Ok(if !rc.is_empty() { Some(rc) } else { None })
}

pub fn read_using_buffer(
    s: &mut Box<dyn ReadAndWrite>,
    b: &mut VecDeque<u8>,
) -> Result<Option<Vec<Rpc>>, io::Error> {
    let mut buffer = [0; 1024 * 1024];
    let mut total_read = 0;

    loop {
        let r = s.read(&mut buffer)?;
        total_read += r;
        b.extend(&buffer[0..r]);
        if r < buffer.len() {
            break;
        }
    }

    // This indicates that the socket was closed cleanly from other side, our expectation is
    // we don't have any data left in our buffer
    if total_read == 0 {
        println!("We read zero, our vecqueue = {}", b.len());
        if b.len() >= 8 {
            println!("Our header len = {}", get_hdr_len(b));
        }
        assert!(b.is_empty());
        return Err(io::ErrorKind::NotConnected.into());
    }

    _read_into_rpc(b)
}

pub fn write_rpc_panic(s: &mut Box<dyn ReadAndWrite>, rpc: Rpc) {
    let bytes = rpc_to_bytes(&rpc);
    s.write_all(&bytes).unwrap();
}

pub fn write(s: &mut Box<dyn ReadAndWrite>, rpc: Rpc, wb: &mut VecDeque<u8>) -> Result<bool> {
    let bytes = rpc_to_bytes(&rpc);
    wb.extend(bytes);
    write_buffer(s, wb)
}

pub fn write_buffer(
    s: &mut Box<dyn ReadAndWrite>,
    write_buffer: &mut VecDeque<u8>,
) -> Result<bool> {
    let amt_written = s.write(write_buffer.make_contiguous());
    let rc = match amt_written {
        Err(r) => match r.kind() {
            io::ErrorKind::WouldBlock => true,
            _ => {
                return Err(r.into());
            }
        },
        Ok(bytes_written) => {
            write_buffer.drain(0..bytes_written);
            false
        }
    };
    Ok(rc)
}

pub fn bytes_to_rpc(d: &[u8]) -> Rpc {
    let (result, _size): (Packet, usize) = bincode::decode_from_slice(d, CONFIG).unwrap();
    payload_to_rpc(result)
}

pub fn rpc_to_bytes(cmd: &Rpc) -> Vec<u8> {
    let c_bytes = bincode::encode_to_vec(cmd, CONFIG).unwrap();
    let packet = Packet {
        length: 8 + 4 + 8 + c_bytes.len() as u64 + 4, // length(8), version(4), payload(8 + #bytes), payload_crc(4)
        version: 1,
        payload_crc: 0,
        payload: c_bytes,
    };

    let packet_len = packet.length;
    let r = bincode::encode_to_vec(packet, CONFIG).unwrap();
    assert_eq!(packet_len, r.len() as u64);
    r
}

#[test]
fn test_ser_des() {
    let r = Rpc::HaveDataReq(vec![
        (
            0xFE,
            [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31,
            ],
        ),
        (
            0xFF,
            [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31,
            ],
        ),
    ]);

    let ser = rpc_to_bytes(&r);
    let d = bytes_to_rpc(&ser);

    assert!(r == d);
    match d {
        Rpc::HaveDataReq(v) => {
            for i in v.iter() {
                println!("{} - {:02X?}", i.0, i.1)
            }
        }
        _ => {
            assert!(false, "de-serialized RPC was not a Rpc::HaveDataReq");
        }
    }
}
