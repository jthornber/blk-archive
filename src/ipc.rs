use epoll;

use anyhow::Result;

use std::io::{Read as IoRead, Write as IoWrite};
use std::net::TcpListener;
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use tempfile::{Builder, TempDir};

pub trait ReadAndWrite: IoRead + IoWrite + AsRawFd + Send {}

impl<T: IoRead + IoWrite + AsRawFd + Send> ReadAndWrite for T {}

pub fn read_event(fd: i32) -> epoll::Event {
    epoll::Event {
        data: fd as u64,
        events: epoll::Events::EPOLLIN.bits()
            | epoll::Events::EPOLLERR.bits()
            | epoll::Events::EPOLLHUP.bits(),
    }
}

pub fn create_connected_socket(
    addr: String,
) -> Result<Box<dyn ReadAndWrite + Send>, std::io::Error> {
    if addr.starts_with('/') {
        let s = UnixStream::connect(addr)?;
        s.set_nonblocking(true)?;
        Ok(Box::new(s))
    } else {
        let s = TcpStream::connect(addr)?;
        s.set_nonblocking(true)?;
        s.set_nodelay(true)
            .expect("unable to disable nagle, should we be disabling nagle?");
        Ok(Box::new(s))
    }
}

pub fn create_listening_socket(addr: &String) -> Result<Box<dyn Listening + Send>, std::io::Error> {
    if addr.starts_with('/') {
        let s = UnixListeningWrap::new(addr.to_string())?;
        Ok(Box::new(s))
    } else {
        let s = TcpListeningWrap::new(addr.to_string())?;
        Ok(Box::new(s))
    }
}

pub trait Listening: AsRawFd + Send {
    fn accept(&self) -> std::io::Result<(Box<dyn ReadAndWrite>, String)>;
}

struct TcpListeningWrap {
    s: TcpListener,
}

struct UnixListeningWrap {
    s: UnixListener,
}

impl TcpListeningWrap {
    fn new(addr: String) -> std::io::Result<Self> {
        Ok(Self {
            s: TcpListener::bind(addr)?,
        })
    }
}

impl UnixListeningWrap {
    fn new(addr: String) -> std::io::Result<Self> {
        Ok(Self {
            s: UnixListener::bind(addr)?,
        })
    }
}

impl Listening for TcpListeningWrap {
    fn accept(&self) -> std::io::Result<(Box<dyn ReadAndWrite>, String)> {
        let (s, a) = self.s.accept()?;
        s.set_nodelay(true)?;
        s.set_nonblocking(true)?;
        Ok((Box::new(s), a.to_string()))
    }
}

impl AsRawFd for TcpListeningWrap {
    fn as_raw_fd(&self) -> RawFd {
        self.s.as_raw_fd()
    }
}

impl Listening for UnixListeningWrap {
    fn accept(&self) -> std::io::Result<(Box<dyn ReadAndWrite>, String)> {
        let (s, a) = self.s.accept()?;
        s.set_nonblocking(true)?;
        let addr = a.as_pathname().unwrap_or_else(|| Path::new("unknown"));
        Ok((Box::new(s), String::from(addr.to_str().unwrap())))
    }
}

impl AsRawFd for UnixListeningWrap {
    fn as_raw_fd(&self) -> RawFd {
        self.s.as_raw_fd()
    }
}

pub fn create_unix_ipc_dir() -> anyhow::Result<TempDir> {
    Ok(Builder::new()
        .prefix("blkarchive_")
        .rand_bytes(10)
        .tempdir()?)
}
