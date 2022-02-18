use anyhow::Result;

//-----------------------------------------

pub type IoVec<'a> = Vec<&'a [u8]>;

pub trait IoVecHandler {
    fn handle(&mut self, iov: &IoVec) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

pub trait Splitter {
    fn next(&mut self, buffer: Vec<u8>, handler: &mut dyn IoVecHandler) -> Result<()>;
    fn complete(self, handler: &mut dyn IoVecHandler) -> Result<()>;
}

//-----------------------------------------

