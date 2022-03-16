use anyhow::Result;

//-----------------------------------------

pub type IoVec<'a> = Vec<&'a [u8]>;

pub trait IoVecHandler {
    fn handle_data(&mut self, iov: &IoVec) -> Result<()>;
    fn handle_gap(&mut self, len: u64) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

//-----------------------------------------
