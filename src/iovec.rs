use anyhow::Result;

//-----------------------------------------

pub type IoVec<'a> = Vec<&'a [u8]>;

pub trait IoVecHandler {
    fn handle_data(&mut self, iov: &IoVec) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

//-----------------------------------------
