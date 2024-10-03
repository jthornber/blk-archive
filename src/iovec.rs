use anyhow::Result;

//-----------------------------------------

pub type IoVec<'a> = Vec<&'a [u8]>;

pub trait IoVecHandler {
    fn handle_data(&mut self, iov: &IoVec) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

pub fn io_vec_to_vec(iov: &IoVec) -> Vec<u8> {
    let mut rc = Vec::new();
    for v in iov {
        rc.extend_from_slice(v);
    }
    rc
}

//-----------------------------------------
