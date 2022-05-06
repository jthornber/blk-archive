use anyhow::Result;

use crate::iovec::*;

//-----------------------------------------

pub trait Splitter {
    fn next_data(&mut self, buffer: Vec<u8>, handler: &mut dyn IoVecHandler) -> Result<()>;

    // Call this to indicate non contiguous data.
    fn next_break(&mut self, handler: &mut dyn IoVecHandler) -> Result<()>;
    fn complete(self, handler: &mut dyn IoVecHandler) -> Result<()>;
}

//-----------------------------------------

