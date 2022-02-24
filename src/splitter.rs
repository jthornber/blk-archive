use anyhow::Result;

use crate::iovec::*;

//-----------------------------------------

pub trait Splitter {
    fn next(&mut self, buffer: Vec<u8>, handler: &mut dyn IoVecHandler) -> Result<()>;
    fn complete(self, handler: &mut dyn IoVecHandler) -> Result<()>;
}

//-----------------------------------------

