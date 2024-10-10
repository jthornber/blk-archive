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

pub fn iov_len_(iov: &IoVec) -> u64 {
    let mut len = 0;
    for v in iov {
        len += v.len() as u64;
    }

    len
}

pub fn first_b_(iov: &IoVec) -> Option<u8> {
    if let Some(v) = iov.iter().find(|v| !v.is_empty()) {
        return Some(v[0]);
    }

    None
}

pub fn all_same(iov: &IoVec) -> Option<u8> {
    if let Some(first_b) = first_b_(iov) {
        for v in iov.iter() {
            for b in *v {
                if *b != first_b {
                    return None;
                }
            }
        }
        Some(first_b)
    } else {
        None
    }
}

//-----------------------------------------
