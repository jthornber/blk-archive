use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::OpenOptions;
use std::path::Path;

//------------------------------------------------

#[derive(Default)]
pub struct SlabOffsets {
    // FIXME: I don't like having this public
    pub offsets: Vec<u64>,
}

impl SlabOffsets {
    pub fn read_offset_file<P: AsRef<Path>>(p: P) -> Result<Self> {
        let r = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(p)
            .context("opening offset file")?;

        let len = r.metadata().context("offset metadata")?.len();
        let mut r = std::io::BufReader::new(r);
        let nr_entries = len / std::mem::size_of::<u64>() as u64;
        let mut offsets = Vec::with_capacity(nr_entries as usize);
        for _ in 0..nr_entries {
            offsets.push(
                r.read_u64::<LittleEndian>()
                    .context("offsets read failed")?,
            );
        }

        Ok(Self { offsets })
    }

    pub fn write_offset_file<P: AsRef<Path>>(&self, p: P) -> Result<()> {
        let mut w = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .truncate(true)
            .open(p)?;

        for o in &self.offsets {
            w.write_u64::<LittleEndian>(*o)?;
        }

        Ok(())
    }
}

//------------------------------------------------
