use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::hash_index::*;
use crate::slab::*;
use crate::stream::*;

//------------------------------

pub trait Builder {
    fn next(&mut self, e: &MapEntry, len: u64, w: &mut Vec<u8>) -> Result<()>;

    // I'd rather pass plain 'self' here, but that won't work with runtime
    // polymorphism.
    fn complete(&mut self, w: &mut Vec<u8>) -> Result<()>;
}

pub struct MappingBuilder {
    // We insert a Pos instruction for every 'index_period' entries.
    index_period: u64,
    entries_emitted: u64,
    position: u64, // byte len of stream so far
    entry: Option<MapEntry>,
    vm_state: VMState,
}

fn pack_instrs<W: Write>(w: &mut W, instrs: &IVec) -> Result<()> {
    for i in instrs {
        i.pack(w)?;
    }
    Ok(())
}

// FIXME: bump up to 128
const INDEX_PERIOD: u64 = 128;

impl Default for MappingBuilder {
    fn default() -> Self {
        Self {
            index_period: INDEX_PERIOD,
            entries_emitted: 0,
            position: 0,
            entry: None,
            vm_state: VMState::default(),
        }
    }
}

impl MappingBuilder {
    fn encode_entry(&mut self, e: &MapEntry, instrs: &mut IVec) -> Result<()> {
        use MapEntry::*;

        match e {
            Fill { byte, len } => {
                self.vm_state.encode_fill(*byte, *len, instrs)?;
            }
            Unmapped { len } => {
                self.vm_state.encode_unmapped(*len, instrs)?;
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                self.vm_state
                    .encode_data(*slab, *offset, *nr_entries, instrs)?;
            }
            Partial {
                begin,
                end,
                slab,
                offset,
                nr_entries,
            } => {
                self.vm_state.encode_partial(*begin, *end, instrs)?;
                self.vm_state
                    .encode_data(*slab, *offset, *nr_entries, instrs)?;
            }
            Ref { .. } => return Err(anyhow!("MappingBuilder does not support Ref")),
        }

        self.entries_emitted += 1;
        if self.entries_emitted % self.index_period == 0 {
            self.vm_state.encode_pos(self.position, instrs)?;
        }

        Ok(())
    }
}

impl Builder for MappingBuilder {
    fn next(&mut self, e: &MapEntry, len: u64, w: &mut Vec<u8>) -> Result<()> {
        use MapEntry::*;

        if self.entry.is_none() {
            self.entry = Some(*e);
            self.position += len;
            return Ok(());
        }

        let mut instrs = Vec::with_capacity(4);
        match (self.entry.take().unwrap(), e) {
            (Fill { byte: b1, len: l1 }, Fill { byte: b2, len: l2 }) if b1 == *b2 => {
                self.entry = Some(Fill {
                    byte: b1,
                    len: l1 + l2,
                });
            }
            (Unmapped { len: l1 }, Unmapped { len: l2 }) => {
                self.entry = Some(Unmapped { len: l1 + l2 });
            }
            (
                Data {
                    slab: s1,
                    offset: o1,
                    nr_entries: n1,
                },
                Data {
                    slab: s2,
                    offset: o2,
                    nr_entries: n2,
                },
            ) => {
                if s1 == *s2 && o1 + n1 == *o2 {
                    self.entry = Some(Data {
                        slab: s1,
                        offset: o1,
                        nr_entries: n1 + n2,
                    });
                } else {
                    self.vm_state.encode_data(s1, o1, n1, &mut instrs)?;
                    self.entry = Some(Data {
                        slab: *s2,
                        offset: *o2,
                        nr_entries: *n2,
                    });
                }
            }
            (old_e, new_e) => {
                self.encode_entry(&old_e, &mut instrs)?;
                self.entry = Some(*new_e);
            }
        }

        self.position += len;
        pack_instrs(w, &instrs)
    }

    fn complete(&mut self, w: &mut Vec<u8>) -> Result<()> {
        if let Some(e) = self.entry.take() {
            let mut instrs = Vec::with_capacity(4);
            self.encode_entry(&e, &mut instrs)?;
            pack_instrs(w, &instrs)?;
        }

        Ok(())
    }
}

//------------------------------

pub struct DeltaBuilder {
    old_entries: StreamIter,
    old_entry: Option<MapEntry>, // unconsumed remnant from the old_entries

    // FIXME: wrap these two up together, lru cache, share somehow with pack?
    hashes_file: Arc<Mutex<SlabFile>>,
    slabs: BTreeMap<u32, Arc<ByIndex>>, // FIXME: why an Arc if they're not shared?

    builder: MappingBuilder,
}

impl DeltaBuilder {
    pub fn new(old_entries: StreamIter, hashes_file: Arc<Mutex<SlabFile>>) -> Self {
        Self {
            old_entries,
            old_entry: None,
            hashes_file,
            slabs: BTreeMap::new(),
            builder: MappingBuilder::default(),
        }
    }

    fn get_index_(&mut self, slab: u32) -> Result<Arc<ByIndex>> {
        let mut hashes_file = self.hashes_file.lock().unwrap();
        let hashes = hashes_file.read(slab)?;
        Ok(Arc::new(ByIndex::new(hashes)?))
    }

    fn get_index(&mut self, slab: u32) -> Result<Arc<ByIndex>> {
        let index = if let Some(index) = self.slabs.get(&slab) {
            index.clone()
        } else {
            let r = self.get_index_(slab)?;
            self.slabs.insert(slab, r.clone());
            r
        };

        Ok(index)
    }

    fn entry_len(&mut self, e: &MapEntry) -> Result<u64> {
        use MapEntry::*;

        match e {
            Fill { len, .. } => Ok(*len),
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                let index = self.get_index(*slab)?;
                let mut total_len = 0;
                for i in *offset..(offset + nr_entries) {
                    let (data_begin, data_end, _) = index.get(i as usize).unwrap();
                    total_len += data_end - data_begin;
                }
                Ok(total_len as u64)
            }
            Unmapped { len } => Ok(*len),
            Partial { begin, end, .. } => Ok((end - begin) as u64),
            Ref { len } => Ok(*len),
        }
    }

    fn split_entry(
        &mut self,
        e: &MapEntry,
        entry_len: u64,
        split_point: u64,
    ) -> (MapEntry, MapEntry) {
        use MapEntry::*;
        assert!(split_point < entry_len);

        match e {
            Fill { byte, .. } => (
                Fill {
                    byte: *byte,
                    len: split_point,
                },
                Fill {
                    byte: *byte,
                    len: entry_len - split_point,
                },
            ),
            Unmapped { .. } => (
                Unmapped { len: split_point },
                Unmapped {
                    len: entry_len - split_point,
                },
            ),
            Data {
                slab,
                offset,
                nr_entries,
            } => (
                Partial {
                    begin: 0,
                    end: split_point as u32,
                    slab: *slab,
                    offset: *offset,
                    nr_entries: *nr_entries,
                },
                Partial {
                    begin: split_point as u32,
                    end: entry_len as u32,
                    slab: *slab,
                    offset: *offset,
                    nr_entries: *nr_entries,
                },
            ),
            Partial {
                begin,
                end,
                slab,
                offset,
                nr_entries,
            } => (
                Partial {
                    begin: *begin,
                    end: *begin + split_point as u32,
                    slab: *slab,
                    offset: *offset,
                    nr_entries: *nr_entries,
                },
                Partial {
                    begin: *begin + split_point as u32,
                    end: *end,
                    slab: *slab,
                    offset: *offset,
                    nr_entries: *nr_entries,
                },
            ),
            Ref { .. } => (
                Ref { len: split_point },
                Ref {
                    len: entry_len - split_point,
                },
            ),
        }
    }

    fn next_old(&mut self) -> Result<Option<MapEntry>> {
        let mut maybe_entry = self.old_entry.take();

        if maybe_entry.is_none() {
            maybe_entry = match self.old_entries.next() {
                Some(re) => Some(re?),
                None => None,
            };
        }

        Ok(maybe_entry)
    }

    fn emit_old(&mut self, len: u64, w: &mut Vec<u8>) -> Result<()> {
        let mut remaining = len;

        while remaining > 0 {
            let maybe_entry = self.next_old()?;

            match maybe_entry {
                Some(e) => {
                    let e_len = self.entry_len(&e)?;

                    if remaining < e_len {
                        let (e1, e2) = self.split_entry(&e, e_len, remaining);
                        self.builder.next(&e1, remaining, w)?;
                        self.old_entry = Some(e2);
                        remaining = 0;
                    } else {
                        self.builder.next(&e, e_len, w)?;
                        remaining -= e_len;
                    }
                }
                None => return Err(anyhow!("expected short stream")),
            }
        }
        Ok(())
    }

    fn skip_old(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;

        while remaining > 0 {
            let maybe_entry = self.next_old()?;

            match maybe_entry {
                Some(e) => {
                    let e_len = self.entry_len(&e)?;

                    if remaining < e_len {
                        let (_, e2) = self.split_entry(&e, e_len, remaining);
                        self.old_entry = Some(e2);
                        remaining = 0;
                    } else {
                        remaining -= e_len;
                    }
                }
                None => return Err(anyhow!("expected short stream")),
            }
        }
        Ok(())
    }
}

impl Builder for DeltaBuilder {
    fn next(&mut self, e: &MapEntry, len: u64, w: &mut Vec<u8>) -> Result<()> {
        use MapEntry::*;

        match e {
            Ref { len } => self.emit_old(*len, w),
            _ => {
                self.skip_old(len)?;
                self.builder.next(e, len, w)
            }
        }
    }

    fn complete(&mut self, w: &mut Vec<u8>) -> Result<()> {
        self.builder.complete(w)
    }
}

//------------------------------

#[cfg(test)]
mod stream_tests {
    use super::*;

    fn mk_run(slab: u32, b: u32, e: u32) -> MapEntry {
        assert!((e - b) < u16::MAX as u32);
        MapEntry::Data {
            slab,
            offset: b,
            nr_entries: e - b,
        }
    }

    #[test]
    fn pack_unpack_cycle() {
        use MapEntry::*;

        let tests: Vec<Vec<MapEntry>> = vec![
            vec![],
            vec![Fill { byte: 0, len: 1 }],
            /*
             * Test doesn't work now we aggregate zeroes
            vec![
                Zero { len: 15 },
                Zero { len: 16 },
                Zero { len: 4095 },
                Zero { len: 4096 },
                Zero {
                    len: (4 * 1024 * 1024) - 1,
                },
                Zero {
                    len: 4 * 1024 * 1024,
                },
                Zero {
                    len: 16 * 1024 * 1024,
                },
            ],
            */
            vec![mk_run(0, 0, 4)],
            vec![mk_run(1, 1, 4)],
            vec![mk_run(1, 1, 1024)],
            vec![mk_run(1, 1, 16000)],
        ];

        for t in tests {
            // pack
            let mut buf: Vec<u8> = Vec::new();

            let mut builder = MappingBuilder::default();
            for e in &t {
                let len = 16; // FIXME: assume all entries are 16 bytes in length
                builder
                    .next(&e, len, &mut buf)
                    .expect("builder.next() failed");
            }
            builder
                .complete(&mut buf)
                .expect("builder.complete() failed");

            // unpack
            let (actual, _) = unpack(&buf[..]).expect("unpack failed");

            assert_eq!(*t, actual);
        }
    }
}

//-----------------------------------------
