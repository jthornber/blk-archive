use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use io::Write;
use nom::{multi::*, number::complete::*, IResult};
use num_enum::TryFromPrimitive;
use std::convert::TryFrom;
use std::io;

//-----------------------------------------

#[derive(Copy, Clone, Debug)]
enum MapInstruction {
    Zero4 { len: u8 },
    Zero12 { len: u16 },
    Zero20 { len: u32 },
    Zero36 { len: u64 },

    Slab16 { slab: u16 },
    Slab32 { slab: u32 },

    // FIXME: investigate how useful signed deltas would be
    SlabDelta4 { delta: u8 },
    SlabDelta12 { delta: u16 },

    Offset4 { offset: u8 },
    Offset12 { offset: u16 },
    Offset20 { offset: u32 },

    OffsetDelta4 { delta: u8 },
    OffsetDelta12 { delta: u16 },

    Emit4 { len: u8 },
    Emit12 { len: u16 },
    Emit20 { len: u32 },
}

// 4 bit tags
#[derive(Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
enum MapTag {
    TagZero4 = 0,
    TagZero12 = 1,
    TagZero20 = 2,
    TagZero36 = 3,

    TagSlab16 = 4,
    TagSlab32 = 5,

    TagSlabDelta4 = 6,
    TagSlabDelta12 = 7,

    TagOffset4 = 8,
    TagOffset12 = 9,
    TagOffset20 = 10,

    TagOffsetDelta4 = 11,
    TagOffsetDelta12 = 12,

    TagEmit4 = 13,
    TagEmit12 = 14,
    TagEmit20 = 15,
}

fn pack_tag(tag: MapTag, nibble: u8) -> u8 {
    ((tag as u8) << 4) | nibble
}

fn unpack_tag(b: u8) -> (MapTag, u8) {
    let tag = MapTag::try_from(b >> 4).expect("bad tag");
    let nibble = b & 0xf;
    (tag, nibble)
}

impl MapInstruction {
    pub fn pack<W: Write>(&self, w: &mut W) -> Result<()> {
        use MapInstruction::*;
        use MapTag::*;
        match self {
            Zero4 { len } => {
                assert!(*len != 0);
                assert!((len & 0xf0) == 0);
                w.write_u8(pack_tag(TagZero4, *len))?;
            }
            Zero12 { len } => {
                assert!(*len != 0);
                assert!((len & 0xf000) == 0);
                w.write_u8(pack_tag(TagZero12, (*len & 0xf) as u8))?;
                w.write_u8((len >> 4) as u8)?;
            }
            Zero20 { len } => {
                assert!(*len != 0);
                assert!((len & 0xf00000) == 0);
                w.write_u8(pack_tag(TagZero20, (*len & 0xf) as u8))?;
                w.write_u16::<LittleEndian>((len >> 4) as u16)?;
            }
            Zero36 { len } => {
                assert!(*len != 0);
                w.write_u8(pack_tag(TagZero36, (*len & 0xf) as u8))?;
                w.write_u32::<LittleEndian>((len >> 4) as u32)?;
            }
            Slab16 { slab } => {
                w.write_u8(pack_tag(TagSlab16, 0))?;
                w.write_u16::<LittleEndian>(*slab)?;
            }
            Slab32 { slab } => {
                w.write_u8(pack_tag(TagSlab32, 0))?;
                w.write_u32::<LittleEndian>(*slab)?;
            }
            SlabDelta4 { delta } => {
                assert!(*delta != 0);
                w.write_u8(pack_tag(TagSlabDelta4, *delta as u8))?;
            }
            SlabDelta12 { delta } => {
                assert!(*delta != 0);
                w.write_u8(pack_tag(TagSlabDelta12, (*delta & 0xf) as u8))?;
                w.write_u8((*delta >> 4) as u8)?;
            }
            Offset4 { offset } => {
                w.write_u8(pack_tag(TagOffset4, *offset))?;
            }
            Offset12 { offset } => {
                w.write_u8(pack_tag(TagOffset12, (*offset & 0xf) as u8))?;
                w.write_u8((*offset >> 4) as u8)?;
            }
            Offset20 { offset } => {
                w.write_u8(pack_tag(TagOffset20, (*offset & 0xf) as u8))?;
                w.write_u16::<LittleEndian>((*offset >> 4) as u16)?;
            }
            OffsetDelta4 { delta } => {
                assert!(*delta != 0);
                w.write_u8(pack_tag(TagOffsetDelta4, *delta))?;
            }
            OffsetDelta12 { delta } => {
                assert!(*delta != 0);
                w.write_u8(pack_tag(TagOffsetDelta12, (*delta & 0xf) as u8))?;
                w.write_u8((*delta >> 4) as u8)?;
            }
            Emit4 { len } => {
                w.write_u8(pack_tag(TagEmit4, *len))?;
            }
            Emit12 { len } => {
                w.write_u8(pack_tag(TagEmit12, (len & 0xf) as u8))?;
                w.write_u8((len >> 4) as u8)?;
            }
            Emit20 { len } => {
                w.write_u8(pack_tag(TagEmit20, (len & 0xf) as u8))?;
                w.write_u16::<LittleEndian>((len >> 4) as u16)?;
            }
        }
        Ok(())
    }

    // Returns None if we've reached the end of input
    fn unpack(input: &[u8]) -> IResult<&[u8], Self> {
        use MapInstruction::*;
        use MapTag::*;

        let (input, b) = le_u8(input)?;
        let (tag, nibble) = unpack_tag(b);

        let v = match tag {
            TagZero4 => (input, Zero4 { len: nibble }),
            TagZero12 => {
                let (input, b) = le_u8(input)?;
                (
                    input,
                    Zero12 {
                        len: ((b as u16) << 4) | (nibble as u16),
                    },
                )
            }
            TagZero20 => {
                let (input, w) = le_u16(input)?;
                (
                    input,
                    Zero20 {
                        len: ((w as u32) << 4) | (nibble as u32),
                    },
                )
            }
            TagZero36 => {
                let (input, w) = le_u32(input)?;
                (
                    input,
                    Zero36 {
                        len: ((w as u64) << 4) | (nibble as u64),
                    },
                )
            }
            TagSlab16 => {
                let (input, w) = le_u16(input)?;
                (input, Slab16 { slab: w })
            }
            TagSlab32 => {
                let (input, w) = le_u32(input)?;
                (input, Slab32 { slab: w })
            }
            TagSlabDelta4 => (input, SlabDelta4 { delta: nibble }),
            TagSlabDelta12 => {
                let (input, w) = le_u8(input)?;
                (
                    input,
                    SlabDelta12 {
                        delta: ((w as u16) << 4) | (nibble as u16),
                    },
                )
            }
            TagOffset4 => (input, Offset4 { offset: nibble }),
            TagOffset12 => {
                let (input, w) = le_u8(input)?;
                (
                    input,
                    Offset12 {
                        offset: ((w as u16) << 4) | (nibble as u16),
                    },
                )
            }
            TagOffset20 => {
                let (input, w) = le_u16(input)?;
                (
                    input,
                    Offset20 {
                        offset: ((w as u32) << 4) | (nibble as u32),
                    },
                )
            }
            TagOffsetDelta4 => (input, OffsetDelta4 { delta: nibble }),
            TagOffsetDelta12 => {
                let (input, w) = le_u8(input)?;
                (
                    input,
                    OffsetDelta12 {
                        delta: ((w as u16) << 4) | (nibble as u16),
                    },
                )
            }
            TagEmit4 => (input, Emit4 { len: nibble }),
            TagEmit12 => {
                let (input, w) = le_u8(input)?;
                (
                    input,
                    Emit12 {
                        len: ((w as u16) << 4) | (nibble as u16),
                    },
                )
            }
            TagEmit20 => {
                let (input, w) = le_u16(input)?;
                (
                    input,
                    Emit20 {
                        len: ((w as u32) << 4) | (nibble as u32),
                    },
                )
            }
        };

        Ok(v)
    }
}

//-----------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapEntry {
    Zero { len: u64 },
    Data { slab: u64, offset: u32 },
}

// FIXME: support runs of zeroed regions
struct Run {
    slab: u64,
    offset: u32,
    len: u16,
}

#[derive(Default)]
struct VMState {
    slab: u64,
    offset: u32,
}

impl VMState {
    fn encode_zero(&mut self, len: u64, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if len < 0x10 {
            instrs.push(Zero4 { len: len as u8 });
        } else if len < 0x1000 {
            instrs.push(Zero12 { len: len as u16 });
        } else if len < 0x100000 {
            instrs.push(Zero20 { len: len as u32 });
        } else if len < 0x10000000 {
            instrs.push(Zero36 { len });
        } else {
            return Err(anyhow!("Zero len too long"));
        }
        Ok(())
    }

    fn encode_slab(&mut self, slab: u64, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if slab != self.slab {
            let delta: u64 = slab.wrapping_sub(self.slab);
            if delta < 0x10 {
                instrs.push(SlabDelta4 { delta: delta as u8 });
            } else if delta < 0x1000 {
                instrs.push(SlabDelta12 {
                    delta: delta as u16,
                });
            } else if slab <= u16::MAX as u64 {
                instrs.push(Slab16 { slab: slab as u16 });
            } else if slab <= u32::MAX as u64 {
                instrs.push(Slab32 { slab: slab as u32 });
            } else {
                return Err(anyhow!("slab index too large"));
            }
            self.slab = slab;
        }
        Ok(())
    }

    fn encode_offset(&mut self, offset: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if offset != self.offset {
            let delta: u32 = offset.wrapping_sub(self.offset);
            if delta < 0x10 {
                instrs.push(OffsetDelta4 { delta: delta as u8 });
            } else if delta < 0x1000 {
                instrs.push(OffsetDelta12 {
                    delta: delta as u16,
                });
            } else if offset < 0x10 {
                instrs.push(Offset4 {
                    offset: offset as u8,
                });
            } else if offset < 0x1000 {
                instrs.push(Offset12 {
                    offset: offset as u16,
                });
            } else if offset < 0x100000 {
                instrs.push(Offset20 { offset });
            } else {
                return Err(anyhow!("offset too large"));
            }
            self.offset = offset;
        }
        Ok(())
    }

    fn encode_emit(&mut self, len: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if len < 0x10 {
            instrs.push(Emit4 { len: len as u8 });
        } else if len < 0x1000 {
            instrs.push(Emit12 { len: len as u16 });
        } else if len < 0x100000 {
            instrs.push(Emit20 { len });
        }
        self.offset += len;
        Ok(())
    }

    fn encode_run(&mut self, run: &Run, instrs: &mut IVec) -> Result<()> {
        self.encode_slab(run.slab, instrs)?;
        self.encode_offset(run.offset, instrs)?;
        self.encode_emit(run.len as u32, instrs)?;
        Ok(())
    }
}

#[derive(Default)]
pub struct MappingBuilder {
    run: Option<Run>,
    vm_state: VMState,
}

type IVec = Vec<MapInstruction>;

fn pack_instrs<W: Write>(w: &mut W, instrs: &IVec) -> Result<()> {
    for i in instrs {
        i.pack(w)?;
    }
    Ok(())
}

impl MappingBuilder {
    pub fn next<W: Write>(&mut self, e: &MapEntry, w: &mut W) -> Result<()> {
        use MapEntry::*;

        let mut instrs = Vec::new();
        match e {
            Zero { len } => {
                let mut run = None;
                std::mem::swap(&mut run, &mut self.run);
                if let Some(run) = run {
                    self.vm_state.encode_run(&run, &mut instrs)?;
                }
                self.vm_state.encode_zero(*len, &mut instrs)?;
            }
            Data { slab, offset } => {
                let new_run = Run {
                    slab: *slab,
                    offset: *offset,
                    len: 0, // len gets incremented to 1 after insert
                };
                let run = self.run.get_or_insert(new_run);
                if (*slab == run.slab) && (*offset == (run.offset + run.len as u32)) {
                    run.len += 1;
                } else {
                    self.vm_state.encode_run(run, &mut instrs)?;
                    let new_run = Run {
                        slab: *slab,
                        offset: *offset,
                        len: 1,
                    };
                    self.run = Some(new_run);
                }
            }
        }

        pack_instrs(w, &instrs)
    }

    pub fn complete<W: Write>(mut self, w: &mut W) -> Result<()> {
        let mut instrs = Vec::new();
        if let Some(run) = self.run {
            self.vm_state.encode_run(&run, &mut instrs)?;
            pack_instrs(w, &instrs)?;
        }
        Ok(())
    }
}

//-----------------------------------------

#[derive(Default)]
pub struct MappingUnpacker {
    vm_state: VMState,
}

impl MappingUnpacker {
    fn emit_run(&mut self, r: &mut Vec<MapEntry>, len: usize) {
        for _ in 0..len {
            r.push(MapEntry::Data {
                slab: self.vm_state.slab,
                offset: self.vm_state.offset,
            });
            self.vm_state.offset += 1;
        }
    }

    pub fn unpack(&mut self, buf: &[u8]) -> Result<Vec<MapEntry>> {
        use MapInstruction::*;

        let mut r = Vec::new();
        let (_, instrs) = many0(MapInstruction::unpack)(buf)
            .map_err(|_| anyhow!("unable to parse MappingInstruction"))?;

        for instr in instrs {
            match instr {
                Zero4 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }
                Zero12 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }
                Zero20 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }
                Zero36 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }
                Slab16 { slab } => {
                    self.vm_state.slab = slab as u64;
                }
                Slab32 { slab } => {
                    self.vm_state.slab = slab as u64;
                }
                SlabDelta4 { delta } => {
                    self.vm_state.slab += delta as u64;
                }
                SlabDelta12 { delta } => {
                    self.vm_state.slab += delta as u64;
                }
                Offset4 { offset } => {
                    self.vm_state.offset = offset as u32;
                }
                Offset12 { offset } => {
                    self.vm_state.offset = offset as u32;
                }
                Offset20 { offset } => {
                    self.vm_state.offset = offset as u32;
                }
                OffsetDelta4 { delta } => {
                    self.vm_state.offset += delta as u32;
                }
                OffsetDelta12 { delta } => {
                    self.vm_state.offset += delta as u32;
                }
                Emit4 { len } => {
                    self.emit_run(&mut r, len as usize);
                }
                Emit12 { len } => {
                    self.emit_run(&mut r, len as usize);
                }
                Emit20 { len } => {
                    self.emit_run(&mut r, len as usize);
                }
            }
        }
        Ok(r)
    }
}

//-----------------------------------------

// This starts from a fresh vm state each time, so don't use
// for cases where you get the stream in chunks (ie. slabs).
pub fn unpack(buf: &[u8]) -> Result<Vec<MapEntry>> {
    let mut unpacker = MappingUnpacker::default();
    let r = unpacker.unpack(buf)?;
    Ok(r)
}

//-----------------------------------------

#[cfg(test)]
mod stream_tests {
    use super::*;

    fn mk_run(slab: u64, b: u32, e: u32) -> Vec<MapEntry> {
        assert!((e - b) < u16::MAX as u32);
        let mut r = Vec::new();
        for i in b..e {
            r.push(MapEntry::Data { slab, offset: i });
        }
        r
    }

    #[test]
    fn pack_unpack_cycle() {
        use MapEntry::*;

        let tests: Vec<Vec<MapEntry>> = vec![
            vec![],
            vec![Zero { len: 1 }],
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
            mk_run(0, 0, 4),
            mk_run(1, 1, 4),
            mk_run(1, 1, 1024),
            mk_run(1, 1, 16000),
        ];

        for t in tests {
            // pack
            let mut buf: Vec<u8> = Vec::new();
            let mut c = std::io::Cursor::new(&mut buf);

            let mut builder = MappingBuilder::default();
            for e in &t {
                builder.next(&e, &mut c).expect("builder.next() failed");
            }
            builder.complete(&mut c).expect("builder.complete() failed");

            // unpack
            let actual = unpack(&buf[..]).expect("unpack failed");

            assert_eq!(*t, actual);
        }
    }
}

//-----------------------------------------
