use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use io::Write;
use nom::{combinator::fail, multi::*, number::complete::*, IResult};
use num_enum::TryFromPrimitive;
use std::convert::TryFrom;
use std::io;

//-----------------------------------------

// FIXME: when encoding it will be common to switch to a different
// stack entry, emit, switch back and increment a similar amount to
// what was just emitted.  How do we compile this efficiently?  Keep
// an array of stack entries in slab order, then do a binary search for
// the lower bound?

#[derive(Copy, Clone, Debug)]
enum MapInstruction {
    Rot { index: u8 },

    Zero8 { len: u8 },
    Zero16 { len: u16 },
    Zero32 { len: u32 },
    Zero64 { len: u64 },

    Unmapped8 { len: u8 },
    Unmapped16 { len: u16 },
    Unmapped32 { len: u32 },
    Unmapped64 { len: u64 },

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
    TagRot,

    // These have the operand length packed in the low nibble
    TagZero,
    TagUnmapped,

    TagSlab16,
    TagSlab32,

    TagSlabDelta4,
    TagSlabDelta12,

    TagOffset4,
    TagOffset12,
    TagOffset20,

    TagOffsetDelta4,
    TagOffsetDelta12,

    TagEmit4,
    TagEmit12,
    TagEmit20,
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
            Rot { index } => {
                assert!(*index < STACK_SIZE as u8);
                w.write_u8(pack_tag(TagRot, *index))?;
            }
            Zero8 { len } => {
                w.write_u8(pack_tag(TagZero, 1))?;
                w.write_u8(*len)?;
            }
            Zero16 { len } => {
                w.write_u8(pack_tag(TagZero, 2))?;
                w.write_u16::<LittleEndian>(*len)?;
            }
            Zero32 { len } => {
                w.write_u8(pack_tag(TagZero, 4))?;
                w.write_u32::<LittleEndian>(*len)?;
            }
            Zero64 { len } => {
                w.write_u8(pack_tag(TagZero, 8))?;
                w.write_u64::<LittleEndian>(*len)?;
            }
            Unmapped8 { len } => {
                w.write_u8(pack_tag(TagUnmapped, 1))?;
                w.write_u8(*len)?;
            }
            Unmapped16 { len } => {
                w.write_u8(pack_tag(TagUnmapped, 2))?;
                w.write_u16::<LittleEndian>(*len)?;
            }
            Unmapped32 { len } => {
                w.write_u8(pack_tag(TagUnmapped, 4))?;
                w.write_u32::<LittleEndian>(*len)?;
            }
            Unmapped64 { len } => {
                w.write_u8(pack_tag(TagUnmapped, 8))?;
                w.write_u64::<LittleEndian>(*len)?;
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
            TagRot => (input, Rot { index: nibble }),
            TagZero => match nibble {
                1 => {
                    let (input, len) = le_u8(input)?;
                    (input, Zero8 { len })
                }
                2 => {
                    let (input, len) = le_u16(input)?;
                    (input, Zero16 { len })
                }
                4 => {
                    let (input, len) = le_u32(input)?;
                    (input, Zero32 { len })
                }
                8 => {
                    let (input, len) = le_u64(input)?;
                    (input, Zero64 { len })
                }
                _ => {
                    // Bad length for zero tag
                    fail(input)?
                }
            },
            TagUnmapped => match nibble {
                1 => {
                    let (input, len) = le_u8(input)?;
                    (input, Unmapped8 { len })
                }
                2 => {
                    let (input, len) = le_u16(input)?;
                    (input, Unmapped16 { len })
                }
                4 => {
                    let (input, len) = le_u32(input)?;
                    (input, Unmapped32 { len })
                }
                8 => {
                    let (input, len) = le_u64(input)?;
                    (input, Unmapped64 { len })
                }
                _ => {
                    // Bad length for unmapped tag
                    fail(input)?
                }
            },
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
    Unmapped { len: u64 },
    Data { slab: u32, offset: u32 },
}

// FIXME: support runs of zeroed regions
struct Run {
    slab: u32,
    offset: u32,
    len: u16,
}

#[derive(Default, Copy, Clone)]
struct Register {
    slab: u32,
    offset: u32,
}

const STACK_SIZE: usize = 16;

#[derive(Default)]
struct VMState {
    stack: [Register; STACK_SIZE],
}

impl VMState {
    fn top(&mut self) -> &mut Register {
        &mut self.stack[STACK_SIZE - 1]
    }

    // FIXME: slow
    fn rot_stack(&mut self, index: usize) {
        let tmp = self.stack[index];
        for i in index..(STACK_SIZE - 1) {
            self.stack[i] = self.stack[i + 1];
        }
        self.stack[STACK_SIZE - 1] = tmp;
    }

    // Finds the register with the nearest, but lower slab
    // FIXME: slow
    fn select_slab(&mut self, slab: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if self.top().slab == slab {
            // The common case
            return Ok(());
        }

        let mut min = u32::MAX;
        let mut index = 0; // default to the oldest stack entry
        for (i, r) in self.stack.iter().enumerate() {
            if r.slab <= slab {
                let delta = slab - r.slab;
                if delta < min {
                    min = delta;
                    index = i;
                }
            }
        }

        if index != STACK_SIZE - 1 {
            instrs.push(Rot { index: index as u8 });
            self.rot_stack(index);
        }

        Ok(())
    }

    fn encode_zero(&mut self, len: u64, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if len < 0x10 {
            instrs.push(Zero8 { len: len as u8 });
        } else if len < 0x10000 {
            instrs.push(Zero16 { len: len as u16 });
        } else if len < 0x100000000 {
            instrs.push(Zero32 { len: len as u32 });
        } else {
            instrs.push(Zero64 { len });
        }

        Ok(())
    }

    fn encode_slab(&mut self, slab: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        self.select_slab(slab, instrs)?;

        let top = self.top();
        if slab != top.slab {
            let delta: u32 = slab.wrapping_sub(top.slab);
            if delta < 0x10 {
                instrs.push(SlabDelta4 { delta: delta as u8 });
            } else if delta < 0x1000 {
                instrs.push(SlabDelta12 {
                    delta: delta as u16,
                });
            } else if slab <= u16::MAX as u32 {
                instrs.push(Slab16 { slab: slab as u16 });
            } else if slab <= u32::MAX as u32 {
                instrs.push(Slab32 { slab: slab as u32 });
            } else {
                return Err(anyhow!("slab index too large"));
            }
            top.slab = slab;
        }
        Ok(())
    }

    fn encode_offset(&mut self, offset: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        let top = self.top();
        if offset != top.offset {
            let delta: u32 = offset.wrapping_sub(top.offset);
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
            top.offset = offset;
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
        let top = self.top();
        top.offset += len;
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
            Unmapped { .. } => {
                todo!();
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
        let top = self.vm_state.top();
        for _ in 0..len {
            r.push(MapEntry::Data {
                slab: top.slab,
                offset: top.offset,
            });
            top.offset += 1;
        }
    }

    pub fn unpack(&mut self, buf: &[u8]) -> Result<Vec<MapEntry>> {
        use MapInstruction::*;

        let mut r = Vec::new();
        let (_, instrs) = many0(MapInstruction::unpack)(buf)
            .map_err(|_| anyhow!("unable to parse MappingInstruction"))?;

        for instr in instrs {
            match instr {
                Rot { index } => {
                    self.vm_state.rot_stack(index as usize);
                }

                Zero8 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }
                Zero16 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }
                Zero32 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }
                Zero64 { len } => {
                    r.push(MapEntry::Zero { len: len as u64 });
                }

                Unmapped8 { len } => {
                    r.push(MapEntry::Unmapped { len: len as u64 });
                }
                Unmapped16 { len } => {
                    r.push(MapEntry::Unmapped { len: len as u64 });
                }
                Unmapped32 { len } => {
                    r.push(MapEntry::Unmapped { len: len as u64 });
                }
                Unmapped64 { len } => {
                    r.push(MapEntry::Unmapped { len: len as u64 });
                }

                Slab16 { slab } => {
                    self.vm_state.top().slab = slab as u32;
                }
                Slab32 { slab } => {
                    self.vm_state.top().slab = slab as u32;
                }
                SlabDelta4 { delta } => {
                    self.vm_state.top().slab += delta as u32;
                }
                SlabDelta12 { delta } => {
                    self.vm_state.top().slab += delta as u32;
                }
                Offset4 { offset } => {
                    self.vm_state.top().offset = offset as u32;
                }
                Offset12 { offset } => {
                    self.vm_state.top().offset = offset as u32;
                }
                Offset20 { offset } => {
                    self.vm_state.top().offset = offset as u32;
                }
                OffsetDelta4 { delta } => {
                    self.vm_state.top().offset += delta as u32;
                }
                OffsetDelta12 { delta } => {
                    self.vm_state.top().offset += delta as u32;
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
