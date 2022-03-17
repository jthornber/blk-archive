use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{combinator::fail, multi::*, number::complete::*, IResult};
use num_enum::TryFromPrimitive;
use std::convert::TryFrom;
use std::io::Write;
use std::path::PathBuf;

use crate::slab::*;

//-----------------------------------------

// FIXME: when encoding it will be common to switch to a different
// stack entry, emit, switch back and increment a similar amount to
// what was just emitted.  How do we compile this efficiently?  Keep
// an array of stack entries in slab order, then do a binary search for
// the lower bound?

#[derive(Copy, Clone, Debug)]
enum MapInstruction {
    Rot { index: u8 },
    Dup { index: u8 },

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
    TagDup,

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
            Dup { index } => {
                assert!(*index < STACK_SIZE as u8);
                w.write_u8(pack_tag(TagDup, *index))?;
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
            TagDup => (input, Dup { index: nibble }),
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

impl Register {
    fn leq(&self, rhs: &Self) -> bool {
        if self.slab < rhs.slab {
            true
        } else if self.slab == rhs.slab && self.offset <= rhs.offset {
            true
        } else {
            false
        }
    }
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

    // FIXME: slow
    fn dup(&mut self, index: usize) {
        let tmp = self.stack[index];
        for i in 0..(STACK_SIZE - 1) {
            self.stack[i] = self.stack[i + 1];
        }
        self.stack[STACK_SIZE - 1] = tmp;
    }

    // Finds the register with the nearest, but lower slab
    // FIXME: slow
    // FIXME: consider offset
    fn nearest_register(&mut self, slab: u32, offset: u32) -> usize {
        let target = Register { slab, offset };
        let mut index = 0; // default to the oldest stack entry
        let mut min = self.stack[index].clone();
        for (i, r) in self.stack.iter().enumerate().skip(1) {
            if r.leq(&target) && min.leq(r) {
                min = r.clone();
                index = i;
            }
        }

        index
    }

    fn select_register(&mut self, slab: u32, offset: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        let top = self.top();
        if top.slab == slab && top.offset == offset {
            // Nothing to be done
            return Ok(());
        }

        let index = self.nearest_register(slab, offset);
        let reg = &self.stack[index];
        // if reg.slab == slab {
        if index != STACK_SIZE - 1 {
            instrs.push(Rot { index: index as u8 });
            self.rot_stack(index);
        }
        /*
        } else {
            instrs.push(Dup { index: index as u8 });
            self.dup(index);
        }
        */

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

    fn encode_unmapped(&mut self, len: u64, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if len < 0x10 {
            instrs.push(Unmapped8 { len: len as u8 });
        } else if len < 0x10000 {
            instrs.push(Unmapped16 { len: len as u16 });
        } else if len < 0x100000000 {
            instrs.push(Unmapped32 { len: len as u32 });
        } else {
            instrs.push(Unmapped64 { len });
        }

        Ok(())
    }

    fn encode_slab(&mut self, slab: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

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
        self.select_register(run.slab, run.offset, instrs)?;
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
            Unmapped { len } => {
                // FIXME: factor out common code
                let mut run = None;
                std::mem::swap(&mut run, &mut self.run);
                if let Some(run) = run {
                    self.vm_state.encode_run(&run, &mut instrs)?;
                }
                self.vm_state.encode_unmapped(*len, &mut instrs)?;
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
                Dup { index } => {
                    self.vm_state.dup(index as usize);
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

fn unpack_instructions(buf: &[u8]) -> Result<Vec<MapInstruction>> {
    let (_, instrs) = many0(MapInstruction::unpack)(buf)
        .map_err(|_| anyhow!("unable to parse MappingInstruction"))?;
    Ok(instrs)
}

//-----------------------------------------

#[derive(Debug, Default)]
struct Stats {
    rot: u64,
    dup: u64,
    zero8: u64,
    zero16: u64,
    zero32: u64,
    zero64: u64,
    unmapped8: u64,
    unmapped16: u64,
    unmapped32: u64,
    unmapped64: u64,
    slab16: u64,
    slab32: u64,
    slab_delta4: u64,
    slab_delta12: u64,
    offset4: u64,
    offset12: u64,
    offset20: u64,
    offset_delta4: u64,
    offset_delta12: u64,
    emit4: u64,
    emit12: u64,
    emit20: u64,
}

pub struct Dumper {
    stream_file: SlabFile,
    vm_state: VMState,
    stats: Stats,
}

impl Dumper {
    // Assumes current directory is the root of the archive.
    pub fn new(stream: &str) -> Result<Self> {
        let stream_path: PathBuf = ["streams", stream, "stream"].iter().collect();
        let stream_file = SlabFile::open_for_read(stream_path)?;

        Ok(Self {
            stream_file,
            vm_state: VMState::default(),
            stats: Stats::default(),
        })
    }

    fn exec(&mut self, instr: &MapInstruction) {
        use MapInstruction::*;

        match instr {
            Rot { index } => {
                self.stats.rot += 1;
                self.vm_state.rot_stack(*index as usize);
            }
            Dup { index } => {
                self.stats.dup += 1;
                self.vm_state.dup(*index as usize);
            }

            Zero8 { .. } => {
                self.stats.zero8 += 1;
            }
            Zero16 { .. } => {
                self.stats.zero16 += 1;
            }
            Zero32 { .. } => {
                self.stats.zero32 += 1;
            }
            Zero64 { .. } => {
                self.stats.zero64 += 1;
            }

            Unmapped8 { .. } => {
                self.stats.unmapped8 += 1;
            }
            Unmapped16 { .. } => {
                self.stats.unmapped16 += 1;
            }
            Unmapped32 { .. } => {
                self.stats.unmapped32 += 1;
            }
            Unmapped64 { .. } => {
                self.stats.unmapped64 += 1;
            }

            Slab16 { slab } => {
                self.stats.slab16 += 1;
                self.vm_state.top().slab = *slab as u32;
            }
            Slab32 { slab } => {
                self.stats.slab32 += 1;
                self.vm_state.top().slab = *slab as u32;
            }
            SlabDelta4 { delta } => {
                self.stats.slab_delta4 += 1;
                self.vm_state.top().slab += *delta as u32;
            }
            SlabDelta12 { delta } => {
                self.stats.slab_delta12 += 1;
                self.vm_state.top().slab += *delta as u32;
            }
            Offset4 { offset } => {
                self.stats.offset4 += 1;
                self.vm_state.top().offset = *offset as u32;
            }
            Offset12 { offset } => {
                self.stats.offset12 += 1;
                self.vm_state.top().offset = *offset as u32;
            }
            Offset20 { offset } => {
                self.stats.offset20 += 1;
                self.vm_state.top().offset = *offset as u32;
            }
            OffsetDelta4 { delta } => {
                self.stats.offset_delta4 += 1;
                self.vm_state.top().offset += *delta as u32;
            }
            OffsetDelta12 { delta } => {
                self.stats.offset_delta12 += 1;
                self.vm_state.top().offset += *delta as u32;
            }
            Emit4 { len } => {
                self.stats.emit4 += 1;
                self.vm_state.top().offset += *len as u32;
            }
            Emit12 { len } => {
                self.stats.emit12 += 1;
                self.vm_state.top().offset += *len as u32;
            }
            Emit20 { len } => {
                self.stats.emit20 += 1;
                self.vm_state.top().offset += *len as u32;
            }
        }
    }

    fn pp_instr(&self, instr: &MapInstruction) -> String {
        use MapInstruction::*;

        match instr {
            Rot { index } => {
                format!("  rot {}", index)
            }
            Dup { index } => {
                format!("  dup {}", index)
            }

            Zero8 { len } => {
                format!(" zero {}", len)
            }
            Zero16 { len } => {
                format!(" zero {}", len)
            }
            Zero32 { len } => {
                format!(" zero {}", len)
            }
            Zero64 { len } => {
                format!(" zero {}", len)
            }

            Unmapped8 { len } => {
                format!("unmap {}", len)
            }
            Unmapped16 { len } => {
                format!("unmap {}", len)
            }
            Unmapped32 { len } => {
                format!("unmap {}", len)
            }
            Unmapped64 { len } => {
                format!("unmap {}", len)
            }

            Slab16 { slab } => {
                format!("s.set {}", slab)
            }
            Slab32 { slab } => {
                format!("s.set {}", slab)
            }
            SlabDelta4 { delta } => {
                format!("s.inc {}", delta)
            }
            SlabDelta12 { delta } => {
                format!("s.inc {}", delta)
            }
            Offset4 { offset } => {
                format!("o.set {}", offset)
            }
            Offset12 { offset } => {
                format!("o.set {}", offset)
            }
            Offset20 { offset } => {
                format!("o.set {}", offset)
            }
            OffsetDelta4 { delta } => {
                format!("o.inc {}", delta)
            }
            OffsetDelta12 { delta } => {
                format!("o.inc {}", delta)
            }
            Emit4 { len } => {
                format!(" emit {}", len)
            }
            Emit12 { len } => {
                format!(" emit {}", len)
            }
            Emit20 { len } => {
                format!(" emit {:<10}", len)
            }
        }
    }

    fn format_stack(&self) -> Result<String> {
        use std::fmt::Write;

        let mut buf = String::new();

        for i in 0..STACK_SIZE {
            let reg = self.vm_state.stack[STACK_SIZE - i - 1];
            write!(&mut buf, "{}:{} ", reg.slab, reg.offset)?;
        }

        Ok(buf)
    }

    fn effects_stack(instr: &MapInstruction) -> bool {
        use MapInstruction::*;

        match instr {
            Zero8 { .. }
            | Zero16 { .. }
            | Zero32 { .. }
            | Zero64 { .. }
            | Unmapped8 { .. }
            | Unmapped16 { .. }
            | Unmapped32 { .. }
            | Unmapped64 { .. } => false,
            _ => true,
        }
    }

    pub fn dump(&mut self) -> Result<()> {
        let nr_slabs = self.stream_file.get_nr_slabs();

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u32)?;
            let entries = unpack_instructions(&stream_data[..])?;

            for (i, e) in entries.iter().enumerate() {
                self.exec(e);

                if Self::effects_stack(e) {
                    let stack = self.format_stack()?;
                    println!("{:0>10x}   {:20}{:20}", i, self.pp_instr(e), &stack);
                } else {
                    println!("{:0>10x}   {:20}", i, self.pp_instr(e));
                }
            }
        }

        let mut stats = Vec::new();
        stats.push(("rot", self.stats.rot));
        stats.push(("dup", self.stats.dup));
        stats.push(("zero8", self.stats.zero8));
        stats.push(("zero16", self.stats.zero16));
        stats.push(("zero32", self.stats.zero32));
        stats.push(("zero64", self.stats.zero64));
        stats.push(("unmapped8", self.stats.unmapped8));
        stats.push(("unmapped16", self.stats.unmapped16));
        stats.push(("unmapped32", self.stats.unmapped32));
        stats.push(("unmapped64", self.stats.unmapped64));
        stats.push(("slab16", self.stats.slab16));
        stats.push(("slab32", self.stats.slab32));
        stats.push(("slab_delta4", self.stats.slab_delta4));
        stats.push(("slab_delta12", self.stats.slab_delta12));
        stats.push(("offset4", self.stats.offset4));
        stats.push(("offset12", self.stats.offset12));
        stats.push(("offset20", self.stats.offset20));
        stats.push(("offset_delta4", self.stats.offset_delta4));
        stats.push(("offset_delta12", self.stats.offset_delta12));
        stats.push(("emit4", self.stats.emit4));
        stats.push(("emit12", self.stats.emit12));
        stats.push(("emit20", self.stats.emit20));

        stats.sort_by(|l, r| r.1.cmp(&l.1));

        println!("\n\nInstruction frequencies:\n");
        for (instr, count) in stats {
            println!("    {:>15} {:<10}", instr, count);
        }

        Ok(())
    }
}

//-----------------------------------------

#[cfg(test)]
mod stream_tests {
    use super::*;

    fn mk_run(slab: u32, b: u32, e: u32) -> Vec<MapEntry> {
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
