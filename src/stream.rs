use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{combinator::fail, multi::*, number::complete::*, IResult};
use num_enum::TryFromPrimitive;
use rkyv::{Archive, Deserialize, Serialize};
use serde_json::json;
use serde_json::to_string_pretty;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crate::output::Output;
use crate::slab::*;
use crate::stack::*;

//-----------------------------------------

/// Sign extends a given number of bits.

fn sign_extend(x: i32, nbits: u32) -> i32 {
    let n = std::mem::size_of_val(&x) as u32 * 8 - nbits;
    x.wrapping_shl(n).wrapping_shr(n)
}

#[test]
fn test_sign_extend() {
    let tests = vec![
        (0b000, 0),
        (0b001, 1),
        (0b010, 2),
        (0b011, 3),
        (0b100, -4),
        (0b101, -3),
        (0b110, -2),
        (0b111, -1),
    ];

    for (input, output) in tests {
        assert_eq!(sign_extend(input, 3), output);
    }
}

const I4_MIN: i64 = -8;
const I4_MAX: i64 = 7;
const I12_MIN: i64 = -2048;
const I12_MAX: i64 = 2047;

fn delta_as_i4(lhs: u32, rhs: u32) -> Option<i8> {
    let delta: i64 = (rhs as i64) - (lhs as i64);
    if (I4_MIN..=I4_MAX).contains(&delta) {
        let n = ((delta as u64) & 0xf) as i8;
        Some(n)
    } else {
        None
    }
}

#[test]
fn test_delta_as_i4() {
    let tests = vec![
        (0, 0, Some(0)),
        (0, 3, Some(3)),
        (3, 0, Some(0b1101)),
        (0, 1234, None),
        (1234, 12, None),
    ];

    for (lhs, rhs, expected) in tests {
        let actual = delta_as_i4(lhs, rhs);
        assert_eq!(expected, actual);
    }
}

fn delta_as_i12(lhs: u32, rhs: u32) -> Option<i16> {
    let delta: i64 = (rhs as i64) - (lhs as i64);
    if (I12_MIN..=I12_MAX).contains(&delta) {
        let n = ((delta as u64) & 0xfff) as i16;
        Some(n)
    } else {
        None
    }
}

#[test]
fn test_delta_as_i12() {
    let tests = vec![
        (0, 0, Some(0)),
        (0, 230, Some(230)),
        (231, 0, Some(-231)),
        (0, 12340, None),
        (12340, 12, None),
    ];

    for (lhs, rhs, expected) in tests {
        let actual = delta_as_i12(lhs, rhs).map(|n| sign_extend(n as i32, 12));
        assert_eq!(expected, actual);
    }
}

//-----------------------------------------

// FIXME: when encoding it will be common to switch to a different
// stack entry, emit, switch back and increment a similar amount to
// what was just emitted.  How do we compile this efficiently?  Keep
// an array of stack entries in slab order, then do a binary search for
// the lower bound?

#[derive(Copy, Clone, Debug)]
pub enum MapInstruction {
    Rot { index: u8 },
    Dup { index: u8 },

    SetFill { byte: u8 },

    Fill8 { len: u8 },
    Fill16 { len: u16 },
    Fill32 { len: u32 },
    Fill64 { len: u64 },

    Unmapped8 { len: u8 },
    Unmapped16 { len: u16 },
    Unmapped32 { len: u32 },
    Unmapped64 { len: u64 },

    Slab16 { slab: u16 },
    Slab32 { slab: u32 },

    SlabDelta4 { delta: i8 },
    SlabDelta12 { delta: i16 },

    // Increments the slab and sets the offset to 0
    NextSlab,

    Offset4 { offset: u8 },
    Offset12 { offset: u16 },
    Offset20 { offset: u32 },

    OffsetDelta4 { delta: i8 },
    OffsetDelta12 { delta: i16 },

    Emit4 { len: u8 },
    Emit12 { len: u16 },
    Emit20 { len: u32 },

    Pos32 { pos: u32 },
    Pos64 { pos: u64 },

    Partial { begin: u32, end: u32 },
}

// 4 bit tags
#[derive(Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
enum MapTag {
    TagRot,
    TagDup,

    // These have the operand length packed in the low nibble
    TagFill,               // This also doubles up as SetFill
    TagUnmappedPosPartial, // Doubles up as Pos or Partial

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
            SetFill { byte } => {
                w.write_u8(pack_tag(TagFill, 5))?;
                w.write_u8(*byte)?;
            }
            Fill8 { len } => {
                w.write_u8(pack_tag(TagFill, 1))?;
                w.write_u8(*len)?;
            }
            Fill16 { len } => {
                w.write_u8(pack_tag(TagFill, 2))?;
                w.write_u16::<LittleEndian>(*len)?;
            }
            Fill32 { len } => {
                w.write_u8(pack_tag(TagFill, 3))?;
                w.write_u32::<LittleEndian>(*len)?;
            }
            Fill64 { len } => {
                w.write_u8(pack_tag(TagFill, 4))?;
                w.write_u64::<LittleEndian>(*len)?;
            }
            Unmapped8 { len } => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 1))?;
                w.write_u8(*len)?;
            }
            Unmapped16 { len } => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 2))?;
                w.write_u16::<LittleEndian>(*len)?;
            }
            Unmapped32 { len } => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 3))?;
                w.write_u32::<LittleEndian>(*len)?;
            }
            Unmapped64 { len } => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 4))?;
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
                let delta = *delta as u8 & 0xf;
                w.write_u8(pack_tag(TagSlabDelta4, delta))?;
            }
            SlabDelta12 { delta } => {
                assert!(*delta != 0);
                let delta = *delta as u16;
                w.write_u8(pack_tag(TagSlabDelta12, (delta & 0xf) as u8))?;
                w.write_u8((delta >> 4) as u8)?;
            }
            NextSlab => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 8))?;
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
                let delta = *delta as u8 & 0xf;
                w.write_u8(pack_tag(TagOffsetDelta4, delta))?;
            }
            OffsetDelta12 { delta } => {
                assert!(*delta != 0);
                let delta = *delta as u16;
                w.write_u8(pack_tag(TagOffsetDelta12, (delta & 0xf) as u8))?;
                w.write_u8((delta >> 4) as u8)?;
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
            Pos32 { pos } => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 5))?;
                w.write_u32::<LittleEndian>(*pos)?;
            }
            Pos64 { pos } => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 6))?;
                w.write_u64::<LittleEndian>(*pos)?;
            }
            Partial { begin, end } => {
                w.write_u8(pack_tag(TagUnmappedPosPartial, 7))?;
                w.write_u32::<LittleEndian>(*begin)?;
                w.write_u32::<LittleEndian>(*end)?;
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
            TagFill => match nibble {
                1 => {
                    let (input, len) = le_u8(input)?;
                    (input, Fill8 { len })
                }
                2 => {
                    let (input, len) = le_u16(input)?;
                    (input, Fill16 { len })
                }
                3 => {
                    let (input, len) = le_u32(input)?;
                    (input, Fill32 { len })
                }
                4 => {
                    let (input, len) = le_u64(input)?;
                    (input, Fill64 { len })
                }
                5 => {
                    // set fill
                    let (input, byte) = le_u8(input)?;
                    (input, SetFill { byte })
                }
                _ => {
                    // Bad length for fill tag
                    fail(input)?
                }
            },
            TagUnmappedPosPartial => match nibble {
                1 => {
                    let (input, len) = le_u8(input)?;
                    (input, Unmapped8 { len })
                }
                2 => {
                    let (input, len) = le_u16(input)?;
                    (input, Unmapped16 { len })
                }
                3 => {
                    let (input, len) = le_u32(input)?;
                    (input, Unmapped32 { len })
                }
                4 => {
                    let (input, len) = le_u64(input)?;
                    (input, Unmapped64 { len })
                }
                5 => {
                    let (input, pos) = le_u32(input)?;
                    (input, Pos32 { pos })
                }
                6 => {
                    let (input, pos) = le_u64(input)?;
                    (input, Pos64 { pos })
                }
                7 => {
                    let (input, begin) = le_u32(input)?;
                    let (input, end) = le_u32(input)?;
                    (input, Partial { begin, end })
                }
                8 => (input, NextSlab {}),
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
            TagSlabDelta4 => (
                input,
                SlabDelta4 {
                    delta: sign_extend(nibble as i32, 4) as i8,
                },
            ),
            TagSlabDelta12 => {
                let (input, w) = le_u8(input)?;
                let op = (((w as u16) << 4) | (nibble as u16)) as i32;
                let delta = sign_extend(op, 12) as i16;
                (input, SlabDelta12 { delta })
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
            TagOffsetDelta4 => {
                let delta = sign_extend(nibble as i32, 4) as i8;
                (input, OffsetDelta4 { delta })
            }
            TagOffsetDelta12 => {
                let (input, w) = le_u8(input)?;
                let op = (((w as u16) << 4) | (nibble as u16)) as i32;
                let delta = sign_extend(op, 12) as i16;
                (input, OffsetDelta12 { delta })
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

pub type IVec = Vec<MapInstruction>;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum MapEntry {
    Fill {
        byte: u8,
        len: u64,
    },
    Unmapped {
        len: u64,
    },
    Data {
        slab: u32,
        offset: u32,
        nr_entries: u32,
    },
    Partial {
        begin: u32,
        end: u32,

        // These are the data fields
        slab: u32,
        offset: u32,
        nr_entries: u32,
    },
    Ref {
        len: u64,
    },
}

pub trait MapEntryHandler {
    fn handle(&mut self, e: &MapEntry) -> Result<()>;
}

#[derive(Default, Copy, Clone, PartialEq, Eq)]
struct Register {
    slab: u32,
    offset: u32,
}

const STACK_SIZE: usize = 16;

#[derive(Default)]
pub struct VMState {
    fill: u8,
    stack: Stack<Register, generic_array::typenum::U16>,
    partial: Option<(u32, u32)>,
}

impl VMState {
    fn top(&mut self) -> &mut Register {
        self.stack.get_mut(0)
    }

    fn rot_stack(&mut self, index: usize) {
        self.stack.rot(index)
    }

    fn dup(&mut self, index: usize) {
        self.stack.dup(index)
    }

    fn next_slab(&mut self) {
        let r = self.top();
        r.slab += 1;
        r.offset = 0;
    }

    fn set_partial(&mut self, begin: u32, end: u32) -> Result<()> {
        if self.partial.is_some() {
            return Err(anyhow!("nested partials"));
        }

        self.partial = Some((begin, end));
        Ok(())
    }

    fn distance_cost(r1: &Register, r2: &Register) -> usize {
        ((r2.slab as i64 - r1.slab as i64).abs() * 1024
            + (r2.offset as i64 - r1.offset as i64).abs()) as usize
    }

    // Finds the register that would take the fewest bytes to encode.
    // This doesn't consider the top of the stack (index 0).
    // FIXME: so slow
    fn nearest_register(&mut self, slab: u32, offset: u32) -> usize {
        let target = Register { slab, offset };
        let mut index = 0;
        let mut min_cost = Self::distance_cost(self.stack.get(index), &target);

        for i in 1..STACK_SIZE {
            let cost = Self::distance_cost(self.stack.get(i), &target);
            if cost < min_cost {
                min_cost = cost;
                index = i;
            }
        }
        index
    }

    fn select_register(&mut self, slab: u32, offset: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        let top = self.top();
        if slab == top.slab + 1 && offset == 0 {
            instrs.push(NextSlab);
            self.next_slab();
            return Ok(());
        } else if top.slab == slab && delta_as_i12(top.offset, offset).is_some() {
            // We can encode this with a couple of bytes, so no point doing
            // any stack shuffling which would at best take 2 bytes.
            return Ok(());
        }

        let index = self.nearest_register(slab, offset);
        if index == 0 {
            let reg = self.stack.get(index);
            if reg.slab != slab && !(reg.slab == slab + 1 && reg.offset == 0) {
                instrs.push(Dup { index: index as u8 });
                self.dup(index);
            }
        } else {
            instrs.push(Rot { index: index as u8 });
            self.rot_stack(index);
        }

        Ok(())
    }

    pub fn encode_fill(&mut self, byte: u8, len: u64, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        if self.fill != byte {
            instrs.push(SetFill { byte });
            self.fill = byte;
        }

        if len < 0x10 {
            instrs.push(Fill8 { len: len as u8 });
        } else if len < 0x10000 {
            instrs.push(Fill16 { len: len as u16 });
        } else if len < 0x100000000 {
            instrs.push(Fill32 { len: len as u32 });
        } else {
            instrs.push(Fill64 { len });
        }

        Ok(())
    }

    pub fn encode_unmapped(&mut self, len: u64, instrs: &mut IVec) -> Result<()> {
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

    pub fn encode_pos(&mut self, pos: u64, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;
        if pos < u32::MAX as u64 {
            instrs.push(Pos32 { pos: pos as u32 });
        } else {
            instrs.push(Pos64 { pos });
        }

        Ok(())
    }

    pub fn encode_slab(&mut self, slab: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        let top = self.top();
        if slab != top.slab {
            if let Some(delta) = delta_as_i4(top.slab, slab) {
                instrs.push(SlabDelta4 { delta });
            } else if let Some(delta) = delta_as_i12(top.slab, slab) {
                instrs.push(SlabDelta12 { delta });
            } else if slab <= u16::MAX as u32 {
                instrs.push(Slab16 { slab: slab as u16 });
            } else {
                instrs.push(Slab32 { slab });
            }
            top.slab = slab;
        }
        Ok(())
    }

    pub fn encode_offset(&mut self, offset: u32, instrs: &mut IVec) -> Result<()> {
        use MapInstruction::*;

        let top = self.top();
        if offset != top.offset {
            if let Some(delta) = delta_as_i4(top.offset, offset) {
                instrs.push(OffsetDelta4 { delta });
            } else if let Some(delta) = delta_as_i12(top.offset, offset) {
                instrs.push(OffsetDelta12 { delta });
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
                return Err(anyhow!("offset too large ({})", offset));
            }
            top.offset = offset;
        }
        Ok(())
    }

    pub fn encode_emit(&mut self, len: u32, instrs: &mut IVec) -> Result<()> {
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

    pub fn encode_data(
        &mut self,
        slab: u32,
        offset: u32,
        nr_entries: u32,
        instrs: &mut IVec,
    ) -> Result<()> {
        self.select_register(slab, offset, instrs)?;
        self.encode_slab(slab, instrs)?;
        self.encode_offset(offset, instrs)?;
        self.encode_emit(nr_entries, instrs)?;
        Ok(())
    }

    pub fn encode_partial(&mut self, begin: u32, end: u32, instrs: &mut IVec) -> Result<()> {
        instrs.push(MapInstruction::Partial { begin, end });
        Ok(())
    }
}

//--------------------------------

#[derive(Default)]
pub struct MappingUnpacker {
    vm_state: VMState,
}

type EntryVec = Vec<MapEntry>;

// (byte_pos, entry index)
type PosVec = Vec<(u64, usize)>;

impl MappingUnpacker {
    fn emit_run(&mut self, r: &mut Vec<MapEntry>, len: usize) {
        let p = self.vm_state.partial.take();
        let top = self.vm_state.top();
        if let Some((begin, end)) = p {
            r.push(MapEntry::Partial {
                begin,
                end,
                slab: top.slab,
                offset: top.offset,
                nr_entries: len as u32,
            });
        } else {
            r.push(MapEntry::Data {
                slab: top.slab,
                offset: top.offset,
                nr_entries: len as u32,
            });
        }
        top.offset += len as u32;
    }

    fn emit_unmapped(&mut self, len: u64, r: &mut Vec<MapEntry>) {
        assert!(self.vm_state.partial.is_none());
        r.push(MapEntry::Unmapped { len });
    }

    fn emit_fill(&mut self, byte: u8, len: u64, r: &mut Vec<MapEntry>) {
        assert!(self.vm_state.partial.is_none());
        r.push(MapEntry::Fill { byte, len });
    }

    pub fn unpack(&mut self, buf: &[u8]) -> Result<(EntryVec, PosVec)> {
        use MapInstruction::*;

        let mut entries = Vec::new();
        let mut positions = Vec::new();
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

                SetFill { byte } => {
                    self.vm_state.fill = byte;
                }
                Fill8 { len } => {
                    self.emit_fill(self.vm_state.fill, len as u64, &mut entries);
                }
                Fill16 { len } => {
                    self.emit_fill(self.vm_state.fill, len as u64, &mut entries);
                }
                Fill32 { len } => {
                    self.emit_fill(self.vm_state.fill, len as u64, &mut entries);
                }
                Fill64 { len } => {
                    self.emit_fill(self.vm_state.fill, len, &mut entries);
                }

                Unmapped8 { len } => {
                    self.emit_unmapped(len as u64, &mut entries);
                }
                Unmapped16 { len } => {
                    self.emit_unmapped(len as u64, &mut entries);
                }
                Unmapped32 { len } => {
                    self.emit_unmapped(len as u64, &mut entries);
                }
                Unmapped64 { len } => {
                    self.emit_unmapped(len, &mut entries);
                }

                Slab16 { slab } => {
                    self.vm_state.top().slab = slab as u32;
                }
                Slab32 { slab } => {
                    self.vm_state.top().slab = slab;
                }
                SlabDelta4 { delta } => {
                    let top = self.vm_state.top();
                    top.slab = top.slab.wrapping_add(delta as u32);
                }
                SlabDelta12 { delta } => {
                    let top = self.vm_state.top();
                    top.slab = top.slab.wrapping_add(delta as u32);
                }
                NextSlab => {
                    let r = self.vm_state.top();
                    r.slab += 1;
                    r.offset = 0;
                }
                Offset4 { offset } => {
                    self.vm_state.top().offset = offset as u32;
                }
                Offset12 { offset } => {
                    self.vm_state.top().offset = offset as u32;
                }
                Offset20 { offset } => {
                    self.vm_state.top().offset = offset;
                }
                OffsetDelta4 { delta } => {
                    let top = self.vm_state.top();
                    top.offset = top.offset.wrapping_add(delta as u32);
                }
                OffsetDelta12 { delta } => {
                    let top = self.vm_state.top();
                    top.offset = top.offset.wrapping_add(delta as u32);
                }
                Emit4 { len } => {
                    self.emit_run(&mut entries, len as usize);
                }
                Emit12 { len } => {
                    self.emit_run(&mut entries, len as usize);
                }
                Emit20 { len } => {
                    self.emit_run(&mut entries, len as usize);
                }
                Pos32 { pos } => {
                    positions.push((pos as u64, entries.len()));
                }
                Pos64 { pos } => {
                    positions.push((pos, entries.len()));
                }
                Partial { begin, end } => {
                    self.vm_state.set_partial(begin, end)?;
                }
            }
        }
        Ok((entries, positions))
    }
}

//-----------------------------------------

// This starts from a fresh vm state each time, so don't use
// for cases where you get the stream in chunks (ie. slabs).
pub fn unpack(buf: &[u8]) -> Result<(EntryVec, PosVec)> {
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

pub struct StreamIter {
    file: SlabFile,
    slab: u32,
    entries: Vec<MapEntry>,
    index: usize,
}

impl StreamIter {
    pub fn new(mut file: SlabFile) -> Result<Self> {
        let entries = Self::read_slab(&mut file, 0)?;
        Ok(Self {
            file,
            slab: 0,
            entries,
            index: 0,
        })
    }

    fn read_slab(file: &mut SlabFile, slab: u32) -> Result<Vec<MapEntry>> {
        let buf = file.read(slab)?;
        let (entries, _positions) = unpack(&buf)?;
        Ok(entries)
    }

    fn next_slab(&mut self) -> Result<bool> {
        if self.slab >= self.file.get_nr_slabs() as u32 {
            return Ok(false);
        }

        self.slab += 1;
        let entries = Self::read_slab(&mut self.file, self.slab)?;
        self.entries = entries;
        self.index = 0;
        Ok(true)
    }
}

impl Iterator for StreamIter {
    type Item = Result<MapEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.entries.len() {
            match self.next_slab() {
                Err(e) => {
                    return Some(Err(e));
                }
                Ok(true) => {
                    // do nothing
                }
                Ok(false) => return None,
            }
        }

        let r = Some(Ok(self.entries[self.index]));
        self.index += 1;
        r
    }
}

//-----------------------------------------

#[derive(Debug, Default)]
struct Stats {
    rot: u64,
    dup: u64,
    set_fill: u64,
    fill8: u64,
    fill16: u64,
    fill32: u64,
    fill64: u64,
    unmapped8: u64,
    unmapped16: u64,
    unmapped32: u64,
    unmapped64: u64,
    slab16: u64,
    slab32: u64,
    slab_delta4: u64,
    slab_delta12: u64,
    next_slab: u64,
    offset4: u64,
    offset12: u64,
    offset20: u64,
    offset_delta4: u64,
    offset_delta12: u64,
    emit4: u64,
    emit12: u64,
    emit20: u64,
    pos32: u64,
    pos64: u64,
    partial: u64,
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
        let stream_file = SlabFileBuilder::open(stream_path).build()?;

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

            SetFill { byte } => {
                self.stats.set_fill += 1;
                self.vm_state.fill = *byte;
            }
            Fill8 { .. } => {
                self.stats.fill8 += 1;
            }
            Fill16 { .. } => {
                self.stats.fill16 += 1;
            }
            Fill32 { .. } => {
                self.stats.fill32 += 1;
            }
            Fill64 { .. } => {
                self.stats.fill64 += 1;
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
                self.vm_state.top().slab = *slab;
            }
            SlabDelta4 { delta } => {
                self.stats.slab_delta4 += 1;
                let top = self.vm_state.top();
                top.slab = top.slab.wrapping_add(*delta as u32);
            }
            SlabDelta12 { delta } => {
                self.stats.slab_delta12 += 1;
                let top = self.vm_state.top();
                top.slab = top.slab.wrapping_add(*delta as u32);
            }
            NextSlab => {
                self.stats.next_slab += 1;
                let r = self.vm_state.top();
                r.slab += 1;
                r.offset = 0;
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
                self.vm_state.top().offset = *offset;
            }
            OffsetDelta4 { delta } => {
                self.stats.offset_delta4 += 1;
                let top = self.vm_state.top();
                top.offset = top.offset.wrapping_add(*delta as u32);
            }
            OffsetDelta12 { delta } => {
                self.stats.offset_delta12 += 1;
                let top = self.vm_state.top();
                top.offset = top.offset.wrapping_add(*delta as u32);
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
                self.vm_state.top().offset += *len;
            }
            Pos32 { .. } => {
                self.stats.pos32 += 1;
            }
            Pos64 { .. } => {
                self.stats.pos64 += 1;
            }
            Partial { .. } => {
                self.stats.partial += 1;
            }
        }
    }

    fn pp_instr(&self, instr: &MapInstruction) -> String {
        use MapInstruction::*;

        match instr {
            Rot { index } => {
                format!("   rot {}", index)
            }
            Dup { index } => {
                format!("   dup {}", index)
            }

            SetFill { byte } => {
                format!("set-fill {}", byte)
            }
            Fill8 { len } => {
                format!("   fill {} ({})", len, self.vm_state.fill)
            }
            Fill16 { len } => {
                format!("   fill {} ({})", len, self.vm_state.fill)
            }
            Fill32 { len } => {
                format!("   fill {} ({})", len, self.vm_state.fill)
            }
            Fill64 { len } => {
                format!("   fill {} ({})", len, self.vm_state.fill)
            }

            Unmapped8 { len } => {
                format!("   unmap {}", len)
            }
            Unmapped16 { len } => {
                format!("   unmap {}", len)
            }
            Unmapped32 { len } => {
                format!("   unmap {}", len)
            }
            Unmapped64 { len } => {
                format!("   unmap {}", len)
            }

            Slab16 { slab } => {
                format!("   s.set {}", slab)
            }
            Slab32 { slab } => {
                format!("   s.set {}", slab)
            }
            SlabDelta4 { delta } => {
                format!("   s.add {}", delta)
            }
            SlabDelta12 { delta } => {
                format!("   s.add {}", delta)
            }
            NextSlab => "   next".to_string(),
            Offset4 { offset } => {
                format!("   o.set {}", offset)
            }
            Offset12 { offset } => {
                format!("   o.set {}", offset)
            }
            Offset20 { offset } => {
                format!("   o.set {}", offset)
            }
            OffsetDelta4 { delta } => {
                format!("   o.add {}", delta)
            }
            OffsetDelta12 { delta } => {
                format!("   o.add {}", delta)
            }
            Emit4 { len } => {
                format!("   emit {}", len)
            }
            Emit12 { len } => {
                format!("   emit {}", len)
            }
            Emit20 { len } => {
                format!("   emit {:<10}", len)
            }
            Pos32 { pos } => {
                format!("   pos {:<10}", pos)
            }
            Pos64 { pos } => {
                format!("   pos {:<10}", pos)
            }
            Partial { begin, end } => {
                let str = format!("{}..{}", begin, end);
                format!("   part {:<10}", str)
            }
        }
    }

    fn format_stack(&self) -> Result<String> {
        use std::fmt::Write;

        let mut buf = String::new();

        for i in 0..STACK_SIZE {
            let reg = self.vm_state.stack.get(i);
            write!(&mut buf, "{}:{} ", reg.slab, reg.offset)?;
        }

        Ok(buf)
    }

    fn effects_stack(instr: &MapInstruction) -> bool {
        use MapInstruction::*;

        !matches!(
            instr,
            Fill8 { .. }
                | Fill16 { .. }
                | Fill32 { .. }
                | Fill64 { .. }
                | SetFill { .. }
                | Pos32 { .. }
                | Pos64 { .. }
                | Unmapped8 { .. }
                | Unmapped16 { .. }
                | Unmapped32 { .. }
                | Unmapped64 { .. }
                | Partial { .. }
        )
    }

    pub fn dump(&mut self, output: Arc<Output>) -> Result<()> {
        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut json_stream = Vec::new();

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u32)?;
            let entries = unpack_instructions(&stream_data[..])?;

            for (i, e) in entries.iter().enumerate() {
                self.exec(e);

                if Self::effects_stack(e) {
                    let stack = self.format_stack()?;
                    if output.json {
                        json_stream.push(json!(
                            {"entry": i, "instruction": format!("{:?}",e), "stack": &stack}
                        ));
                    } else {
                        println!("{:0>10x}   {:20}{:20}", i, self.pp_instr(e), &stack,);
                    }
                } else if output.json {
                    json_stream.push(json!(
                        {"address": i, "instruction": format!("{:?}",e), "stack":""}
                    ));
                } else {
                    println!("{:0>10x}   {:20}", i, self.pp_instr(e));
                }
            }
        }

        let mut stats = vec![
            ("rot", self.stats.rot),
            ("dup", self.stats.dup),
            ("set-fill", self.stats.set_fill),
            ("fill8", self.stats.fill8),
            ("fill16", self.stats.fill16),
            ("fill32", self.stats.fill32),
            ("fill64", self.stats.fill64),
            ("unmapped8", self.stats.unmapped8),
            ("unmapped16", self.stats.unmapped16),
            ("unmapped32", self.stats.unmapped32),
            ("unmapped64", self.stats.unmapped64),
            ("slab16", self.stats.slab16),
            ("slab32", self.stats.slab32),
            ("slab_delta4", self.stats.slab_delta4),
            ("slab_delta12", self.stats.slab_delta12),
            ("next", self.stats.next_slab),
            ("offset4", self.stats.offset4),
            ("offset12", self.stats.offset12),
            ("offset20", self.stats.offset20),
            ("offset_delta4", self.stats.offset_delta4),
            ("offset_delta12", self.stats.offset_delta12),
            ("emit4", self.stats.emit4),
            ("emit12", self.stats.emit12),
            ("emit20", self.stats.emit20),
            ("pos32", self.stats.pos32),
            ("pos64", self.stats.pos64),
        ];

        stats.sort_by(|l, r| r.1.cmp(&l.1));

        if output.json {
            let hm_stats: HashMap<_, _> = stats.into_iter().collect();
            println!(
                "{}",
                to_string_pretty(&json!({
                    "instructions": json_stream, "stats": hm_stats
                }))
                .unwrap()
            );
        } else {
            println!("\n\nInstruction frequencies:\n");
            for (instr, count) in stats {
                println!("    {:>15} {:<10}", instr, count);
            }
        }

        Ok(())
    }
}

//-----------------------------------------
