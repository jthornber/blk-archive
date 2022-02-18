use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use io::Write;
use std::io;

//-----------------------------------------

enum MapInstruction {
    Zero4 { len: u8 },
    Zero12 { len: u16 },
    Zero20 { len: u32 },
    Zero36 { len: u64 },

    Slab16 { slab: u16 },
    Slab32 { slab: u32 },

    // FIXME: investigate how useful signed deltas would be
    SlabDelta4 { slab: u8 },
    SlabDelta12 { slab: u16 },

    Offset4 { offset: u8 },
    Offset12 { offset: u16 },
    Offset20 { offset: u32 },

    OffsetDelta4 { offset: u8 },
    OffsetDelta12 { offset: u16 },

    Emit4 { len: u8 },
    Emit12 { len: u16 },
    Emit20 { len: u32 },
}

// 4 bit tags
// FIXME: put explicit numeric values in once these settle down.
enum MapTag {
    TagZero4,
    TagZero12,
    TagZero20,
    TagZero36,

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

fn unpack_tag(_b: u8) -> (MapTag, u8) {
    todo!();
}

impl MapInstruction {
    pub fn pack<W: Write>(&self, w: &mut W) -> Result<()> {
        use MapInstruction::*;
        use MapTag::*;
        match self {
            Zero4 { len } => {
                assert!((len & 0xf0) == 0);
                w.write_u8(pack_tag(TagZero4, *len))?;
            }
            Zero12 { len } => {
                assert!((len & 0xf000) == 0);
                w.write_u8(pack_tag(TagZero12, (*len & 0xf) as u8))?;
                w.write_u8((len >> 4) as u8)?;
            }
            Zero20 { len } => {
                assert!((len & 0xf00000) == 0);
                w.write_u8(pack_tag(TagZero20, (*len & 0xf) as u8))?;
                w.write_u16::<LittleEndian>((len >> 4) as u16)?;
            }
            Zero36 { len } => {
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
            SlabDelta4 { slab } => {
                // FIXME: sign extend
                w.write_u8(pack_tag(TagSlabDelta4, *slab as u8))?;
            }
            SlabDelta12 { slab } => {
                w.write_u8(pack_tag(TagSlabDelta12, (*slab & 0xf) as u8))?;
                w.write_u8((*slab >> 4) as u8)?;
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
            OffsetDelta4 { offset } => {
                w.write_u8(pack_tag(TagOffsetDelta4, *offset))?;
            }
            OffsetDelta12 { offset } => {
                w.write_u8(pack_tag(TagOffsetDelta12, (*offset & 0xf) as u8))?;
                w.write_u8((*offset >> 4) as u8)?;
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
}

//-----------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
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

struct VMState {
    slab: u64,
    offset: u32,
}

impl Default for VMState {
    fn default() -> Self {
        Self { slab: 0, offset: 0 }
    }
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
                instrs.push(SlabDelta4 { slab: slab as u8 });
            } else if delta < 0x1000 {
                instrs.push(SlabDelta12 { slab: slab as u16 });
            } else {
                if slab < u16::MAX as u64 {
                    instrs.push(Slab16 { slab: slab as u16 });
                } else if slab <= u32::MAX as u64 {
                    instrs.push(Slab32 { slab: slab as u32 });
                } else {
                    return Err(anyhow!("slab index too large"));
                }
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
                instrs.push(OffsetDelta4 {
                    offset: offset as u8,
                });
            } else if delta < 0x1000 {
                instrs.push(OffsetDelta12 {
                    offset: offset as u16,
                });
            } else {
                if offset < 0x10 {
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

pub struct DevMapBuilder {
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

impl Default for DevMapBuilder {
    fn default() -> Self {
        Self {
            run: None,
            vm_state: VMState::default(),
        }
    }
}

impl DevMapBuilder {
    pub fn next<W: Write>(&mut self, e: &MapEntry, w: &mut W) -> Result<()> {
        use MapEntry::*;

        let mut instrs = Vec::new();
        match e {
            Zero { len } => {
                self.vm_state.encode_zero(*len, &mut instrs)?;
            }
            Data { slab, offset } => {
                let new_run = Run {
                    slab: *slab,
                    offset: *offset,
                    len: 0,
                };
                let run = self.run.get_or_insert(new_run);
                if (*slab == run.slab) && (*offset == (run.offset + run.len as u32)) {
                    run.len += 1;
                } else {
                    self.vm_state.encode_run(&run, &mut instrs)?;
                    let new_run = Run {
                        slab: *slab,
                        offset: *offset,
                        len: 0,
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
