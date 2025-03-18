use super::builder::*;
use super::*;
use anyhow::ensure;
use tempfile::*;

// ensure the writer could handle unordered writes from the compressors
#[test]
fn write_unordered() -> Result<()> {
    let td = tempdir()?;
    let path = td.path().join("slab_file");
    let mut slab = SlabFileBuilder::create(path.clone()).build()?;

    let (_, tx0) = slab.reserve_slab();
    let (_, tx1) = slab.reserve_slab();
    let (_, tx2) = slab.reserve_slab();

    tx2.send(SlabData {
        index: 2,
        data: vec![2; 1536],
    })?;
    drop(tx2);

    tx0.send(SlabData {
        index: 0,
        data: vec![0; 512],
    })?;
    drop(tx0);

    tx1.send(SlabData {
        index: 1,
        data: vec![1; 1024],
    })?;
    drop(tx1);

    slab.close()?;
    drop(slab);

    let mut slab = SlabFileBuilder::open(path).build()?;

    for i in 0..3u8 {
        let data = slab.read(i as u32)?;
        ensure!(data.len() == (i as usize + 1) * 512);
        ensure!(data.iter().all(|&v| v == i));
    }

    Ok(())
}
