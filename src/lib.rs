use std::fs;

use anyhow::{Context, Result};
use camino::Utf8Path;

use memmap2::Mmap;

fn read_mcap<P: AsRef<Utf8Path>>(path: P) -> Result<Mmap> {
    let fd = fs::File::open(path.as_ref()).context("Couldn't open MCap file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCap file")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_mcap() {
        let path = "testdata/demo.mcap";
        let mmap = read_mcap(path).unwrap();
        assert!(!mmap.is_empty());
    }
}
