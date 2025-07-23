use std::fs;

use anyhow::{Context, Result};
use camino::Utf8Path;
use mcap::MessageStream;
use memmap2::Mmap;

mod create_test_data;

fn read_mcap<P: AsRef<Utf8Path>>(path: P) -> Result<Mmap> {
    let fd = fs::File::open(path.as_ref()).context("Couldn't open MCap file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCap file")
}

fn rosbag2parquet<P: AsRef<Utf8Path>>(path: P) -> Result<()> {
    let mmap = read_mcap(path)?;
    let message_stream = MessageStream::new(&mmap).context("Failed to create message stream")?;

    for (index, message_result) in message_stream.enumerate() {
        let message =
            message_result.with_context(|| format!("Failed to read message {}", index))?;

        if let Some(schema) = &message.channel.schema {
            let schema_name = schema.name.clone();
            let schema_data = schema.data.clone();

            let schema_text = std::str::from_utf8(&schema_data)?;
            println!("Schema: {} ({})", schema_name, schema_text);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_test_data::create_test_mcap_file;

    #[test]
    fn test_read_mcap() {
        // Create a test MCAP file
        let test_path = "testdata/test_read.mcap";
        create_test_mcap_file(test_path).unwrap();
        
        let mmap = read_mcap(test_path).unwrap();
        assert!(!mmap.is_empty());
        
        // Cleanup
        std::fs::remove_file(test_path).unwrap();
    }

    #[test]
    fn test_rosbag2parquet() {
        // Create a test MCAP file
        let test_path = "testdata/test_rosbag2parquet.mcap";
        create_test_mcap_file(test_path).unwrap();
        
        rosbag2parquet(test_path).unwrap();
        
        // Cleanup
        std::fs::remove_file(test_path).unwrap();
    }
}
