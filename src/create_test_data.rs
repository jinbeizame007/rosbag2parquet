use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use mcap::{Channel, Message, Schema, Writer};

pub fn create_test_mcap_file(path: &str) -> anyhow::Result<()> {
    let file = File::create(path)?;
    let mut writer = Writer::new(BufWriter::new(file))?;

    // Create schemas with IDs
    let string_schema = Arc::new(Schema {
        id: 1,
        name: "std_msgs/msg/String".to_string(),
        encoding: "ros2msg".to_string(),
        data: Cow::Borrowed(b"string data"),
    });

    let sensor_schema = Arc::new(Schema {
        id: 2,
        name: "sensor_msgs/msg/Temperature".to_string(),
        encoding: "ros2msg".to_string(),
        data: Cow::Borrowed(b"std_msgs/Header header\nfloat64 temperature\nfloat64 variance"),
    });

    // Create channels with IDs
    let string_channel = Arc::new(Channel {
        id: 1,
        topic: "/chatter".to_string(),
        schema: Some(string_schema),
        message_encoding: "cdr".to_string(),
        metadata: BTreeMap::new(),
    });

    let sensor_channel = Arc::new(Channel {
        id: 2,
        topic: "/temperature".to_string(),
        schema: Some(sensor_schema),
        message_encoding: "cdr".to_string(),
        metadata: BTreeMap::new(),
    });

    // Write some messages
    // For simplicity, using JSON-like data instead of actual CDR encoding
    for i in 0..5 {
        // String message
        let string_data = format!(r#"{{"data": "Hello ROS2 {}"}}"#, i);
        writer.write(&Message {
            channel: string_channel.clone(),
            sequence: i as u32,
            log_time: i * 1_000_000_000, // nanoseconds
            publish_time: i * 1_000_000_000,
            data: Cow::Owned(string_data.into_bytes()),
        })?;

        // Temperature message
        let temp_data = format!(
            r#"{{"header": {{"stamp": {{"sec": {}, "nanosec": 0}}, "frame_id": "base_link"}}, "temperature": {}, "variance": 0.1}}"#,
            i, 20.0 + i as f64 * 0.5
        );
        writer.write(&Message {
            channel: sensor_channel.clone(),
            sequence: i as u32,
            log_time: i * 1_000_000_000,
            publish_time: i * 1_000_000_000,
            data: Cow::Owned(temp_data.into_bytes()),
        })?;
    }

    writer.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mcap() {
        create_test_mcap_file("testdata/test_generated.mcap").unwrap();
        assert!(std::path::Path::new("testdata/test_generated.mcap").exists());
        std::fs::remove_file("testdata/test_generated.mcap").unwrap();
    }
}