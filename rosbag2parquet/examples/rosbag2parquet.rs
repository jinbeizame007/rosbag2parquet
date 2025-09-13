use rosbag2parquet::{rosbag2parquet, Config};
use std::collections::HashSet;

fn main() {
    let mut topic_names = HashSet::new();
    topic_names.insert("/livox/imu".to_string());
    topic_names.insert("/livox/lidar".to_string());
    topic_names.insert("/camera/image_color/compressed".to_string());

    let test_path = "testdata/r3live/hku_park_00_0.mcap";
    let config = Config::default()
        .set_include_topic_names(Some(topic_names))
        .set_compression_from_str("SNAPPY");
    if let Err(e) = rosbag2parquet(&[test_path], config) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
