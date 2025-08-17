use rosbag2parquet::rosbag2parquet;
use std::collections::HashSet;

fn main() {
    let mut topic_names = HashSet::new();
    topic_names.insert("/livox/imu".to_string());
    topic_names.insert("/livox/lidar".to_string());
    topic_names.insert("/camera/image_color/compressed".to_string());

    let test_path = "testdata/rosbag/r3live/hku_park_00/hku_park_00_0.mcap";
    rosbag2parquet(&test_path, Some(topic_names));
}
