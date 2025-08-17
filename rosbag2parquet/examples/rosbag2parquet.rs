use rosbag2parquet::rosbag2parquet;
use std::collections::HashSet;

fn main() {
    let test_path = "datasets/r3live/hku_park_00/hku_park_00_0.mcap";
    rosbag2parquet(&test_path, None); //Some(topic_names));
}
