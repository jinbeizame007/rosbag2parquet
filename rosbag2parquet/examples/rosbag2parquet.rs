use rosbag2parquet::rosbag2parquet;
use std::collections::HashSet;

fn main() {
    let mut topic_names = HashSet::new();
    // topic_names.insert("sensor_msgs/msg/JointState".to_string());
    // topic_names.insert("geometry_msgs/msg/QuaternionStamped".to_string());
    // topic_names.insert("sensor_msgs/msg/PointCloud2".to_string());
    // topic_names.insert("geometry_msgs/msg/PointStamped".to_string());
    // topic_names.insert("std_msgs/msg/String".to_string());
    // topic_names.insert("std_msgs/msg/Float64".to_string());
    // topic_names.insert("geometry_msgs/msg/TwistStamped".to_string());
    topic_names.insert("sensor_msgs/msg/Image".to_string());
    // topic_names.insert("visualization_msgs/msg/MarkerArray".to_string());
    // topic_names.insert("nav_msgs/msg/OccupancyGrid".to_string());
    // topic_names.insert("diagnostic_msgs/msg/DiagnosticArray".to_string());
    // topic_names.insert("diagnostic_msgs/msg/DiagnosticStatus".to_string());
    // topic_names.insert("tf2_msgs/msg/TFMessage".to_string());

    let test_path = "rosbags/large2/large2.mcap";
    rosbag2parquet(&test_path, Some(topic_names));
}
