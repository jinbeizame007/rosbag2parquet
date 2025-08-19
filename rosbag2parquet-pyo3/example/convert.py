import os
import rosbag2parquet

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
mcap_path = project_root + "/testdata/rosbag/base_msgs/base_msgs_0.mcap"

# Convert all topics
rosbag2parquet.convert(mcap_path)

# Convert specific topics only
rosbag2parquet.convert(mcap_path, 
        output_dir=os.path.dirname(mcap_path) + "/parquet",
        topics=["/one_shot/twist", "/one_shot/vector3"])
