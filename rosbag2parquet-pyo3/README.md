# rosbag2parquet

Convert ROS bag files (MCAP format) to Apache Parquet format.

## Installation

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) first, then:

```bash
# Build wheel
uv tool install maturin
maturin build --release

# Install in your project (wheel file is in `rosbag2parquet/target/wheel/` directory)
uv add path/to/rosbag2parquet-*.whl
```

## Usage

```python
import rosbag2parquet

# Convert all topics
rosbag2parquet.convert("path/to/file.mcap")

# Convert specific topics only
rosbag2parquet.convert("path/to/file.mcap", 
        output_dir="path/to/output",
        topics=["/imu", "/lidar"])
```
