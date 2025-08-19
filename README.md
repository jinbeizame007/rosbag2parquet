# rosbag2parquet

A ROS-independent CLI to convert ROS2 bag files (MCAP format) to Apache Parquet.

⚠️ **Work In Progress**: This package is currently in alpha development. APIs may change.

## CLI

### Installation

Install [Rust](https://www.rust-lang.org/tools/install) first, then:

```bash
cargo install --path rosbag2parquet-cli 
```

### Usage

```bash
# Download R3LIVE dataset (only hku_park_00_0.mcap)
bash scripts/download_r3live_dataset.bash

# Convert all topics
rosbag2parquet convert ./testdata/r3live/hku_park_00_0.mcap

# Convert specific topics
rosbag2parquet convert ./testdata/r3live/hku_park_00_0.mcap --topic /livox/imu /livox/lidar
```

## Python

### Installation

Install [Rust](https://www.rust-lang.org/tools/install) and [uv](https://docs.astral.sh/uv/getting-started/installation/) first, then:

```bash
cd rosbag2parquet-pyo3

# Build wheel
uv tool install maturin
maturin build --release

# Install in your project (wheel file is in `rosbag2parquet/target/wheel/` directory)
uv add path/to/rosbag2parquet-*.whl
```

### Usage

```python
import rosbag2parquet

# Convert all topics
rosbag2parquet.convert("path/to/file.mcap")

# Convert specific topics only
rosbag2parquet.convert("path/to/file.mcap", 
        output_dir="path/to/output",
        topics=["/imu", "/lidar"])
```

## Credits

This project uses the R3LIVE dataset from [DapengFeng's MCAP Robotics Dataset Collection](https://huggingface.co/datasets/DapengFeng/MCAP), which is a converted version of the original R3LIVE dataset. The dataset is licensed under CC-BY-NC-4.0 for non-commercial use only.
