<div align="center">

# rosbag2parquet

</div>

<div align="center">
    <img src="media/logo1.png" alt="siras" width="40%">
</div>

<div align="center">

A ROS-independent CLI to convert ROS2 bag files (MCAP format) to Apache Parquet.

[![CI](https://img.shields.io/github/actions/workflow/status/jinbeizame007/rosbag2parquet/ci.yml?style=flat-square)](https://github.com/jinbeizame007/rosbag2parquet/actions/workflows/ci.yml)
[![LICENSE](https://img.shields.io/badge/LICENSE-MIT-blue?style=flat-square)](https://github.com/jinbeizame007/rosbag2parquet/blob/main/LICENSE)
[![STATUS](https://img.shields.io/badge/status-alpha-red?style=flat-square)](https://github.com/jinbeizame007/rosbag2parquet)

*This repository is still under development! Specifications are subject to change without notice.*

</div>

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
rosbag2parquet convert ./testdata/r3live/hku_park_00_0.mcap --topics /livox/imu /livox/lidar

# Specify the time range [ns]
rosbag2parquet convert ./testdata/r3live/hku_park_00_0.mcap --start-time 1627720595994265000 --end-time 1627720595994542500 

# Specify the output directory
rosbag2parquet convert ./testdata/r3live/hku_park_00_0.mcap --output-dir ./parquet

# Specify the compression format (SNAPPY by default)
rosbag2parquet convert ./testdata/r3live/hku_park_00_0.mcap --compression zstd --compression-level 5
```

If the output directory is not provided, *rosbag2parquet* will create a `parquet` directory in the same location as the mcap file.
Each parquet file will be saved with a path that corresponds to its topic name.

For example,

```
r3live
├── hku_park_00_0.mcap
└── parquet
    ├── camera
    │   └── image_color
    │       └── compressed.parquet
    └── livox
        ├── imu.parquet
        └── lidar.parquet
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
