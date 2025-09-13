<div align="center">

# rosbag2parquet

</div>

<div align="center">
    <img src="media/logo1.png" alt="siras" width="40%">
</div>

<div align="center">

A ROS-independent CLI to convert ROS 2 bag files (MCAP format) into Apache Parquet.

[![CI](https://img.shields.io/github/actions/workflow/status/jinbeizame007/rosbag2parquet/ci.yml?style=flat-square)](https://github.com/jinbeizame007/rosbag2parquet/actions/workflows/ci.yml)
[![LICENSE](https://img.shields.io/badge/LICENSE-MIT-blue?style=flat-square)](https://github.com/jinbeizame007/rosbag2parquet/blob/main/LICENSE)
[![STATUS](https://img.shields.io/badge/status-alpha-red?style=flat-square)](https://github.com/jinbeizame007/rosbag2parquet)

*This repository is still under development! Specifications are subject to change without notice.*

</div>

## Why Parquet?

MCAP files are optimized for recording, but not for analytics. [Apache Parquet](https://parquet.apache.org/) is an open-source, columnar, binary file format built for efficient analytics. It bridges the gap between ROS and the modern data analytics ecosystem.

- ⚡️ **Fast Queries**
    - Within the Parquet file for each ROS topic, data is stored column-by-column. This allows you to query specific fields of a topic without reading entire ROS messages. This columnar approach dramatically reduces I/O and accelerates data retrieval.
- 📦 **Efficient Storage**
    - Parquet applies highly efficient compression on a per-column basis. It achieves a much higher compression ratio than general-purpose compression on a rosbag. This leads to smaller files, reducing storage costs and network transfer times.
- 🧱 **Typed & Structured**
    - Unlike text-based formats that lose type information, Parquet preserves the complete structure of your ROS messages. Nested messages become nested structs, arrays maintain their dimensions, and numeric types retain their precision. This type safety eliminates the fragile parsing code that often breaks when message definitions evolve, replacing runtime errors with compile-time guarantees in strongly-typed languages.
- 🌍 **Universal Compatibility**
    - Parquet is a first-class citizen in the data science and engineering worlds. Converted ROS data becomes immediately accessible in universal and powerful tools like Apache Arrow, Pandas, and Polars.

## CLI

### Installation

Install [Rust](https://www.rust-lang.org/tools/install) first, then:

```bash
cargo install --path rosbag2parquet-cli 
```

### Usage

```
Convert ROS2 bag files to Parquet format

Usage: rosbag2parquet [OPTIONS] [INPUTS]...

Arguments:
  [INPUTS]...
          MCAP file paths or a directory containing MCAP files

Options:
      --topics <TOPICS>...
          A space-separated list of topics to include

      --exclude <EXCLUDE>...
          A space-separated list of topics to exclude

      --start-time <START_TIME>
          Start time [ns] of the messages to include

      --end-time <END_TIME>
          End time [ns] of the messages to include

      --output-dir <OUTPUT_DIR>
          Output directory for the converted Parquet files

      --compression <COMPRESSION>
          Compression algorithm to use

          Possible values:
          - uncompressed: No compression
          - snappy:       Snappy compression ( no level support)
          - gzip:         Gzip compression (levels 0-9)
          - lzo:          LZO compression (no level support)
          - brotli:       Brotli compression (levels 0-11)
          - lz4:          LZ4 compression (no level support)
          - zstd:         Zstandard compression (levels 1-22)
          - lz4-raw:      Raw LZ4 compression (no level support)

          [default: snappy]

      --compression-level <COMPRESSION_LEVEL>
          Compression level (only for gzip, brotli, zstd)

          Valid ranges:
          - gzip: 0-9 (default: 6)
          - brotli: 0-11 (default: 6)
          - zstd: 1-22 (default: 3)

      --threads <THREADS>
          Number of threads to use

          If not specified, the number of threads will be automatically determined.

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

If no output directory is specified, *rosbag2parquet* creates a `parquet` directory under the current directory.
Each parquet file is organized in subdirectories named after the source MCAP files, with the file structure based on topic names.

For example,

```
parquet
├── array_msgs_0
│   └── one_shot
│       ├── imu.parquet
│       └── joint_state.parquet
├── base_msgs_0
│   └── one_shot
│       ├── string.parquet
│       ├── twist.parquet
│       └── vector3.parquet
├── hku_park_00_0
│   ├── camera
│   │   └── image_color
│   │       └── compressed.parquet
│   └── livox
│       ├── imu.parquet
│       └── lidar.parquet
```

### Examples

```bash
# Download R3LIVE dataset (only hku_park_00_0.mcap)
bash scripts/download_r3live_dataset.bash

# Convert all topics
rosbag2parquet ./testdata/r3live/hku_park_00_0.mcap

# Convert mutiple rosbags in parallel
rosbag2parquet ./testdata/array_msgs/array_msgs_0.mcap ./testdata/base_msgs/base_msgs_0.mcap

# Convert all rosbags included in a directory in parallel
rosbag2parquet ./testdata

# Convert specific topics
rosbag2parquet ./testdata/r3live/hku_park_00_0.mcap --topics /livox/imu /livox/lidar

# Specify the time range [ns]
rosbag2parquet ./testdata/r3live/hku_park_00_0.mcap --start-time 1627720595994265000 --end-time 1627720595994542500 

# Specify the output directory
rosbag2parquet ./testdata/r3live/hku_park_00_0.mcap --output-dir ./parquet

# Specify the compression format (SNAPPY by default)
rosbag2parquet ./testdata/r3live/hku_park_00_0.mcap --compression zstd --compression-level 5
```

## Python

### Installation

Install [Rust](https://www.rust-lang.org/tools/install) and [uv](https://docs.astral.sh/uv/getting-started/installation/) first, then:

```bash
cd rosbag2parquet-pyo3

# Build wheel
uv tool install maturin
maturin build --release

# Install the built wheel into your project
# The wheel file can be found in the `rosbag2parquet/target/wheels/` directory
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
