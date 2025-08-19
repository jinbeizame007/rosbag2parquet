# rosbag2parquet

⚠️ **Work In Progress**: This package is currently in alpha development. APIs may change.

Convert ROS bag files (MCAP format) to Apache Parquet format for efficient data analysis.

## Installation

### Standard Installation (Once Published to PyPI)

```bash
# Recommended: Use virtual environment
python3 -m venv venv
source venv/bin/activate
pip install rosbag2parquet

# Alternative: Use pipx for isolated CLI usage
pipx install rosbag2parquet

# Alternative: Use uv (fastest)
uv add rosbag2parquet
```

### From Source (Current Development Version)

Since this package is not yet published to PyPI, install from source:

#### Method 1: Virtual Environment (Industry Standard)

```bash
uv tool install maturin
maturin build --release
```

```bash
# In your project (.whieel is in the `rosbag2parquet/target/wheel` directory)
uv add {path to .wheel file}
```

```bash
# Clone the repository
git clone <repository-url>
cd rosbag2parquet/rosbag2parquet-pyo3

# Using uv (recommended by Maturin)
uv venv
source .venv/bin/activate
maturin develop --uv

# Using pip with virtual environment
python3 -m venv venv
source venv/bin/activate
pip install maturin
maturin develop
```

#### Method 2: Using pipx (Isolated Tool Installation)

```bash
# Install pipx if not available
# macOS: brew install pipx
# Ubuntu: apt install pipx
# Other: pip install --user pipx

# Build wheel first
maturin build --release

# Install with pipx (creates isolated environment)
pipx install target/wheels/rosbag2parquet-0.1.0-*.whl
```

#### Method 3: Direct System Installation

**For most environments (non-externally managed Python):**
```bash
# Build wheel first
maturin build --release

# Standard installation
pip install --user target/wheels/rosbag2parquet-0.1.0-*.whl
```

**For macOS Homebrew Python (externally managed):**
```bash
# Build wheel first
maturin build --release

# Override PEP 668 protection (use with caution)
pip install --break-system-packages --user target/wheels/rosbag2parquet-0.1.0-*.whl
```

### Why the Different Installation Methods?

**PEP 668 (Externally Managed Environments)** was introduced to prevent conflicts between system package managers and Python package managers. Some Python distributions (like Homebrew on macOS 14+) implement this standard to protect the system Python environment.

**Recommended approach**: Use virtual environments or pipx to avoid conflicts and maintain clean dependencies.

## Usage

```python
from rosbag2parquet import convert

# Convert all topics
convert("testdata/rosbag/base_msgs/base_msgs_0.mcap")

# Convert to specific output directory
convert("testdata/rosbag/base_msgs/base_msgs_0.mcap", 
        output_dir="testdata/rosbag/base_msgs/parquet")

# Convert specific topics only
convert("testdata/rosbag/base_msgs/base_msgs_0.mcap", 
        topics=["/one_shot/twist", "/one_shot/vector3"])
```

## Development

```bash
# Install uv and maturin
pip3 install uv maturin

# Create virtual environment
uv venv
source .venv/bin/activate

# Build and install locally
maturin develop --uv

# Build release wheel
maturin build --release
```
