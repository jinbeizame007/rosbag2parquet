# rosbag2parquet

```bash
cargo install --path rosbag2parquet-cli 
```

```bash
# Download R3LIVE dataset (only hku_park_00_0.mcap)
bash scripts/download_r3live_dataset.bash

# Convert all topics
rosbag2parquet convert ./testdata/rosbag/r3live/hku_park_00_0.mcap

# Convert specific topics
rosbag2parquet convert ./testdata/rosbag/r3live/hku_park_00_0.mcap --topic /livox/imu /livox/lidar
```

#### Credits

This project uses the R3LIVE dataset from [DapengFeng's MCAP Robotics Dataset Collection](https://huggingface.co/datasets/DapengFeng/MCAP), which is a converted version of the original R3LIVE dataset. The dataset is licensed under CC-BY-NC-4.0 for non-commercial use only.
