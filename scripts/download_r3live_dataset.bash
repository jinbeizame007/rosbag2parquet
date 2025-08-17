#!/bin/bash

set -e

mkdir -p ./testdata/rosbag/r3live/hku_park_00
curl -L https://huggingface.co/datasets/DapengFeng/MCAP/resolve/main/R3LIVE/hku_park_00/hku_park_00_0.mcap -o ./testdata/rosbag/r3live/hku_park_00/hku_park_00_0.mcap
