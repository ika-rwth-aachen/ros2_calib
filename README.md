# ros2_calib

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-brightgreen.svg)](https://python.org)
[![ROS 2](https://img.shields.io/badge/ROS_2-mcap-blue.svg)](https://docs.ros.org/en/jazzy/p/rosbag2_storage_mcap/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![PySide6](https://img.shields.io/badge/PySide6-GUI-orange.svg)](https://pypi.org/project/PySide6/)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)

```
          ██████   ██████  ███████ ██████       ██████  █████   ██      ██ ██████  
          ██   ██ ██    ██ ██           ██      ██      ██   ██ ██      ██ ██   ██ 
          ██████  ██    ██ ███████  █████       ██      ███████ ██      ██ ██████  
          ██   ██ ██    ██      ██ ██           ██      ██   ██ ██      ██ ██   ██ 
          ██   ██  ██████  ███████ ███████       ██████ ██   ██ ███████ ██ ██████  

              ═══════════════════════════════════════════════════════════
                           Manual LiDAR-Camera Calibration Tool          
                          🎯 Precise • 🚀 Fast • 🔧 Interactive          
              ═══════════════════════════════════════════════════════════
```

**ros2_calib** is a manual LiDAR-Camera calibration tool for ROS 2 that provides an intuitive graphical interface for performing precise extrinsic calibration between LiDAR sensors and cameras. Built with PySide6, it operates on recorded rosbag data without requiring a ROS 2 environment. It supports reading `/tf_static` transforms from rosbags and allows users to quickly calibrate and export the resulting transformation directly into URDF format. Although it is a manual calibration tool, it is faster to use than a target-based calibration method and is more accurate than automatic methods.

## Screenshots

### Rosbag Loading and Topic Selection
![Topic View](assets/topic_view.png)

### TF Tree Visualization and Initial Transform Selection
![Transform View](assets/tree_view.png)

### Calibration Interface
![Screenshot](assets/calibration_view.png)

### Target Link Selection and URDF Export
![Node View](assets/export_view.png)

## Features

- **🎯 Interactive Calibration**: Point-and-click interface for 2D-3D correspondences
- **🔄 Real-time Visualization**: Live point cloud projection with adjustable parameters  
- **🧠 Smart Algorithms**: RANSAC-based PnP solver with Scipy least-squares refinement
- **🌳 TF Tree Integration**: Visual transform chain management and URDF export
- **🧹 Point Cloud Cleaning**: Advanced occlusion removal using the RePLAy algorithm
- **💾 Offline Processing**: Works with .mcap rosbag files - no live ROS 2 required
- **⌨️ Keyboard Shortcuts**: ESC to cancel, Backspace to delete, Enter to confirm
- **🎨 Easy to  UI**: Organized sections with responsive design

## Installation

### Prerequisites

- Tested with Python 3.12.3 and Ubuntu 24.04
- Compatible rosbag files in `.mcap` format

### Rosbag Requirements

Your rosbag file (.mcap format) should contain the following topics:

**Required:**
- **Camera topics**: `/camera/image_raw` or `/camera/image_rect`
    - `sensor_msgs/Image`
    - `sensor_msgs/CompressedImage`
- **Camera info**: `/camera/camera_info` (sensor_msgs/CameraInfo)
- **LiDAR topics**: `/lidar/points` or similar (sensor_msgs/PointCloud2)

**Optional but Recommended:**
- **Transform topics**: `/tf_static` (tf2_msgs/TFMessage) 
  - Contains static transformations between sensor frames
  - If not available, you'll need to manually specify initial transforms


### Install from Source

```bash
# Clone the repository
git clone https://github.com/ika-rwth-aachen/ros2_calib.git
cd ros2_calib

# Create a virtual environment
python -m venv .venv
source ./venv/bin/activate

# Install in development mode
python -m pip install .
```

### Install from PyPI

```bash
pip install ros2-calib
```

## Quick Start

1. **Launch the application**:
   ```bash
   ros2_calib
   ```

2. **Load your rosbag**: Click "Load Rosbag" and select your .mcap file

3. **Select topics**: Choose your image, point cloud, camera info, and TF topics

4. **Set initial transform**: Configure the transformation between LiDAR and camera frames

5. **Create correspondences**: Click corresponding points in the 2D image and 3D point cloud

6. **Calibrate**: Run the calibration algorithm to get precise extrinsic parameters

7. **Export results**: View transformation chains and export URDF-ready transforms

## Workflow Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Rosbag   │ -> │  Select Topics  │ -> │ Set Initial TF  │ -> │   Interactive   │
│   (.mcap file)  │    │  (img/pcd/info) │    │  (manual/auto)  │    │  Calibration    │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                                             │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│   Export URDF   │ <- │ Transform Chain │ <- │ View Results &  │ <----------┘
│   Transform     │    │  Visualization  │    │  TF Integration │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Core Architecture

- **main.py**: Application entry point with PySide6 QApplication setup
- **main_window.py**: Multi-view interface with stacked widget navigation
- **calibration_widget.py**: Interactive calibration view with 2D/3D visualization
- **calibration.py**: Core mathematical algorithms using OpenCV and Scipy
- **transformation_widget.py**: TF tree visualization using NodeGraphQt
- **bag_handler.py**: Rosbag processing and message extraction utilities
- **ros_utils.py**: Mock ROS 2 message types for offline operation
- **lidar_cleaner.py**: Point cloud cleaning based on RePLAy Algorithm (ECCV 2024)

## Algorithm Details

### Two-Stage Calibration Process

1. **Initial Estimation**: OpenCV's `solvePnPRansac` for robust pose estimation
2. **Refinement**: Scipy's `least_squares` optimization minimizing reprojection error
3. **Quality Assessment**: Automatic outlier detection and correspondence validation

### Point Cloud Processing

- **Occlusion Removal**: RePLAy algorithm removes projective artifacts
- **Intensity-based Coloring**: Configurable colormap visualization
- **Real-time Projection**: Live updates during manual adjustments

## Configuration

The tool automatically handles:
- **Message Format Detection**: Supports Image and CompressedImage types
- **Coordinate Frame Resolution**: TF tree parsing and path finding
- **Camera Model Integration**: Full camera info and distortion support

## Development

### Code Quality

```bash
# Run linter
ruff check

# Format code  
ruff format
```

### Project Structure

```
ros2_calib/
├── ros2_calib/
│   ├── __init__.py
│   ├── main.py              # Application entry point
│   ├── main_window.py       # Main GUI window with multi-view navigation
│   ├── calibration_widget.py # Interactive calibration interface
│   ├── calibration.py       # Core calibration algorithms
│   ├── transformation_widget.py # TF tree visualization
│   ├── bag_handler.py       # Rosbag processing utilities
│   ├── ros_utils.py         # Mock ROS message types
│   └── lidar_cleaner.py     # Point cloud cleaning algorithms
├── pyproject.toml          # Project configuration
└── README.md              # This file
```

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Troubleshooting

### Common Issues

- **"No topics found"**: Ensure your .mcap file contains the required sensor topics
- **"TF tree empty"**: Check that your rosbag includes transform messages
- **Calibration fails**: Verify you have at least 4 correspondence points

### Getting Help

- Open an [issue](https://github.com/ika-rwth-aachen/ros2_calib/issues) for bug reports

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Citation

If you use this tool in your research, please cite:

```bibtex
@software{ros2_calib,
  title={ros2\_calib: Manual LiDAR-Camera Calibration Tool},
  author={Till Beemelmanns},
  year={2024},
  url={https://github.com/ika-rwth-aachen/ros2_calib}
}
```

## Acknowledgments

### Point Cloud Cleaning Algorithm

We integrate the RePLAy algorithm for removing projective LiDAR artifacts:

```bibtex
@inproceedings{zhu2024replay,
  title={RePLAy: Remove Projective LiDAR Depthmap Artifacts via Exploiting Epipolar Geometry},
  author={Zhu, Shengjie and Ganesan, Girish Chandar and Kumar, Abhinav and Liu, Xiaoming},
  booktitle={ECCV},
  year={2024},
}
```

### Dependencies

- [PySide6](https://pypi.org/project/PySide6/) - Cross-platform GUI toolkit
- [OpenCV](https://opencv.org/) - Computer vision algorithms  
- [NumPy](https://numpy.org/) - Numerical computing
- [SciPy](https://scipy.org/) - Scientific computing
- [NodeGraphQt](https://github.com/jchanvfx/NodeGraphQt) - Node graph visualization
- [rosbags](https://github.com/ternaris/rosbags) - Pure Python rosbag processing

---

## Notice 

> [!IMPORTANT]  
> This repository is open-sourced and maintained by the [**Institute for Automotive Engineering (ika) at RWTH Aachen University**](https://www.ika.rwth-aachen.de/).  
> We cover a wide variety of research topics within our [*Vehicle Intelligence & Automated Driving*](https://www.ika.rwth-aachen.de/en/competences/fields-of-research/vehicle-intelligence-automated-driving.html) domain.  
> If you would like to learn more about how we can support your automated driving or robotics efforts, feel free to reach out to us!  
> :email: ***opensource@ika.rwth-aachen.de***




