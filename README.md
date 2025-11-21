# ros2_calib

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-brightgreen.svg)](https://python.org)
[![ROS 2](https://img.shields.io/badge/ROS_2-mcap-blue.svg)](https://docs.ros.org/en/jazzy/p/rosbag2_storage_mcap/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![PySide6](https://img.shields.io/badge/PySide6-GUI-orange.svg)](https://pypi.org/project/PySide6/)
[![Publish to PyPI](https://github.com/ika-rwth-aachen/ros2_calib/actions/workflows/publish.yml/badge.svg)](https://github.com/ika-rwth-aachen/ros2_calib/actions/workflows/publish.yml)
[![DOI](https://zenodo.org/badge/1040800706.svg)](https://doi.org/10.5281/zenodo.17119720)

```
          ██████   ██████  ███████ ██████       ██████  █████   ██      ██ ██████  
          ██   ██ ██    ██ ██           ██      ██      ██   ██ ██      ██ ██   ██ 
          ██████  ██    ██ ███████  █████       ██      ███████ ██      ██ ██████  
          ██   ██ ██    ██      ██ ██           ██      ██   ██ ██      ██ ██   ██ 
          ██   ██  ██████  ███████ ███████       ██████ ██   ██ ███████ ██ ██████  

              ═══════════════════════════════════════════════════════════
                          Multi-Sensor Calibration Tool for ROS 2
                          🎯 Precise • 🚀 Fast • 🔧 Interactive
                             >>>  pip install ros2-calib  <<<
              ═══════════════════════════════════════════════════════════
```

**ros2_calib** is a multi-sensor calibration tool for ROS 2 that provides intuitive graphical interfaces for performing precise extrinsic calibration between different sensor types. The tool supports both **LiDAR-to-Camera** and **LiDAR-to-LiDAR** calibration workflows. Built with PySide6, it operates on recorded rosbag data without requiring a live ROS 2 environment. It supports reading `/tf_static` transforms from rosbags and allows users to quickly calibrate and export the resulting transformation directly into URDF format. Although it is a manual calibration tool, we made the expierence that it is faster to use than a target-based calibration method and is more accurate than automatic methods.

## Screenshots

### Start Window with Calibration Type Selection
![Start Window](assets/start_view.png)

### Rosbag Loading and Topic Selection
![Topic View](assets/topic_view.png)

### TF Tree Visualization and Initial Transform Selection
![Transform View](assets/tree_view.png)

### LiDAR-to-Camera Calibration Interface
![LiDAR-to-Camera Calibration](assets/calibration_view.png)

### LiDAR-to-LiDAR Calibration Interface (Open3D)
![LiDAR-to-LiDAR Calibration](assets/lidar2lidar_view.png)

### Target Link Selection and URDF Export
![Export View](assets/export_view.png)

## Features

### LiDAR-to-Camera Calibration
- **🎯 Interactive Calibration**: Point-and-click interface for 2D-3D correspondences
- **🔄 Real-time Visualization**: Live point cloud projection with adjustable parameters
- **🧠 Smart Algorithms**: RANSAC-based PnP solver with Scipy least-squares refinement
- **🧹 Point Cloud Cleaning**: Advanced occlusion removal using the RePLAy algorithm
- **⌨️ Keyboard Shortcuts**: ESC to cancel, Backspace to delete, Enter to confirm

### LiDAR-to-LiDAR Calibration
- **🎮 3D Interactive Interface**: Open3D-based point cloud visualization
- **🔧 Manual Adjustment**: Precise transformation controls with step-size configuration
- **🤖 Automatic Registration**: Point-to-point and point-to-plane ICP algorithms
- **👁️ Dual Visualization**: Independent control over source and target point cloud visibility
- **📊 Calibration View**: Live transformation matrix updates and registration metrics

### General Features
- **🌳 TF Tree Integration**: Visual transform chain management and URDF export
- **💾 Offline Processing**: Works with .mcap rosbag files - no live ROS 2 required
- **🎨 Easy-to-Use UI**: Organized sections with responsive design
- **📱 Multi-Calibration Types**: Seamless switching between different calibration workflows

## Installation

### Prerequisites

- Tested with Python 3.12.3 and Ubuntu 24.04
- Compatible rosbag files in `.mcap` format

### Rosbag Requirements

Your rosbag file (`.mcap` format, tested with Jazzy and Humble) should contain the following topics:

**For LiDAR-to-Camera Calibration:**
- **Camera topics**: `/camera/image_raw` or `/camera/image_rect`
    - `sensor_msgs/Image`
    - `sensor_msgs/CompressedImage`
- **Camera info**: `/camera/camera_info` (sensor_msgs/CameraInfo)
- **LiDAR topics**: `/lidar/points` or similar (sensor_msgs/PointCloud2)

**For LiDAR-to-LiDAR Calibration:**
- **Source LiDAR topic**: `/lidar1/points` (sensor_msgs/PointCloud2)
- **Target LiDAR topic**: `/lidar2/points` (sensor_msgs/PointCloud2)

**Optional but Recommended:**
- **Transform topics**: `/tf_static` (tf2_msgs/TFMessage) 
  - Contains static transformations between sensor frames
  - If not available, you'll need to manually specify initial transforms

Furthermore, the metadata file (**metadata.yaml**) must be present in the same
directory as the `.mcap` file (usually automatically created by ROS 2 when recording).
Topics can be named differently; the tool allows you to select the correct topics during setup.

### Install from PyPI

```bash
pip install ros2-calib
```

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

## Quick Start

1. **Launch the application**:
   ```bash
   ros2_calib
   ```

2. **Select calibration type**: Choose between LiDAR-to-Camera or LiDAR-to-LiDAR calibration

3. **Load your rosbag**: Click "Load Rosbag" and select your .mcap file

4. **Select topics**: Choose your sensor topics based on calibration type

5. **Set initial transform**: Configure the transformation between sensor frames

6. **Perform calibration**:
   - **LiDAR-to-Camera**: Select frame, create 2D-3D correspondences, run calibration
   - **LiDAR-to-LiDAR**: Use interactive 3D interface with manual adjustment and/or ICP

7. **Export results**: View transformation chains and export URDF-ready transforms

## Workflow Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Select Calib    │ -> │   Load Rosbag   │ -> │  Select Topics  │ -> │ Set Initial TF  │
│ Type (L2C/L2L)  │    │   (.mcap file)  │    │   (sensors)     │    │  (manual/auto)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                                             │
                       ┌─────────────────┐    ┌─────────────────┐            │
                       │ Frame Selection │ -> │   LiDAR2Camera  │ <----------┘
                       │  (L2C only)     │    │  Calibration    │
                       └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│   Export URDF   │ <- │ Transform Chain │ <- │ View Results &  │ <----------┘
│   Transform     │    │  Visualization  │    │  TF Integration │            │
└─────────────────┘    └─────────────────┘    └─────────────────┘            │
                                                       ▲                     │
                       ┌─────────────────┐    ┌─────────────────┐            │
                       │   LiDAR2LiDAR   │ -> │                 │ <----------┘
                       │ Calibration O3D │    │                 │
                       └─────────────────┘    └─────────────────┘
```

## Core Architecture

- **main.py**: Application entry point with PySide6 QApplication setup
- **main_window.py**: Multi-view interface with stacked widget navigation and calibration type selection
- **calibration_widget.py**: Interactive LiDAR-to-Camera calibration view with 2D/3D visualization
- **lidar2lidar_o3d_widget.py**: Open3D-based LiDAR-to-LiDAR calibration interface
- **frame_selection_widget.py**: Frame selection interface for synchronized sensor data
- **calibration.py**: Core mathematical algorithms using OpenCV and Scipy
- **bag_handler.py**: Rosbag processing and message extraction utilities
- **ros_utils.py**: Mock ROS 2 message types for offline operation
- **tf_graph_widget.py**: Interactive TF tree visualization using NodeGraphQt
- **lidar_cleaner.py**: Point cloud cleaning based on RePLAy Algorithm (ECCV 2024)

## Algorithm Details

### LiDAR-to-Camera Calibration

**Two-Stage Calibration Process:**
1. **Initial Estimation**: OpenCV's `solvePnPRansac` for robust pose estimation
2. **Refinement**: Scipy's `least_squares` optimization minimizing reprojection error
3. **Quality Assessment**: Automatic outlier detection and correspondence validation

**Point Cloud Processing:**
- **Occlusion Removal**: RePLAy algorithm removes projective artifacts
- **Intensity-based Coloring**: Configurable colormap visualization
- **Real-time Projection**: Live updates during manual adjustments

### LiDAR-to-LiDAR Calibration

**Interactive 3D Calibration:**
- **Manual Adjustment**: Precise translation and rotation controls with configurable step sizes
- **Automatic Registration**: Point-to-point and point-to-plane ICP algorithms
- **Real-time Visualization**: Open3D-based 3D rendering with dual point cloud display
- **Quality Metrics**: Live fitness and RMSE feedback during ICP registration

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
  title={ros2\_calib: A Multi-Sensor Calibration Tool for ROS2},
  author={Till Beemelmanns},
  year={2025},
  url={https://github.com/ika-rwth-aachen/ros2_calib},
  publisher={Zenodo},
  doi={10.5281/zenodo.17119720},
  url={https://doi.org/10.5281/zenodo.17119720},
  swhid={swh:1:dir:0ac40351ccecf475772e49b7578fde53d224f6c4
         ;origin=https://doi.org/10.5281/zenodo.17119720;vi
         sit=swh:1:snp:98106a33aaa26f183d05dce909dc2da8c53b
         dab7;anchor=swh:1:rel:519b58ff48f20a36f6c36d6b3d06
         4bf1e7418432;path=ika-rwth-aachen-
         ros2\_calib-6fb095d
         },
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
- [Open3D](https://www.open3d.org/) - 3D visualization and point cloud processing
- [NodeGraphQt](https://github.com/jchanvfx/NodeGraphQt) - Node graph visualization
- [rosbags](https://github.com/ternaris/rosbags) - Pure Python rosbag processing

---

## Notice 

> [!IMPORTANT]  
> This repository is open-sourced and maintained by the [**Institute for Automotive Engineering (ika) at RWTH Aachen University**](https://www.ika.rwth-aachen.de/).  
> We cover a wide variety of research topics within our [*Vehicle Intelligence & Automated Driving*](https://www.ika.rwth-aachen.de/en/competences/fields-of-research/vehicle-intelligence-automated-driving.html) domain.  
> If you would like to learn more about how we can support your automated driving or robotics efforts, feel free to reach out to us!  
> :email: ***opensource@ika.rwth-aachen.de***




