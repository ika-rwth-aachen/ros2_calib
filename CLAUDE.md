# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-sensor calibration tool for ROS 2 that provides graphical interfaces for performing extrinsic calibration between different sensor types. The application supports both LiDAR-to-Camera and LiDAR-to-LiDAR calibration workflows. It is built with PySide6 and operates on recorded rosbag data (.mcap files) without requiring a live ROS 2 environment.

## Development Commands

### Installation and Setup
```bash
# Install the project in development mode
python -m pip install .
```

### Running the Application
```bash
# Run the calibration tool
ros2_calib
```

### Code Quality
```bash
# Run linter (configured in pyproject.toml)
ruff check

# Format code
ruff format
```

## Architecture Overview

The application follows a modular GUI-based architecture:

**Core Workflows:**

**LiDAR-to-Camera Calibration:**
1. User selects LiDAR-to-Camera calibration type
2. User loads a rosbag file (.mcap format)
3. Application displays available topics for selection
4. User selects image, point cloud, and camera info topics
5. Frame selection view allows choosing from multiple synchronized frames
6. Interactive calibration view allows manual 2D-3D correspondences
7. RANSAC-based PnP solver provides initial estimate
8. Scipy least-squares optimization refines the transformation

**LiDAR-to-LiDAR Calibration:**
1. User selects LiDAR-to-LiDAR calibration type
2. User loads a rosbag file (.mcap format)
3. Application displays available topics for selection
4. User selects source and target point cloud topics
5. Transform selection allows setting initial transformation estimate
6. Open3D-based interactive 3D calibration interface
7. Manual adjustment and ICP-based automatic registration
8. Export calibrated transformation matrix

**Key Components:**

- **main.py**: Application entry point with PySide6 QApplication setup
- **main_window.py**: Primary GUI window handling rosbag loading, topic selection, and calibration type selection
- **calibration_widget.py**: Interactive widget for 2D/3D point selection and visualization (LiDAR-to-Camera)
- **lidar2lidar_o3d_widget.py**: Open3D-based interactive 3D calibration interface (LiDAR-to-LiDAR)
- **frame_selection_widget.py**: Frame selection interface for choosing synchronized sensor data
- **calibration.py**: Core mathematical calibration logic using OpenCV and Scipy
- **bag_handler.py**: Rosbag file processing and message extraction utilities
- **ros_utils.py**: Mock ROS 2 message dataclasses (PointCloud2, Image, CameraInfo) and conversion utilities
- **tf_graph_widget.py**: Interactive TF tree visualization using NodeGraphQt
- **lidar_cleaner.py**: Point cloud processing based on RePLAy ECCV 2024 paper for removing occluded points
- **tf_transformations.py**: Transform utilities for coordinate frame conversions

**Application Flow:** The main application uses a QStackedWidget to manage multiple views:
1. Calibration type selection view (LiDAR-to-Camera vs LiDAR-to-LiDAR)
2. Rosbag loading and topic selection (main_window.py)
3. Transform selection and TF tree management
4. Frame selection for synchronized data (LiDAR-to-Camera only)
5. Interactive calibration view:
   - 2D/3D visualization (calibration_widget.py) for LiDAR-to-Camera
   - Open3D 3D interface (lidar2lidar_o3d_widget.py) for LiDAR-to-LiDAR
6. Calibration export view with transformation results

**Dependencies:** The project uses `rosbags` library for ROS bag processing, avoiding dependency on live ROS 2 installation. All ROS message types are mocked as dataclasses in `ros_utils.py`. NodeGraphQt provides the graph visualization for TF trees. Open3D is used for 3D visualization and point cloud processing in LiDAR-to-LiDAR calibration.

**Calibration Algorithms:**
- **LiDAR-to-Camera**: Two-stage approach using OpenCV's `solvePnPRansac` for robust initial pose estimation followed by Scipy's `least_squares` optimization for refinement. The objective function minimizes reprojection error between 3D LiDAR points and 2D image correspondences.
- **LiDAR-to-LiDAR**: Interactive manual adjustment with automatic ICP (Iterative Closest Point) registration using Open3D. Supports both point-to-point and point-to-plane ICP algorithms for fine-tuning transformations.

Point cloud cleaning uses algorithms from the RePLAy paper to remove occluded points.

## Configuration

- **Linting**: Configured in `pyproject.toml` with ruff (line length: 100, select: E, F, W, I)
- **Entry Point**: Defined in `pyproject.toml` as `ros2_calib = "ros2_calib.main:main"`
- **Dependencies**: PySide6, rosbags, numpy, opencv-python-headless, scipy, ruff, NodeGraphQt, transforms3d, open3d, setuptools

## Development Notes

- The application is designed to work offline with recorded rosbag data
- GUI framework: PySide6 for cross-platform compatibility  
- No test suite is currently present in the codebase
- Code uses modern Python with type hints and dataclasses
- The LiDAR cleaning implementation is based on the RePLAy ECCV 2024 paper for removing projective artifacts
- Transform visualization uses NodeGraphQt for interactive graph-based TF tree management