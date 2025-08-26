# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a manual LiDAR-Camera calibration tool for ROS 2 that provides a graphical interface for performing extrinsic calibration between LiDAR sensors and cameras. The application is built with PySide6 and operates on recorded rosbag data (.mcap files) without requiring a live ROS 2 environment.

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

**Core Workflow:**
1. User loads a rosbag file (.mcap format)
2. Application displays available topics for selection
3. User selects image, point cloud, and camera info topics
4. Interactive calibration view allows manual 2D-3D correspondences
5. RANSAC-based PnP solver provides initial estimate
6. Scipy least-squares optimization refines the transformation

**Key Components:**

- **main.py**: Application entry point with PySide6 QApplication setup
- **main_window.py**: Primary GUI window handling rosbag loading and topic selection
- **calibration_widget.py**: Interactive widget for 2D/3D point selection and visualization
- **calibration.py**: Core mathematical calibration logic using OpenCV and Scipy
- **bag_handler.py**: Rosbag file processing and message extraction utilities
- **ros_utils.py**: Mock ROS 2 message dataclasses (PointCloud2, Image, CameraInfo) and conversion utilities

**Key Components (Continued):**

- **transformation_widget.py**: Node graph visualization for TF trees using NodeGraphQt
- **lidar_cleaner.py**: Point cloud processing based on RePLAy ECCV 2024 paper for removing occluded points
- **tf_transformations.py**: Transform utilities for coordinate frame conversions

**Application Flow:** The main application uses a QStackedWidget to manage multiple views:
1. Initial view for rosbag loading and topic selection (main_window.py)  
2. Interactive calibration view with 2D/3D visualization (calibration_widget.py)
3. Transform tree visualization and management (transformation_widget.py)

**Dependencies:** The project uses `rosbags` library for ROS bag processing, avoiding dependency on live ROS 2 installation. All ROS message types are mocked as dataclasses in `ros_utils.py`. NodeGraphQt provides the graph visualization for TF trees.

**Calibration Algorithm:** Two-stage approach using OpenCV's `solvePnPRansac` for robust initial pose estimation followed by Scipy's `least_squares` optimization for refinement. The objective function minimizes reprojection error between 3D LiDAR points and 2D image correspondences. Point cloud cleaning uses algorithms from the RePLAy paper to remove occluded points.

## Configuration

- **Linting**: Configured in `pyproject.toml` with ruff (line length: 100, select: E, F, W, I)
- **Entry Point**: Defined in `pyproject.toml` as `ros2_calib = "ros2_calib.main:main"`
- **Dependencies**: PySide6, rosbags, numpy, opencv-python-headless, scipy, ruff, NodeGraphQt, transforms3d, setuptools

## Development Notes

- The application is designed to work offline with recorded rosbag data
- GUI framework: PySide6 for cross-platform compatibility  
- No test suite is currently present in the codebase
- Code uses modern Python with type hints and dataclasses
- The LiDAR cleaning implementation is based on the RePLAy ECCV 2024 paper for removing projective artifacts
- Transform visualization uses NodeGraphQt for interactive graph-based TF tree management