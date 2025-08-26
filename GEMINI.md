# Gemini Code Assistant Context

This file provides context for the Gemini code assistant to understand the project structure, purpose, and conventions.

## Project Overview

This project is a graphical tool for performing manual extrinsic calibration between a LiDAR sensor and a camera for ROS 2. It is a standalone Python application that uses PySide6 for its user interface.

The tool's workflow is as follows:
1.  The user loads a rosbag (`.mcap` file).
2.  The application reads the bag and displays the available topics.
3.  The user selects the appropriate topics for the image, point cloud, and camera information.
4.  The application then proceeds to a calibration view where the user can manually create 2D-3D correspondences between the image and the point cloud.
5.  The core calibration logic uses these correspondences to calculate the rigid transformation (rotation and translation) between the camera and LiDAR frames. It employs OpenCV's RANSAC-based PnP solver for a robust initial estimate and Scipy's least-squares optimization for refinement.

The project is structured to be independent of a live ROS 2 environment for its core functionality, instead relying on the `rosbags` library to process recorded data. It uses dataclasses to mock the necessary ROS 2 message structures.

## Key Files

-   `pyproject.toml`: Defines the project metadata, dependencies (`PySide6`, `rosbags`, `numpy`, `opencv-python-headless`, `scipy`), and the main entry point script.
-   `ros2_calib/main.py`: The main entry point for the application.
-   `ros2_calib/main_window.py`: Implements the main GUI window, handling rosbag loading and topic selection.
-   `ros2_calib/calibration_widget.py`: (Inferred) The widget that handles the interactive 2D/3D point selection and visualization for the calibration process.
-   `ros2_calib/calibration.py`: Contains the core mathematical logic for performing the calibration using OpenCV and Scipy.
-   `ros2_calib/bag_handler.py`: Provides functions for reading topic information and messages from rosbag files.
-   `ros2_calib/ros_utils.py`: Defines mock dataclasses for ROS 2 message types (`PointCloud2`, `Image`, `CameraInfo`) and includes utility functions for converting message data into NumPy arrays.

## Building and Running

### Installation

To install the project and its dependencies, run the following command from the project root directory:

```bash
python -m pip install .
```

### Running the Application

Once installed, the application can be run using the command defined in `pyproject.toml`:

```bash
ros2_calib
```

## Development Conventions

-   **GUI:** The user interface is built with PySide6.
-   **ROS 2 Data:** The application interacts with ROS 2 data via `.mcap` rosbag files. It does not require a running ROS 2 daemon.
-   **Calibration Logic:** The calibration algorithm is implemented in the `calibrate` function in `ros2_calib/calibration.py`. It uses a robust RANSAC approach followed by a least-squares refinement.
-   **Code Style:** The code is written in modern Python, using type hints and dataclasses. There is no specific linter configuration file present.
