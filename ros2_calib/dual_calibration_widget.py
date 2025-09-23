# MIT License
#
# Copyright (c) 2025 Institute for Automotive Engineering (ika), RWTH Aachen University
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from functools import partial

import cv2
import matplotlib.cm as cm
import numpy as np
from PySide6.QtCore import QEvent, QPointF, Qt, Signal
from PySide6.QtGui import QBrush, QColor, QImage, QPen, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QGraphicsEllipseItem,
    QGraphicsScene,
    QGraphicsView,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from scipy.spatial import KDTree
from scipy.spatial.transform import Rotation

from . import calibration
from .calibration_widget import PointCloudItem, ZoomableView
from .common import AppConstants, Colors, UIStyles
from .lidar_cleaner import LiDARCleaner


class DualCalibrationWidget(QWidget):
    calibration_completed = Signal(object)  # Signal to emit calibrated transforms

    def __init__(self, image_msg, master_pointcloud_msg, second_pointcloud_msg, camerainfo_msg,
                 ros_utils, initial_master_transform, initial_second_transform,
                 master_frame_id, second_frame_id):
        super().__init__()

        # Store messages and configurations
        self.image_msg = image_msg
        self.master_pointcloud_msg = master_pointcloud_msg
        self.second_pointcloud_msg = second_pointcloud_msg
        self.camerainfo_msg = camerainfo_msg
        self.ros_utils = ros_utils

        # Frame IDs
        self.master_frame_id = master_frame_id
        self.second_frame_id = second_frame_id
        self.camera_frame_id = getattr(camerainfo_msg.header, 'frame_id', 'camera_optical_frame')

        # Display names for UI
        self.master_display_name = f"{self.master_frame_id} (Master)"
        self.second_display_name = self.second_frame_id

        # Transformations
        self.initial_master_transform = initial_master_transform
        self.initial_second_transform = initial_second_transform
        self.master_extrinsics = np.copy(self.initial_master_transform)
        self.second_extrinsics = np.copy(self.initial_second_transform)

        # Correspondences
        self.master_cam_correspondences = {}  # Master LiDAR to camera
        self.second_cam_correspondences = {}  # Second LiDAR to camera
        self.lidar_to_lidar_correspondences = {}  # Second to master LiDAR

        # Visualization state
        self.master_visible = True
        self.second_visible = True

        # Selection state
        self.selection_mode = None
        self.selected_2d_point = None
        self.temp_2d_marker = []
        self.current_3d_selection = []
        self.highlighted_3d_items = []
        self.selected_3d_items_map = {}

        # Point cloud visualization items
        self.master_point_cloud_item = None
        self.second_point_cloud_item = None
        self.master_kdtree = None
        self.second_kdtree = None

        # Occlusion masks
        self.master_occlusion_mask = None
        self.second_occlusion_mask = None

        self.setup_ui()
        self.display_image()
        self.project_master_pointcloud()
        self.project_second_pointcloud()
        self.update_manual_inputs()
        self.display_camera_intrinsics()

    def setup_ui(self):
        main_layout = QHBoxLayout()
        self.setLayout(main_layout)

        # Left side: Image view
        left_layout = QVBoxLayout()
        self.scene = QGraphicsScene()
        self.view = ZoomableView(self.scene)
        self.view.viewport().installEventFilter(self)
        left_layout.addWidget(self.view)

        # Right side: Controls with tabs
        right_layout = QVBoxLayout()

        # Tab widget for organized controls
        self.tab_widget = QTabWidget()

        # Tab 1: Point Cloud Controls
        self.setup_pointcloud_tab()

        # Tab 2: Correspondence Management
        self.setup_correspondence_tab()

        # Tab 3: Manual Calibration
        self.setup_manual_calibration_tab()

        # Tab 4: Calibration Execution
        self.setup_calibration_tab()

        right_layout.addWidget(self.tab_widget)

        main_layout.addLayout(left_layout, 4)  # Image takes more space
        main_layout.addLayout(right_layout, 2)  # Controls take less space

    def setup_pointcloud_tab(self):
        pointcloud_tab = QWidget()
        layout = QVBoxLayout(pointcloud_tab)

        # Master point cloud controls
        master_group = QGroupBox(self.master_display_name)
        master_layout = QFormLayout(master_group)

        # Master visibility
        self.master_visible_checkbox = QCheckBox(f"Show {self.master_display_name}")
        self.master_visible_checkbox.setChecked(True)
        self.master_visible_checkbox.toggled.connect(self.toggle_master_visibility)
        master_layout.addRow(self.master_visible_checkbox)

        # Master point size
        self.master_point_size_spinbox = QSpinBox()
        self.master_point_size_spinbox.setRange(1, 10)
        self.master_point_size_spinbox.setValue(AppConstants.DEFAULT_POINT_SIZE)
        self.master_point_size_spinbox.valueChanged.connect(self.update_master_visualization)
        master_layout.addRow("Point Size:", self.master_point_size_spinbox)

        # Master colormap
        self.master_colormap_combo = QComboBox()
        self.master_colormap_combo.addItems([
            "autumn", "jet", "winter", "summer", "spring", "hot", "magma",
            "inferno", "Spectral", "RdYlGn", "viridis", "plasma"
        ])
        self.master_colormap_combo.setCurrentText("autumn")
        self.master_colormap_combo.currentTextChanged.connect(self.update_master_visualization)
        master_layout.addRow("Colormap:", self.master_colormap_combo)

        # Master colorization mode
        self.master_colorization_mode_combo = QComboBox()
        self.master_colorization_mode_combo.addItems(["Intensity", "Distance"])
        self.master_colorization_mode_combo.currentTextChanged.connect(self.update_master_visualization)
        master_layout.addRow("Color Mode:", self.master_colorization_mode_combo)

        # Master value ranges
        self.master_min_value_spinbox = QDoubleSpinBox()
        self.master_min_value_spinbox.setRange(-1e9, 1e9)
        self.master_min_value_spinbox.setDecimals(2)
        self.master_min_value_spinbox.valueChanged.connect(self.update_master_visualization)
        master_layout.addRow("Min Value:", self.master_min_value_spinbox)

        self.master_max_value_spinbox = QDoubleSpinBox()
        self.master_max_value_spinbox.setRange(-1e9, 1e9)
        self.master_max_value_spinbox.setDecimals(2)
        self.master_max_value_spinbox.valueChanged.connect(self.update_master_visualization)
        master_layout.addRow("Max Value:", self.master_max_value_spinbox)

        # Master occlusion cleaning
        self.master_clean_occlusion_button = QPushButton(f"Clean {self.master_display_name} Occluded Points")
        self.master_clean_occlusion_button.clicked.connect(self.clean_master_occlusion)
        master_layout.addRow(self.master_clean_occlusion_button)

        layout.addWidget(master_group)

        # Second point cloud controls
        second_group = QGroupBox(self.second_display_name)
        second_layout = QFormLayout(second_group)

        # Second visibility
        self.second_visible_checkbox = QCheckBox(f"Show {self.second_display_name}")
        self.second_visible_checkbox.setChecked(True)
        self.second_visible_checkbox.toggled.connect(self.toggle_second_visibility)
        second_layout.addRow(self.second_visible_checkbox)

        # Second point size
        self.second_point_size_spinbox = QSpinBox()
        self.second_point_size_spinbox.setRange(1, 10)
        self.second_point_size_spinbox.setValue(AppConstants.DEFAULT_POINT_SIZE)
        self.second_point_size_spinbox.valueChanged.connect(self.update_second_visualization)
        second_layout.addRow("Point Size:", self.second_point_size_spinbox)

        # Second colormap
        self.second_colormap_combo = QComboBox()
        self.second_colormap_combo.addItems([
            "autumn", "jet", "winter", "summer", "spring", "hot", "magma",
            "inferno", "Spectral", "RdYlGn", "viridis", "plasma"
        ])
        self.second_colormap_combo.setCurrentText("plasma")
        self.second_colormap_combo.currentTextChanged.connect(self.update_second_visualization)
        second_layout.addRow("Colormap:", self.second_colormap_combo)

        # Second colorization mode
        self.second_colorization_mode_combo = QComboBox()
        self.second_colorization_mode_combo.addItems(["Intensity", "Distance"])
        self.second_colorization_mode_combo.currentTextChanged.connect(self.update_second_visualization)
        second_layout.addRow("Color Mode:", self.second_colorization_mode_combo)

        # Second value ranges
        self.second_min_value_spinbox = QDoubleSpinBox()
        self.second_min_value_spinbox.setRange(-1e9, 1e9)
        self.second_min_value_spinbox.setDecimals(2)
        self.second_min_value_spinbox.valueChanged.connect(self.update_second_visualization)
        second_layout.addRow("Min Value:", self.second_min_value_spinbox)

        self.second_max_value_spinbox = QDoubleSpinBox()
        self.second_max_value_spinbox.setRange(-1e9, 1e9)
        self.second_max_value_spinbox.setDecimals(2)
        self.second_max_value_spinbox.valueChanged.connect(self.update_second_visualization)
        second_layout.addRow("Max Value:", self.second_max_value_spinbox)

        # Second occlusion cleaning
        self.second_clean_occlusion_button = QPushButton(f"Clean {self.second_display_name} Occluded Points")
        self.second_clean_occlusion_button.clicked.connect(self.clean_second_occlusion)
        second_layout.addRow(self.second_clean_occlusion_button)

        layout.addWidget(second_group)

        self.tab_widget.addTab(pointcloud_tab, "Point Clouds")

    def setup_correspondence_tab(self):
        correspondence_tab = QWidget()
        layout = QVBoxLayout(correspondence_tab)

        # Correspondence mode selection
        mode_group = QGroupBox("Correspondence Mode")
        mode_layout = QVBoxLayout(mode_group)

        self.correspondence_mode_combo = QComboBox()
        self.correspondence_mode_combo.addItems([
            f"{self.master_display_name} ↔ Camera",
            f"{self.second_display_name} ↔ Camera",
            f"{self.second_display_name} ↔ {self.master_display_name}"
        ])
        mode_layout.addWidget(self.correspondence_mode_combo)

        self.add_corr_button = QPushButton("Add Correspondence")
        self.add_corr_button.setCheckable(True)
        self.add_corr_button.toggled.connect(self.toggle_selection_mode)
        mode_layout.addWidget(self.add_corr_button)

        self.confirm_3d_button = QPushButton("Confirm 3D Selection")
        self.confirm_3d_button.setVisible(False)
        self.confirm_3d_button.clicked.connect(self.finalize_correspondence)
        mode_layout.addWidget(self.confirm_3d_button)

        layout.addWidget(mode_group)

        # Correspondence lists
        lists_group = QGroupBox("Correspondences")
        lists_layout = QVBoxLayout(lists_group)

        # Master-Camera correspondences
        master_cam_label = QLabel(f"{self.master_display_name} ↔ Camera:")
        lists_layout.addWidget(master_cam_label)
        self.master_cam_list_widget = QListWidget()
        self.master_cam_list_widget.setMaximumHeight(100)
        self.master_cam_list_widget.currentItemChanged.connect(self.highlight_master_cam_correspondence)
        lists_layout.addWidget(self.master_cam_list_widget)

        # Second-Camera correspondences
        second_cam_label = QLabel(f"{self.second_display_name} ↔ Camera:")
        lists_layout.addWidget(second_cam_label)
        self.second_cam_list_widget = QListWidget()
        self.second_cam_list_widget.setMaximumHeight(100)
        self.second_cam_list_widget.currentItemChanged.connect(self.highlight_second_cam_correspondence)
        lists_layout.addWidget(self.second_cam_list_widget)

        # LiDAR-LiDAR correspondences
        lidar_lidar_label = QLabel(f"{self.second_display_name} ↔ {self.master_display_name}:")
        lists_layout.addWidget(lidar_lidar_label)
        self.lidar_lidar_list_widget = QListWidget()
        self.lidar_lidar_list_widget.setMaximumHeight(100)
        self.lidar_lidar_list_widget.currentItemChanged.connect(self.highlight_lidar_correspondence)
        lists_layout.addWidget(self.lidar_lidar_list_widget)

        # Delete button
        self.delete_corr_button = QPushButton("Delete Selected")
        self.delete_corr_button.clicked.connect(self.delete_correspondence)
        lists_layout.addWidget(self.delete_corr_button)

        layout.addWidget(lists_group)

        self.tab_widget.addTab(correspondence_tab, "Correspondences")

    def setup_manual_calibration_tab(self):
        manual_tab = QWidget()
        layout = QVBoxLayout(manual_tab)

        # Master LiDAR manual calibration
        master_group = QGroupBox(f"{self.master_display_name} Manual Calibration")
        master_layout = self.create_manual_controls("master")
        master_group.setLayout(master_layout)
        layout.addWidget(master_group)

        # Second LiDAR manual calibration
        second_group = QGroupBox(f"{self.second_display_name} Manual Calibration")
        second_layout = self.create_manual_controls("second")
        second_group.setLayout(second_layout)
        layout.addWidget(second_group)

        # Reset button
        reset_button = QPushButton("Reset All Calibrations")
        reset_button.clicked.connect(self.reset_calibration_state)
        layout.addWidget(reset_button)

        self.tab_widget.addTab(manual_tab, "Manual Calibration")

    def create_manual_controls(self, prefix):
        layout = QGridLayout()

        # Step size controls
        t_step_spinbox = QDoubleSpinBox()
        t_step_spinbox.setRange(0.01, 5.0)
        t_step_spinbox.setValue(AppConstants.DEFAULT_TRANSLATION_STEP)
        t_step_spinbox.setSingleStep(0.1)
        t_step_spinbox.setSuffix(" cm")
        t_step_spinbox.setButtonSymbols(QDoubleSpinBox.NoButtons)
        layout.addWidget(QLabel("Pos Step:"), 0, 0)
        layout.addWidget(t_step_spinbox, 0, 1)

        r_step_spinbox = QDoubleSpinBox()
        r_step_spinbox.setRange(0.01, 10.0)
        r_step_spinbox.setValue(AppConstants.DEFAULT_ROTATION_STEP)
        r_step_spinbox.setSingleStep(0.05)
        r_step_spinbox.setSuffix(" °")
        r_step_spinbox.setButtonSymbols(QDoubleSpinBox.NoButtons)
        layout.addWidget(QLabel("Rot Step:"), 1, 0)
        layout.addWidget(r_step_spinbox, 1, 1)

        # DOF controls
        dof_widgets = {}
        for i, label in enumerate(["x", "y", "z", "roll", "pitch", "yaw"]):
            spinbox = QDoubleSpinBox()
            spinbox.setRange(-1e6, 1e6)
            spinbox.setDecimals(4)
            spinbox.setButtonSymbols(QDoubleSpinBox.NoButtons)
            spinbox.valueChanged.connect(partial(self.update_extrinsics_from_inputs, prefix))

            minus_button = QPushButton("-")
            plus_button = QPushButton("+")
            minus_button.clicked.connect(partial(self.adjust_dof, prefix, label, -1))
            plus_button.clicked.connect(partial(self.adjust_dof, prefix, label, 1))

            layout.addWidget(QLabel(label.capitalize() + ":"), i + 2, 0)
            layout.addWidget(spinbox, i + 2, 1)
            layout.addWidget(minus_button, i + 2, 2)
            layout.addWidget(plus_button, i + 2, 3)

            dof_widgets[label] = spinbox

        # Store references
        setattr(self, f"{prefix}_t_step_spinbox", t_step_spinbox)
        setattr(self, f"{prefix}_r_step_spinbox", r_step_spinbox)
        setattr(self, f"{prefix}_dof_widgets", dof_widgets)

        return layout

    def setup_calibration_tab(self):
        calibration_tab = QWidget()
        layout = QVBoxLayout(calibration_tab)

        # Calibration settings
        settings_group = QGroupBox("Calibration Settings")
        settings_layout = QFormLayout(settings_group)

        self.pnp_solver_combo = QComboBox()
        self.pnp_solver_combo.addItems(["Iterative", "SQPnP", "None"])
        settings_layout.addRow("RANSAC:", self.pnp_solver_combo)

        self.lsq_method_combo = QComboBox()
        self.lsq_method_combo.addItems(["lm", "trf", "dogbox"])
        settings_layout.addRow("LSQ Method:", self.lsq_method_combo)

        layout.addWidget(settings_group)

        # Calibration execution
        execution_group = QGroupBox("Calibration Execution")
        execution_layout = QVBoxLayout(execution_group)

        self.calibrate_button = QPushButton("Calibrate Dual LiDAR Setup")
        self.calibrate_button.clicked.connect(self.run_calibration)
        self.calibrate_button.setStyleSheet(UIStyles.HIGHLIGHT_BUTTON)
        execution_layout.addWidget(self.calibrate_button)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(False)
        execution_layout.addWidget(self.progress_bar)

        self.results_label = QLabel("Ready for calibration")
        self.results_label.setWordWrap(True)
        execution_layout.addWidget(self.results_label)

        layout.addWidget(execution_group)

        # Camera intrinsics display
        intrinsics_group = QGroupBox("Camera Intrinsics")
        intrinsics_layout = QVBoxLayout(intrinsics_group)

        self.intrinsics_display = QTextEdit()
        self.intrinsics_display.setMaximumHeight(200)
        self.intrinsics_display.setReadOnly(True)
        intrinsics_layout.addWidget(self.intrinsics_display)

        layout.addWidget(intrinsics_group)

        # Export
        self.export_button = QPushButton("Export Calibration Results")
        self.export_button.clicked.connect(self.view_calibration_results)
        layout.addWidget(self.export_button)

        self.tab_widget.addTab(calibration_tab, "Calibration")

    def display_image(self):
        """Display the camera image."""
        if (
            hasattr(self.image_msg, "_type")
            and self.image_msg._type == "sensor_msgs/msg/CompressedImage"
        ):
            np_arr = np.frombuffer(self.image_msg.data, np.uint8)
            self.cv_image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        else:
            self.cv_image = self.ros_utils.image_to_numpy(self.image_msg)
        if "bgr" in self.image_msg.encoding:
            self.cv_image = cv2.cvtColor(self.cv_image, cv2.COLOR_BGR2RGB)
        h, w, c = self.cv_image.shape
        q_image = QImage(self.cv_image.data, w, h, 3 * w, QImage.Format_RGB888)
        self.scene.addPixmap(QPixmap.fromImage(q_image))

    def display_camera_intrinsics(self):
        """Display camera intrinsic parameters."""
        K = np.array(self.camerainfo_msg.k).reshape(3, 3)

        display_text = "Camera Matrix K:\n"
        for i in range(3):
            row_text = "  ".join(f"{K[i, j]:8.2f}" for j in range(3))
            display_text += f"[{row_text}]\n"

        display_text += f"\nImage Size: {self.camerainfo_msg.width} x {self.camerainfo_msg.height}\n"
        display_text += f"Distortion: {self.camerainfo_msg.distortion_model}"

        fx, fy = K[0, 0], K[1, 1]
        cx, cy = K[0, 2], K[1, 2]
        display_text += f"\n\nFocal Length: fx={fx:.1f}, fy={fy:.1f}"
        display_text += f"\nPrincipal Point: cx={cx:.1f}, cy={cy:.1f}"

        self.intrinsics_display.setPlainText(display_text)

    def project_master_pointcloud(self, re_read_cloud=True):
        """Project master point cloud to image."""
        if not self.master_visible:
            if self.master_point_cloud_item and self.master_point_cloud_item.scene():
                self.scene.removeItem(self.master_point_cloud_item)
            return

        if re_read_cloud:
            # Extract point cloud data
            cloud_arr = self.ros_utils.pointcloud2_to_structured_array(self.master_pointcloud_msg)
            valid_mask = (
                np.isfinite(cloud_arr["x"])
                & np.isfinite(cloud_arr["y"])
                & np.isfinite(cloud_arr["z"])
            )
            cloud_arr = cloud_arr[valid_mask]
            self.master_points_xyz = np.vstack([cloud_arr["x"], cloud_arr["y"], cloud_arr["z"]]).T
            self.master_intensities = cloud_arr["intensity"]

            # Update min/max values
            self.update_master_min_max_values()

        if not hasattr(self, 'master_points_xyz') or self.master_points_xyz.shape[0] == 0:
            return

        # Remove existing visualization
        if self.master_point_cloud_item and self.master_point_cloud_item.scene():
            self.scene.removeItem(self.master_point_cloud_item)

        # Project points
        K = np.array(self.camerainfo_msg.k).reshape(3, 3)
        rvec, _ = cv2.Rodrigues(self.master_extrinsics[:3, :3])
        tvec = self.master_extrinsics[:3, 3]
        points_proj_cv, _ = cv2.projectPoints(self.master_points_xyz, rvec, tvec, K, None)
        points_proj_cv = points_proj_cv.reshape(-1, 2)

        # Filter visible points
        points_cam = (self.master_extrinsics[:3, :3] @ self.master_points_xyz.T).T + tvec
        z_cam = points_cam[:, 2]

        mask = (
            (z_cam > 0)
            & (points_proj_cv[:, 0] >= 0)
            & (points_proj_cv[:, 0] < self.camerainfo_msg.width)
            & (points_proj_cv[:, 1] >= 0)
            & (points_proj_cv[:, 1] < self.camerainfo_msg.height)
        )

        if self.master_occlusion_mask is not None and len(self.master_occlusion_mask) == len(mask):
            mask = np.logical_and(mask, self.master_occlusion_mask)

        self.master_valid_indices = np.where(mask)[0]
        self.master_points_proj_valid = points_proj_cv[self.master_valid_indices]
        self.master_intensities_valid = self.master_intensities[self.master_valid_indices]

        if self.master_points_proj_valid.shape[0] == 0:
            self.master_kdtree = None
            return

        self.master_kdtree = KDTree(self.master_points_proj_valid)

        # Create colors
        colors = self.create_colors(
            self.master_points_proj_valid,
            self.master_intensities_valid,
            points_cam[self.master_valid_indices],
            self.master_colormap_combo.currentText(),
            self.master_colorization_mode_combo.currentText(),
            self.master_min_value_spinbox.value(),
            self.master_max_value_spinbox.value()
        )
        colors[:, 3] = 0.8  # Alpha for master

        # Create point cloud item
        self.master_point_cloud_item = PointCloudItem(
            self.master_points_proj_valid, colors, self.master_point_size_spinbox.value()
        )
        self.scene.addItem(self.master_point_cloud_item)

    def project_second_pointcloud(self, re_read_cloud=True):
        """Project second point cloud to image."""
        if not self.second_visible:
            if self.second_point_cloud_item and self.second_point_cloud_item.scene():
                self.scene.removeItem(self.second_point_cloud_item)
            return

        if re_read_cloud:
            # Extract point cloud data
            cloud_arr = self.ros_utils.pointcloud2_to_structured_array(self.second_pointcloud_msg)
            valid_mask = (
                np.isfinite(cloud_arr["x"])
                & np.isfinite(cloud_arr["y"])
                & np.isfinite(cloud_arr["z"])
            )
            cloud_arr = cloud_arr[valid_mask]
            self.second_points_xyz = np.vstack([cloud_arr["x"], cloud_arr["y"], cloud_arr["z"]]).T
            self.second_intensities = cloud_arr["intensity"]

            # Update min/max values
            self.update_second_min_max_values()

        if not hasattr(self, 'second_points_xyz') or self.second_points_xyz.shape[0] == 0:
            return

        # Remove existing visualization
        if self.second_point_cloud_item and self.second_point_cloud_item.scene():
            self.scene.removeItem(self.second_point_cloud_item)

        # Project points using second LiDAR's transformation to camera
        K = np.array(self.camerainfo_msg.k).reshape(3, 3)
        rvec, _ = cv2.Rodrigues(self.second_extrinsics[:3, :3])
        tvec = self.second_extrinsics[:3, 3]
        points_proj_cv, _ = cv2.projectPoints(self.second_points_xyz, rvec, tvec, K, None)
        points_proj_cv = points_proj_cv.reshape(-1, 2)

        # Filter visible points
        points_cam = (self.second_extrinsics[:3, :3] @ self.second_points_xyz.T).T + tvec
        z_cam = points_cam[:, 2]

        mask = (
            (z_cam > 0)
            & (points_proj_cv[:, 0] >= 0)
            & (points_proj_cv[:, 0] < self.camerainfo_msg.width)
            & (points_proj_cv[:, 1] >= 0)
            & (points_proj_cv[:, 1] < self.camerainfo_msg.height)
        )

        if self.second_occlusion_mask is not None and len(self.second_occlusion_mask) == len(mask):
            mask = np.logical_and(mask, self.second_occlusion_mask)

        self.second_valid_indices = np.where(mask)[0]
        self.second_points_proj_valid = points_proj_cv[self.second_valid_indices]
        self.second_intensities_valid = self.second_intensities[self.second_valid_indices]

        if self.second_points_proj_valid.shape[0] == 0:
            self.second_kdtree = None
            return

        self.second_kdtree = KDTree(self.second_points_proj_valid)

        # Create colors
        colors = self.create_colors(
            self.second_points_proj_valid,
            self.second_intensities_valid,
            points_cam[self.second_valid_indices],
            self.second_colormap_combo.currentText(),
            self.second_colorization_mode_combo.currentText(),
            self.second_min_value_spinbox.value(),
            self.second_max_value_spinbox.value()
        )
        colors[:, 3] = 0.7  # Slightly more transparent for second

        # Create point cloud item
        self.second_point_cloud_item = PointCloudItem(
            self.second_points_proj_valid, colors, self.second_point_size_spinbox.value()
        )
        self.scene.addItem(self.second_point_cloud_item)

    def create_colors(self, points_proj, intensities, points_cam, colormap_name,
                     colorization_mode, min_val, max_val):
        """Create colors for point cloud visualization."""
        cmap = cm.get_cmap(colormap_name)

        if colorization_mode == "Distance":
            distances = np.linalg.norm(points_cam, axis=1)
            norm_values = np.clip((distances - min_val) / (max_val - min_val + 1e-6), 0, 1)
        else:  # Intensity
            norm_values = np.clip((intensities - min_val) / (max_val - min_val + 1e-6), 0, 1)

        return cmap(norm_values)

    def update_master_min_max_values(self):
        """Update min/max values for master point cloud."""
        if not hasattr(self, 'master_points_xyz') or self.master_points_xyz.shape[0] == 0:
            return

        colorization_mode = self.master_colorization_mode_combo.currentText()
        if colorization_mode == "Distance":
            if hasattr(self, 'master_valid_indices') and len(self.master_valid_indices) > 0:
                tvec = self.master_extrinsics[:3, 3]
                points_cam = (self.master_extrinsics[:3, :3] @ self.master_points_xyz.T).T + tvec
                valid_points_cam = points_cam[self.master_valid_indices]
                distances = np.linalg.norm(valid_points_cam, axis=1)
                min_dist, max_dist = np.quantile(distances, [0.01, 0.99])
                self.master_min_value_spinbox.setValue(min_dist)
                self.master_max_value_spinbox.setValue(max_dist)
        else:  # Intensity
            if hasattr(self, 'master_intensities') and self.master_intensities.size > 0:
                min_i, max_i = np.quantile(self.master_intensities, [0.01, 0.90])
                self.master_min_value_spinbox.setValue(min_i)
                self.master_max_value_spinbox.setValue(max_i)

    def update_second_min_max_values(self):
        """Update min/max values for second point cloud."""
        if not hasattr(self, 'second_points_xyz') or self.second_points_xyz.shape[0] == 0:
            return

        colorization_mode = self.second_colorization_mode_combo.currentText()
        if colorization_mode == "Distance":
            if hasattr(self, 'second_valid_indices') and len(self.second_valid_indices) > 0:
                tvec = self.second_extrinsics[:3, 3]
                points_cam = (self.second_extrinsics[:3, :3] @ self.second_points_xyz.T).T + tvec
                valid_points_cam = points_cam[self.second_valid_indices]
                distances = np.linalg.norm(valid_points_cam, axis=1)
                min_dist, max_dist = np.quantile(distances, [0.01, 0.99])
                self.second_min_value_spinbox.setValue(min_dist)
                self.second_max_value_spinbox.setValue(max_dist)
        else:  # Intensity
            if hasattr(self, 'second_intensities') and self.second_intensities.size > 0:
                min_i, max_i = np.quantile(self.second_intensities, [0.01, 0.90])
                self.second_min_value_spinbox.setValue(min_i)
                self.second_max_value_spinbox.setValue(max_i)

    def toggle_master_visibility(self, visible):
        """Toggle master point cloud visibility."""
        self.master_visible = visible
        self.project_master_pointcloud(re_read_cloud=False)

    def toggle_second_visibility(self, visible):
        """Toggle second point cloud visibility."""
        self.second_visible = visible
        self.project_second_pointcloud(re_read_cloud=False)

    def update_master_visualization(self):
        """Update master point cloud visualization."""
        self.project_master_pointcloud(re_read_cloud=False)

    def update_second_visualization(self):
        """Update second point cloud visualization."""
        self.project_second_pointcloud(re_read_cloud=False)

    def update_manual_inputs(self):
        """Update manual input fields from current transformations."""
        # Master transform
        master_tvec = self.master_extrinsics[:3, 3]
        master_rpy = Rotation.from_matrix(self.master_extrinsics[:3, :3]).as_euler("xyz", degrees=True)

        self.master_dof_widgets["x"].setValue(master_tvec[0])
        self.master_dof_widgets["y"].setValue(master_tvec[1])
        self.master_dof_widgets["z"].setValue(master_tvec[2])
        self.master_dof_widgets["roll"].setValue(master_rpy[0])
        self.master_dof_widgets["pitch"].setValue(master_rpy[1])
        self.master_dof_widgets["yaw"].setValue(master_rpy[2])

        # Second transform
        second_tvec = self.second_extrinsics[:3, 3]
        second_rpy = Rotation.from_matrix(self.second_extrinsics[:3, :3]).as_euler("xyz", degrees=True)

        self.second_dof_widgets["x"].setValue(second_tvec[0])
        self.second_dof_widgets["y"].setValue(second_tvec[1])
        self.second_dof_widgets["z"].setValue(second_tvec[2])
        self.second_dof_widgets["roll"].setValue(second_rpy[0])
        self.second_dof_widgets["pitch"].setValue(second_rpy[1])
        self.second_dof_widgets["yaw"].setValue(second_rpy[2])

    def adjust_dof(self, prefix, dof, direction):
        """Adjust degree of freedom for specified point cloud."""
        dof_widgets = getattr(self, f"{prefix}_dof_widgets")
        t_step_spinbox = getattr(self, f"{prefix}_t_step_spinbox")
        r_step_spinbox = getattr(self, f"{prefix}_r_step_spinbox")

        spinbox = dof_widgets[dof]
        step = t_step_spinbox.value() / 100.0 if dof in "xyz" else r_step_spinbox.value()
        spinbox.setValue(spinbox.value() + direction * step)

    def update_extrinsics_from_inputs(self, prefix, value=None):
        """Update extrinsics from manual input fields."""
        dof_widgets = getattr(self, f"{prefix}_dof_widgets")

        x = dof_widgets["x"].value()
        y = dof_widgets["y"].value()
        z = dof_widgets["z"].value()
        roll = dof_widgets["roll"].value()
        pitch = dof_widgets["pitch"].value()
        yaw = dof_widgets["yaw"].value()

        extrinsics = np.identity(4)
        extrinsics[:3, 3] = [x, y, z]
        extrinsics[:3, :3] = Rotation.from_euler("xyz", [roll, pitch, yaw], degrees=True).as_matrix()

        if prefix == "master":
            self.master_extrinsics = extrinsics
            self.project_master_pointcloud(re_read_cloud=False)
        else:
            self.second_extrinsics = extrinsics
            self.project_second_pointcloud(re_read_cloud=False)

    def clean_master_occlusion(self):
        """Clean occluded points from master point cloud."""
        self.clean_occlusion("master")

    def clean_second_occlusion(self):
        """Clean occluded points from second point cloud."""
        self.clean_occlusion("second")

    def clean_occlusion(self, prefix):
        """Clean occluded points for specified point cloud."""
        print(f"Running occlusion cleaning for {prefix} point cloud...")
        self.progress_bar.setVisible(True)
        QApplication.processEvents()

        K = np.array(self.camerainfo_msg.k).reshape(3, 3)
        h, w = self.camerainfo_msg.height, self.camerainfo_msg.width

        if prefix == "master":
            extrinsics_3x4 = self.master_extrinsics[:3, :]
            points_xyz = self.master_points_xyz.T
            cleaner = LiDARCleaner(K, extrinsics_3x4, points_xyz, h, w)
            self.master_occlusion_mask = cleaner.run()
            num_removed = np.sum(~self.master_occlusion_mask)
            self.project_master_pointcloud(re_read_cloud=False)
        else:
            extrinsics_3x4 = self.second_extrinsics[:3, :]
            points_xyz = self.second_points_xyz.T
            cleaner = LiDARCleaner(K, extrinsics_3x4, points_xyz, h, w)
            self.second_occlusion_mask = cleaner.run()
            num_removed = np.sum(~self.second_occlusion_mask)
            self.project_second_pointcloud(re_read_cloud=False)

        print(f"Occlusion cleaning finished for {prefix}. {num_removed} points identified as occluded.")
        self.progress_bar.setVisible(False)

    def reset_calibration_state(self):
        """Reset all calibration state."""
        self.master_cam_correspondences = {}
        self.second_cam_correspondences = {}
        self.lidar_to_lidar_correspondences = {}

        self.master_extrinsics = np.copy(self.initial_master_transform)
        self.second_extrinsics = np.copy(self.initial_second_transform)

        self.master_occlusion_mask = None
        self.second_occlusion_mask = None

        self.update_correspondence_lists()
        self.update_manual_inputs()
        self.project_master_pointcloud()
        self.project_second_pointcloud()
        self.clear_all_highlighting()
        self.reset_selection_mode()

        self.results_label.setText("Ready for calibration")

    def toggle_selection_mode(self, checked):
        """Toggle correspondence selection mode."""
        if checked:
            corr_mode = self.correspondence_mode_combo.currentText()
            if "Camera" in corr_mode:
                self.selection_mode = "wait_for_2d_click"
                self.add_corr_button.setText("1. Click on 2D Image Point")
            else:  # LiDAR to LiDAR
                self.selection_mode = "wait_for_second_lidar_click"
                self.add_corr_button.setText("1. Click on Second LiDAR Point")
            self.view.setDragMode(QGraphicsView.NoDrag)
        else:
            self.reset_selection_mode()

    def reset_selection_mode(self):
        """Reset selection mode."""
        self.selection_mode = None
        self.selected_2d_point = None
        self.clear_temp_markers()
        self.clear_current_selection()
        self.add_corr_button.setChecked(False)
        self.add_corr_button.setText("Add Correspondence")
        self.confirm_3d_button.setVisible(False)
        self.view.setDragMode(QGraphicsView.ScrollHandDrag)

    def clear_current_selection(self):
        """Clear current 3D selection."""
        for item in self.current_3d_selection:
            if item.scene():
                self.scene.removeItem(item)
        self.current_3d_selection = []
        self.selected_3d_items_map = {}

    def clear_temp_markers(self):
        """Clear temporary markers."""
        for item in self.temp_2d_marker:
            if item.scene():
                self.scene.removeItem(item)
        self.temp_2d_marker = []

    def clear_all_highlighting(self):
        """Clear all highlighting."""
        self.clear_temp_markers()
        self.clear_current_selection()
        for item in self.highlighted_3d_items:
            if item.scene():
                self.scene.removeItem(item)
        self.highlighted_3d_items = []

    def eventFilter(self, source, event):
        """Handle mouse events for correspondence selection."""
        if (
            source is self.view.viewport()
            and event.type() == QEvent.MouseButtonRelease
            and event.button() == Qt.LeftButton
        ):
            if self.selection_mode == "wait_for_2d_click":
                self.handle_2d_point_selection(event.pos())
                return True
            elif self.selection_mode == "wait_for_3d_clicks":
                self.handle_3d_point_selection(event.pos())
                return True
            elif self.selection_mode == "wait_for_second_lidar_click":
                self.handle_second_lidar_selection(event.pos())
                return True
            elif self.selection_mode == "wait_for_master_lidar_clicks":
                self.handle_master_lidar_selection(event.pos())
                return True
        return super().eventFilter(source, event)

    def handle_2d_point_selection(self, pos):
        """Handle 2D point selection in image."""
        self.clear_temp_markers()
        scene_pos = self.view.mapToScene(pos)
        self.selected_2d_point = (scene_pos.x(), scene_pos.y())
        self.draw_cross_marker(scene_pos, QColor(Colors.CORRESPONDENCE_2D))

        # Determine next mode based on correspondence type
        corr_mode = self.correspondence_mode_combo.currentText()
        if f"{self.master_display_name} ↔ Camera" == corr_mode:
            self.selection_mode = "wait_for_3d_clicks"
            self.add_corr_button.setText(f"2. Click on {self.master_display_name} Point(s)")
        else:  # Second LiDAR
            self.selection_mode = "wait_for_3d_clicks"
            self.add_corr_button.setText(f"2. Click on {self.second_display_name} Point(s)")

        self.confirm_3d_button.setVisible(True)

    def handle_3d_point_selection(self, pos):
        """Handle 3D point selection."""
        corr_mode = self.correspondence_mode_combo.currentText()

        if f"{self.master_display_name} ↔ Camera" == corr_mode:
            self.handle_master_point_selection(pos)
        else:  # Second LiDAR
            self.handle_second_point_selection(pos)

    def handle_master_point_selection(self, pos):
        """Handle master LiDAR point selection."""
        if self.master_kdtree is None:
            return

        scene_pos = self.view.mapToScene(pos)
        dist, idx = self.master_kdtree.query([scene_pos.x(), scene_pos.y()], k=1)
        if dist > self.master_point_size_spinbox.value() * 1.5:
            return

        self.toggle_point_selection(idx, self.master_points_proj_valid,
                                  self.master_point_size_spinbox.value(),
                                  QColor(0, 255, 0))  # Green for master

    def handle_second_point_selection(self, pos):
        """Handle second LiDAR point selection."""
        if self.second_kdtree is None:
            return

        scene_pos = self.view.mapToScene(pos)
        dist, idx = self.second_kdtree.query([scene_pos.x(), scene_pos.y()], k=1)
        if dist > self.second_point_size_spinbox.value() * 1.5:
            return

        # Offset index to distinguish from master indices
        offset_idx = idx + 100000  # Large offset to avoid conflicts
        self.toggle_point_selection(offset_idx, self.second_points_proj_valid,
                                  self.second_point_size_spinbox.value(),
                                  QColor(255, 0, 255))  # Magenta for second

    def handle_second_lidar_selection(self, pos):
        """Handle second LiDAR point selection for LiDAR-to-LiDAR correspondence."""
        if self.second_kdtree is None:
            return

        self.clear_temp_markers()
        scene_pos = self.view.mapToScene(pos)
        dist, idx = self.second_kdtree.query([scene_pos.x(), scene_pos.y()], k=1)
        if dist > self.second_point_size_spinbox.value() * 1.5:
            return

        # Store selected second LiDAR point
        self.selected_second_lidar_point = self.second_points_proj_valid[idx]
        self.selected_second_lidar_3d_idx = self.second_valid_indices[idx]

        # Draw marker
        point_2d = self.second_points_proj_valid[idx]
        self.draw_cross_marker(QPointF(point_2d[0], point_2d[1]), QColor(255, 0, 255))

        # Move to next mode
        self.selection_mode = "wait_for_master_lidar_clicks"
        self.add_corr_button.setText(f"2. Click on {self.master_display_name} Point(s)")
        self.confirm_3d_button.setVisible(True)

    def handle_master_lidar_selection(self, pos):
        """Handle master LiDAR point selection for LiDAR-to-LiDAR correspondence."""
        if self.master_kdtree is None:
            return

        scene_pos = self.view.mapToScene(pos)
        dist, idx = self.master_kdtree.query([scene_pos.x(), scene_pos.y()], k=1)
        if dist > self.master_point_size_spinbox.value() * 1.5:
            return

        self.toggle_point_selection(idx, self.master_points_proj_valid,
                                  self.master_point_size_spinbox.value(),
                                  QColor(0, 255, 0))  # Green for master

    def toggle_point_selection(self, idx, points_proj, point_size, color):
        """Toggle point selection state."""
        if idx in self.selected_3d_items_map:
            # Remove selection
            item_to_remove = self.selected_3d_items_map.pop(idx)
            if item_to_remove in self.scene.items():
                self.scene.removeItem(item_to_remove)
            self.current_3d_selection.remove(item_to_remove)
        else:
            # Add selection
            if idx >= 100000:  # Second LiDAR point (offset)
                real_idx = idx - 100000
                point_2d = points_proj[real_idx]
            else:  # Master LiDAR point
                point_2d = points_proj[idx]

            item = QGraphicsEllipseItem(
                point_2d[0] - point_size / 2, point_2d[1] - point_size / 2,
                point_size, point_size
            )
            item.setPen(QPen(color, 2))
            item.setBrush(QBrush(color))
            item.setData(0, idx)
            self.scene.addItem(item)
            self.current_3d_selection.append(item)
            self.selected_3d_items_map[idx] = item

    def draw_cross_marker(self, center, color):
        """Draw cross marker at specified position."""
        pen = QPen(color, 2)
        size = 10
        l1 = self.scene.addLine(center.x() - size, center.y(), center.x() + size, center.y(), pen)
        l2 = self.scene.addLine(center.x(), center.y() - size, center.x(), center.y() + size, pen)
        self.temp_2d_marker.extend([l1, l2])

    def finalize_correspondence(self):
        """Finalize the current correspondence."""
        if not self.current_3d_selection:
            self.reset_selection_mode()
            return

        corr_mode = self.correspondence_mode_combo.currentText()

        if f"{self.master_display_name} ↔ Camera" == corr_mode:
            self.finalize_master_cam_correspondence()
        elif f"{self.second_display_name} ↔ Camera" == corr_mode:
            self.finalize_second_cam_correspondence()
        else:  # Second LiDAR ↔ Master LiDAR
            self.finalize_lidar_lidar_correspondence()

        self.update_correspondence_lists()
        self.reset_selection_mode()

    def finalize_master_cam_correspondence(self):
        """Finalize master LiDAR to camera correspondence."""
        if self.selected_2d_point is None:
            return

        # Get selected 3D points (should be master points only)
        selected_indices = [item.data(0) for item in self.current_3d_selection if item.data(0) < 100000]
        if not selected_indices:
            return

        original_indices = [self.master_valid_indices[i] for i in selected_indices]
        mean_3d_point = np.mean(self.master_points_xyz[original_indices], axis=0)

        self.master_cam_correspondences[self.selected_2d_point] = {
            "3d_mean": mean_3d_point,
            "3d_points_indices": original_indices,
        }

    def finalize_second_cam_correspondence(self):
        """Finalize second LiDAR to camera correspondence."""
        if self.selected_2d_point is None:
            return

        # Get selected 3D points (should be second points only, with offset)
        selected_indices = [item.data(0) - 100000 for item in self.current_3d_selection if item.data(0) >= 100000]
        if not selected_indices:
            return

        original_indices = [self.second_valid_indices[i] for i in selected_indices]
        mean_3d_point = np.mean(self.second_points_xyz[original_indices], axis=0)

        self.second_cam_correspondences[self.selected_2d_point] = {
            "3d_mean": mean_3d_point,
            "3d_points_indices": original_indices,
        }

    def finalize_lidar_lidar_correspondence(self):
        """Finalize second LiDAR to master LiDAR correspondence."""
        if not hasattr(self, 'selected_second_lidar_point'):
            return

        # Get selected master points
        selected_indices = [item.data(0) for item in self.current_3d_selection if item.data(0) < 100000]
        if not selected_indices:
            return

        original_indices = [self.master_valid_indices[i] for i in selected_indices]
        mean_3d_point = np.mean(self.master_points_xyz[original_indices], axis=0)

        # Store correspondence using second LiDAR 3D point as key
        second_lidar_3d = self.second_points_xyz[self.selected_second_lidar_3d_idx]
        self.lidar_to_lidar_correspondences[tuple(second_lidar_3d)] = {
            "master_3d_mean": mean_3d_point,
            "master_3d_points_indices": original_indices,
            "second_lidar_index": self.selected_second_lidar_3d_idx,
        }

        # Clean up selection attributes
        if hasattr(self, 'selected_second_lidar_point'):
            delattr(self, 'selected_second_lidar_point')
        if hasattr(self, 'selected_second_lidar_3d_idx'):
            delattr(self, 'selected_second_lidar_3d_idx')

    def update_correspondence_lists(self):
        """Update all correspondence list widgets."""
        # Master-Camera correspondences
        self.master_cam_list_widget.clear()
        for p2d, corr_data in self.master_cam_correspondences.items():
            p3d = corr_data["3d_mean"]
            item_text = f"Cam ({p2d[0]:.1f}, {p2d[1]:.1f}) ↔ {self.master_display_name} ({p3d[0]:.2f}, {p3d[1]:.2f}, {p3d[2]:.2f})"
            item = QListWidgetItem(item_text)
            item.setData(Qt.UserRole, ("master_cam", p2d))
            self.master_cam_list_widget.addItem(item)

        # Second-Camera correspondences
        self.second_cam_list_widget.clear()
        for p2d, corr_data in self.second_cam_correspondences.items():
            p3d = corr_data["3d_mean"]
            item_text = f"Cam ({p2d[0]:.1f}, {p2d[1]:.1f}) ↔ {self.second_display_name} ({p3d[0]:.2f}, {p3d[1]:.2f}, {p3d[2]:.2f})"
            item = QListWidgetItem(item_text)
            item.setData(Qt.UserRole, ("second_cam", p2d))
            self.second_cam_list_widget.addItem(item)

        # LiDAR-LiDAR correspondences
        self.lidar_lidar_list_widget.clear()
        for second_3d, corr_data in self.lidar_to_lidar_correspondences.items():
            master_3d = corr_data["master_3d_mean"]
            item_text = f"{self.second_display_name} ({second_3d[0]:.2f}, {second_3d[1]:.2f}, {second_3d[2]:.2f}) ↔ {self.master_display_name} ({master_3d[0]:.2f}, {master_3d[1]:.2f}, {master_3d[2]:.2f})"
            item = QListWidgetItem(item_text)
            item.setData(Qt.UserRole, ("lidar_lidar", second_3d))
            self.lidar_lidar_list_widget.addItem(item)

    def highlight_master_cam_correspondence(self, current_item, previous_item):
        """Highlight master-camera correspondence."""
        self.highlight_correspondence(current_item, "master_cam")

    def highlight_second_cam_correspondence(self, current_item, previous_item):
        """Highlight second-camera correspondence."""
        self.highlight_correspondence(current_item, "second_cam")

    def highlight_lidar_correspondence(self, current_item, previous_item):
        """Highlight LiDAR-LiDAR correspondence."""
        self.highlight_correspondence(current_item, "lidar_lidar")

    def highlight_correspondence(self, current_item, corr_type):
        """Highlight correspondence based on type."""
        self.clear_all_highlighting()
        if not current_item:
            return

        corr_data = current_item.data(Qt.UserRole)
        if corr_data[0] != corr_type:
            return

        if corr_type == "master_cam":
            self.highlight_master_cam_corr(corr_data[1])
        elif corr_type == "second_cam":
            self.highlight_second_cam_corr(corr_data[1])
        else:  # lidar_lidar
            self.highlight_lidar_lidar_corr(corr_data[1])

    def highlight_master_cam_corr(self, p2d_key):
        """Highlight master-camera correspondence."""
        corr = self.master_cam_correspondences.get(p2d_key)
        if not corr:
            return

        # Highlight 2D point
        self.draw_cross_marker(QPointF(p2d_key[0], p2d_key[1]), QColor(Colors.CORRESPONDENCE_2D))

        # Highlight 3D points
        self.highlight_3d_points(corr["3d_points_indices"], self.master_valid_indices,
                               self.master_points_proj_valid, self.master_point_size_spinbox.value(),
                               QColor(0, 255, 0))

    def highlight_second_cam_corr(self, p2d_key):
        """Highlight second-camera correspondence."""
        corr = self.second_cam_correspondences.get(p2d_key)
        if not corr:
            return

        # Highlight 2D point
        self.draw_cross_marker(QPointF(p2d_key[0], p2d_key[1]), QColor(Colors.CORRESPONDENCE_2D))

        # Highlight 3D points
        self.highlight_3d_points(corr["3d_points_indices"], self.second_valid_indices,
                               self.second_points_proj_valid, self.second_point_size_spinbox.value(),
                               QColor(255, 0, 255))

    def highlight_lidar_lidar_corr(self, second_3d_key):
        """Highlight LiDAR-LiDAR correspondence."""
        corr = self.lidar_to_lidar_correspondences.get(second_3d_key)
        if not corr:
            return

        # Highlight second LiDAR point
        second_idx = corr["second_lidar_index"]
        if hasattr(self, 'second_valid_indices') and hasattr(self, 'second_points_proj_valid'):
            second_valid_idx_map = {
                orig_idx: valid_idx for valid_idx, orig_idx in enumerate(self.second_valid_indices)
            }
            second_valid_idx = second_valid_idx_map.get(second_idx)
            if second_valid_idx is not None and second_valid_idx < len(self.second_points_proj_valid):
                point_2d = self.second_points_proj_valid[second_valid_idx]
                self.draw_cross_marker(QPointF(point_2d[0], point_2d[1]), QColor(255, 0, 255))

        # Highlight master LiDAR points
        self.highlight_3d_points(corr["master_3d_points_indices"], self.master_valid_indices,
                               self.master_points_proj_valid, self.master_point_size_spinbox.value(),
                               QColor(0, 255, 0))

    def highlight_3d_points(self, original_indices, valid_indices, points_proj_valid, point_size, color):
        """Highlight 3D points."""
        original_to_valid_idx_map = {
            orig_idx: valid_idx for valid_idx, orig_idx in enumerate(valid_indices)
        }

        for original_point_idx in original_indices:
            valid_idx = original_to_valid_idx_map.get(original_point_idx)
            if valid_idx is not None and valid_idx < len(points_proj_valid):
                point_2d = points_proj_valid[valid_idx]
                item = QGraphicsEllipseItem(
                    point_2d[0] - point_size / 2,
                    point_2d[1] - point_size / 2,
                    point_size,
                    point_size,
                )
                item.setPen(QPen(color, 2))
                item.setBrush(QBrush(color))
                self.scene.addItem(item)
                self.highlighted_3d_items.append(item)

    def delete_correspondence(self):
        """Delete selected correspondence."""
        # Check which list has selection
        current_master_item = self.master_cam_list_widget.currentItem()
        current_second_item = self.second_cam_list_widget.currentItem()
        current_lidar_item = self.lidar_lidar_list_widget.currentItem()

        if current_master_item:
            corr_data = current_master_item.data(Qt.UserRole)
            if corr_data[0] == "master_cam":
                p2d_key = corr_data[1]
                if p2d_key in self.master_cam_correspondences:
                    del self.master_cam_correspondences[p2d_key]

        elif current_second_item:
            corr_data = current_second_item.data(Qt.UserRole)
            if corr_data[0] == "second_cam":
                p2d_key = corr_data[1]
                if p2d_key in self.second_cam_correspondences:
                    del self.second_cam_correspondences[p2d_key]

        elif current_lidar_item:
            corr_data = current_lidar_item.data(Qt.UserRole)
            if corr_data[0] == "lidar_lidar":
                second_3d_key = corr_data[1]
                if second_3d_key in self.lidar_to_lidar_correspondences:
                    del self.lidar_to_lidar_correspondences[second_3d_key]

        self.update_correspondence_lists()
        self.clear_all_highlighting()

    def run_calibration(self):
        """Run dual LiDAR calibration."""
        # Check if we have enough correspondences
        master_cam_count = len(self.master_cam_correspondences)
        second_cam_count = len(self.second_cam_correspondences)
        lidar_lidar_count = len(self.lidar_to_lidar_correspondences)

        if master_cam_count < 4:
            self.results_label.setText(f"Error: Need at least 4 {self.master_display_name} ↔ Camera correspondences")
            return

        if second_cam_count < 4 and lidar_lidar_count < 3:
            self.results_label.setText(f"Error: Need at least 4 {self.second_display_name} ↔ Camera OR 3 LiDAR ↔ LiDAR correspondences")
            return

        self.progress_bar.setVisible(True)
        QApplication.processEvents()

        try:
            lsq_method = self.lsq_method_combo.currentText()
            K = np.array(self.camerainfo_msg.k).reshape(3, 3)

            # Prepare master LiDAR to camera correspondences
            master_cam_corr = [(p2d, corr["3d_mean"]) for p2d, corr in self.master_cam_correspondences.items()]

            # Use global optimization for best results
            second_cam_corr = [(p2d, corr["3d_mean"]) for p2d, corr in self.second_cam_correspondences.items()]

            print("[DEBUG] Using global dual LiDAR optimization")
            print(f"[DEBUG] Master-cam: {master_cam_count}, Second-cam: {second_cam_count}, LiDAR-LiDAR: {lidar_lidar_count}")

            self.master_extrinsics, self.second_extrinsics = calibration.calibrate_dual_lidar_global(
                master_cam_corr,
                second_cam_corr,
                self.lidar_to_lidar_correspondences,
                K,
                self.master_extrinsics,  # Use current transforms as initial guess
                self.second_extrinsics,
                lsq_method
            )

            if second_cam_count >= 4 and lidar_lidar_count >= 3:
                self.results_label.setText("Global optimization completed using all correspondence types")
            elif second_cam_count >= 4:
                self.results_label.setText("Global optimization completed using direct camera correspondences")
            elif lidar_lidar_count >= 3:
                self.results_label.setText("Global optimization completed using LiDAR-to-LiDAR correspondences")
            else:
                self.results_label.setText("Calibration completed with limited correspondences")

            # Update visualizations
            self.project_master_pointcloud(re_read_cloud=False)
            self.project_second_pointcloud(re_read_cloud=False)
            self.update_manual_inputs()

        except Exception as e:
            self.results_label.setText(f"Calibration failed: {str(e)}")
            print(f"Calibration error: {e}")

        finally:
            self.progress_bar.setVisible(False)

    def view_calibration_results(self):
        """Emit calibration results."""
        calibration_results = {
            'mode': 'dual_lidar',
            'master_frame_id': self.master_frame_id,
            'second_frame_id': self.second_frame_id,
            'camera_frame_id': self.camera_frame_id,
            'master_to_camera': self.master_extrinsics,
            'second_to_camera': self.second_extrinsics,
        }

        self.calibration_completed.emit(calibration_results)
