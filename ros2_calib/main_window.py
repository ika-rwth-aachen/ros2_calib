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

import os
from typing import Dict, List, Optional

import numpy as np
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QDragEnterEvent, QDropEvent
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QStackedWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from . import ros_utils
from . import tf_transformations as tf
from .bag_handler import (
    RosbagProcessingWorker,
    convert_to_mock,
    get_topic_info,
    get_total_message_count,
)
from .calibration_widget import CalibrationWidget
from .common import UIStyles
from .frame_selection_widget import FrameSelectionWidget
from .lidar2lidar_o3d_widget import launch_lidar2lidar_calibration
from .tf_graph_widget import TFGraphWidget


class MainWindow(QMainWindow):
    # Signal for thread-safe calibration completion
    calibration_completed = Signal(np.ndarray)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("ros2_calib - Multi-Sensor Calibration Tool")
        self.setGeometry(100, 100, 1800, 800)
        self.setAcceptDrops(True)

        # Connect signals for thread-safe communication
        self.calibration_completed.connect(self.show_calibration_results)

        # Initialize data containers
        self.topics = {}
        self.bag_file = None
        self.selected_topics = {}
        self.tf_tree = {}
        self.current_transform = np.eye(4, dtype=np.float64)
        self.calibration_type = "LiDAR2Cam"  # Default type
        self.calibrated_transform = np.eye(4, dtype=np.float64)
        self.original_source_frame = ""
        self.original_target_frame = ""
        self.tf_graph_window = None # To hold a reference to the pop-up window

        # Set up stacked widget for multiple views
        self.stacked_widget = QStackedWidget()
        self.setCentralWidget(self.stacked_widget)

        # Create and add the different views
        self.setup_calibration_type_view()
        self.setup_load_view()
        self.setup_transform_view()
        self.setup_frame_selection_view()
        self.setup_results_view()

        # Start with the calibration type selection view
        self.stacked_widget.setCurrentIndex(0)

    def setup_calibration_type_view(self):
        """Setup the calibration type selection view."""
        self.calib_type_widget = QWidget()
        self.calib_type_layout = QVBoxLayout(self.calib_type_widget)

        self.calib_type_layout.addStretch()
        title_label = QLabel("Select Calibration Type")
        title_label.setStyleSheet("font-size: 24px; font-weight: bold; margin: 20px;")
        title_label.setAlignment(Qt.AlignCenter)
        self.calib_type_layout.addWidget(title_label)
        desc_label = QLabel("Choose the type of calibration you want to perform:")
        desc_label.setStyleSheet("font-size: 14px; margin: 10px; color: #666;")
        desc_label.setAlignment(Qt.AlignCenter)
        self.calib_type_layout.addWidget(desc_label)

        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.addStretch()

        self.lidar2cam_button = QPushButton("LiDAR ↔ Camera\nCalibration")
        self.lidar2cam_button.setStyleSheet(UIStyles.HIGHLIGHT_BUTTON + "QPushButton { min-width: 180px; min-height: 80px; font-size: 16px; margin: 15px; }")
        self.lidar2cam_button.clicked.connect(lambda: self.select_calibration_type("LiDAR2Cam"))
        button_layout.addWidget(self.lidar2cam_button)

        # Add spacing between buttons
        button_layout.addSpacing(40)

        self.lidar2lidar_button = QPushButton("LiDAR ↔ LiDAR\nCalibration")
        self.lidar2lidar_button.setStyleSheet(UIStyles.HIGHLIGHT_BUTTON + "QPushButton { min-width: 180px; min-height: 80px; font-size: 16px; margin: 15px; }")
        self.lidar2lidar_button.clicked.connect(lambda: self.select_calibration_type("LiDAR2LiDAR"))
        button_layout.addWidget(self.lidar2lidar_button)

        button_layout.addStretch()
        self.calib_type_layout.addWidget(button_container)
        self.calib_type_layout.addStretch()
        self.stacked_widget.addWidget(self.calib_type_widget)

    def select_calibration_type(self, calib_type):
        """Handle calibration type selection."""
        self.calibration_type = calib_type
        print(f"[DEBUG] Selected calibration type: {calib_type}")
        self.update_load_view_for_calibration_type()
        self.stacked_widget.setCurrentIndex(1)

    def update_load_view_for_calibration_type(self):
        """Update load view UI based on selected calibration type."""
        is_lidar_cam = self.calibration_type == "LiDAR2Cam"
        self.image_label.setVisible(is_lidar_cam)
        self.image_topic_combo.setVisible(is_lidar_cam)
        self.camerainfo_label.setVisible(is_lidar_cam)
        self.camerainfo_topic_combo.setVisible(is_lidar_cam)
        self.pointcloud2_label.setVisible(not is_lidar_cam)
        self.pointcloud2_topic_combo.setVisible(not is_lidar_cam)
        self.pointcloud_label.setText("PointCloud2 Topic:" if is_lidar_cam else "PointCloud2 Topic (Source):")
        self.selection_group.setTitle(f"Topic Selection for {self.calibration_type} Calibration")
        self.proceed_button.setText("Proceed to Frame Selection" if is_lidar_cam else "Proceed to Transform Selection")

    def setup_load_view(self):
        self.load_widget = QWidget()
        self.load_layout = QVBoxLayout(self.load_widget)
        load_section_layout = QHBoxLayout()
        self.load_bag_button = QPushButton("Load Rosbag")
        self.load_bag_button.clicked.connect(self.load_bag)
        load_section_layout.addWidget(self.load_bag_button)
        load_section_layout.addWidget(QLabel("ROS Version:"))
        self.ros_version_combo = QComboBox()
        self.ros_version_combo.addItems(["JAZZY", "HUMBLE"])
        load_section_layout.addWidget(self.ros_version_combo)
        drag_drop_label = QLabel("or Drag & Drop")
        drag_drop_label.setStyleSheet("color: #666; font-style: italic;")
        load_section_layout.addWidget(drag_drop_label)
        self.bag_path_label = QLabel("No rosbag loaded.")
        self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid gray;")
        load_section_layout.addWidget(self.bag_path_label, 1)
        self.load_layout.addLayout(load_section_layout)
        topic_list_group = QGroupBox("Available Topics")
        topic_list_layout = QVBoxLayout(topic_list_group)
        self.topic_list_widget = QListWidget()
        topic_list_layout.addWidget(self.topic_list_widget)
        self.load_layout.addWidget(topic_list_group, 1)

        self.selection_group = QGroupBox()
        self.calib_topic_layout = QFormLayout(self.selection_group)
        self.image_topic_combo = QComboBox()
        self.image_topic_combo.currentIndexChanged.connect(self.auto_select_camera_info)
        self.camerainfo_topic_combo = QComboBox()
        self.pointcloud_topic_combo = QComboBox()
        self.pointcloud_topic_combo.currentTextChanged.connect(self.validate_lidar_topic_selection)
        self.pointcloud2_topic_combo = QComboBox()
        self.pointcloud2_topic_combo.currentTextChanged.connect(self.validate_lidar_topic_selection)
        self.frame_count_spinbox = QSpinBox()
        self.frame_count_spinbox.setRange(3, 20)
        self.frame_count_spinbox.setValue(6)
        self.frame_count_spinbox.setSuffix(" frames")

        self.image_label = QLabel("Image Topic:")
        self.camerainfo_label = QLabel("CameraInfo Topic:")
        self.pointcloud_label = QLabel("PointCloud2 Topic:")
        self.pointcloud2_label = QLabel("PointCloud2 Topic (Target):")

        self.calib_topic_layout.addRow(self.image_label, self.image_topic_combo)
        self.calib_topic_layout.addRow(self.camerainfo_label, self.camerainfo_topic_combo)
        self.calib_topic_layout.addRow(self.pointcloud_label, self.pointcloud_topic_combo)
        self.calib_topic_layout.addRow(self.pointcloud2_label, self.pointcloud2_topic_combo)
        self.calib_topic_layout.addRow("Frame Samples:", self.frame_count_spinbox)

        self.proceed_button = QPushButton()
        self.proceed_button.setEnabled(False)
        self.proceed_button.clicked.connect(self.process_rosbag_data)
        self.proceed_button.setStyleSheet(UIStyles.HIGHLIGHT_BUTTON)
        self.progress_bar = QProgressBar(visible=False, textVisible=True)
        self.calib_topic_layout.addRow(self.proceed_button)
        self.calib_topic_layout.addRow(self.progress_bar)

        self.load_layout.addWidget(self.selection_group)
        self.stacked_widget.addWidget(self.load_widget)
        self.update_load_view_for_calibration_type()

    def setup_transform_view(self):
        self.transform_widget = QWidget()
        self.transform_layout = QVBoxLayout(self.transform_widget)
        self.tf_title_label = QLabel()
        self.tf_title_label.setStyleSheet("font-size: 14px; font-weight: bold; margin: 10px;")
        self.transform_layout.addWidget(self.tf_title_label)

        back_button_layout = QHBoxLayout()
        self.back_button = QPushButton("← Back to Topic Selection")
        self.back_button.clicked.connect(lambda: self.stacked_widget.setCurrentIndex(1))
        back_button_layout.addWidget(self.back_button)
        back_button_layout.addStretch()
        self.transform_layout.addLayout(back_button_layout)

        tf_group = QGroupBox("Transform Source")
        tf_layout = QVBoxLayout(tf_group)
        tf_topic_layout = QHBoxLayout()
        tf_topic_layout.addWidget(QLabel("TF Topic:"))
        self.tf_topic_combo = QComboBox()
        self.tf_topic_combo.currentTextChanged.connect(self.on_tf_topic_changed)
        tf_topic_layout.addWidget(self.tf_topic_combo, 1)
        self.load_tf_button = QPushButton("Load TF Tree")
        self.load_tf_button.clicked.connect(self.load_tf_tree)
        tf_topic_layout.addWidget(self.load_tf_button)
        tf_layout.addLayout(tf_topic_layout)
        self.show_graph_button = QPushButton("Show TF Tree Graph")
        self.show_graph_button.clicked.connect(self.show_tf_graph)
        self.show_graph_button.setEnabled(False)
        tf_layout.addWidget(self.show_graph_button)
        self.tf_info_text = QTextEdit(plainText="No TF data loaded.", maximumHeight=150)
        tf_layout.addWidget(QLabel("TF Tree Information:"))
        tf_layout.addWidget(self.tf_info_text)
        self.transform_layout.addWidget(tf_group)

        manual_group = QGroupBox("Manual Transform Input")
        manual_layout = QGridLayout(manual_group)
        self.tx_input, self.ty_input, self.tz_input = QLineEdit("0.0"), QLineEdit("0.0"), QLineEdit("0.0")
        self.rx_input, self.ry_input, self.rz_input = QLineEdit("0.0"), QLineEdit("0.0"), QLineEdit("0.0")
        manual_layout.addWidget(QLabel("Translation (x, y, z):"), 0, 0)
        manual_layout.addWidget(self.tx_input, 0, 1)
        manual_layout.addWidget(self.ty_input, 0, 2)
        manual_layout.addWidget(self.tz_input, 0, 3)
        manual_layout.addWidget(QLabel("Rotation (roll, pitch, yaw) [rad]:"), 1, 0)
        manual_layout.addWidget(self.rx_input, 1, 1)
        manual_layout.addWidget(self.ry_input, 1, 2)
        manual_layout.addWidget(self.rz_input, 1, 3)
        self.update_manual_button = QPushButton("Update Transform from Manual Input")
        self.update_manual_button.clicked.connect(self.update_manual_transform)
        manual_layout.addWidget(self.update_manual_button, 2, 0, 1, 4)
        self.transform_layout.addWidget(manual_group)

        transform_group = QGroupBox("Current Transformation Matrix")
        transform_layout_inner = QVBoxLayout(transform_group)
        self.transform_display = QTextEdit(maximumHeight=180, readOnly=True, fontFamily="monospace")
        transform_layout_inner.addWidget(self.transform_display)
        self.transform_layout.addWidget(transform_group)

        button_layout = QHBoxLayout()
        self.use_identity_button = QPushButton("Use Identity Transform")
        self.use_identity_button.clicked.connect(self.use_identity_transform)
        button_layout.addWidget(self.use_identity_button)
        button_layout.addStretch()
        self.confirm_button = QPushButton("Start Calibration")
        self.confirm_button.clicked.connect(self.confirm_transformation)
        self.confirm_button.setStyleSheet(UIStyles.HIGHLIGHT_BUTTON)
        button_layout.addWidget(self.confirm_button)
        self.transform_layout.addLayout(button_layout)
        self.update_transform_display()
        self.stacked_widget.addWidget(self.transform_widget)

    def setup_frame_selection_view(self):
        self.frame_selection_widget = FrameSelectionWidget(self)
        self.frame_selection_widget.frame_selected.connect(self.on_frame_selected)
        self.stacked_widget.addWidget(self.frame_selection_widget)

    def setup_results_view(self):
        self.results_widget = QWidget()
        self.results_layout = QVBoxLayout(self.results_widget)
        title = QLabel("Calibration Export")
        title.setStyleSheet("font-size: 18px; font-weight: bold; margin-bottom: 10px;")
        self.results_layout.addWidget(title)
        back_layout = QHBoxLayout()
        self.results_back_button = QPushButton("← Back to Calibration")
        self.results_back_button.clicked.connect(self.go_back_to_calibration)
        back_layout.addWidget(self.results_back_button)

        # Add frame selection dropdowns right next to the back button
        back_layout.addWidget(QLabel("Source Frame:"))
        self.source_frame_combo = QComboBox()
        self.source_frame_combo.currentTextChanged.connect(self.update_target_transform)
        back_layout.addWidget(self.source_frame_combo)

        back_layout.addWidget(QLabel("Target Frame:"))
        self.target_frame_combo = QComboBox()
        self.target_frame_combo.currentTextChanged.connect(self.update_target_transform)
        back_layout.addWidget(self.target_frame_combo)

        back_layout.addStretch()
        self.results_layout.addLayout(back_layout)

        chain_graph_group = QGroupBox("Transformation Path")
        chain_graph_layout = QVBoxLayout(chain_graph_group)
        self.chain_display = QTextEdit(maximumHeight=60, readOnly=True)
        chain_graph_layout.addWidget(self.chain_display)
        self.graph_container = QWidget(minimumHeight=400, maximumHeight=500)
        self.init_graph_placeholder()
        chain_graph_layout.addWidget(self.graph_container)
        self.results_layout.addWidget(chain_graph_group)

        results_content_layout = QHBoxLayout()
        left_group = QGroupBox("Overall Calibrated Transform")
        left_layout = QVBoxLayout(left_group)
        self.calibration_result_display = QTextEdit(readOnly=True, fontFamily="monospace")
        left_layout.addWidget(self.calibration_result_display)
        results_content_layout.addWidget(left_group)
        right_group = QGroupBox("Export Target Transform (Selected Frames)")
        right_layout = QVBoxLayout(right_group)
        self.final_transform_display = QTextEdit(readOnly=True, fontFamily="monospace")
        right_layout.addWidget(self.final_transform_display)
        results_content_layout.addWidget(right_group)
        self.results_layout.addLayout(results_content_layout)

        export_layout = QHBoxLayout()
        export_layout.addStretch()
        self.export_calibration_button = QPushButton("Export Target Transform")
        self.export_calibration_button.clicked.connect(self.export_calibration_result)
        self.export_calibration_button.setStyleSheet(UIStyles.HIGHLIGHT_BUTTON)
        export_layout.addWidget(self.export_calibration_button)
        export_layout.addStretch()
        self.results_layout.addLayout(export_layout)
        self.stacked_widget.addWidget(self.results_widget)

    def init_graph_placeholder(self):
        """Initialize or reset the graph container with a placeholder."""
        layout = self.graph_container.layout()
        if layout is None:
            layout = QVBoxLayout(self.graph_container)

        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

    def load_bag(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open Rosbag", "", "MCAP Rosbag (*.mcap)")
        if file_path:
            self.load_bag_from_path(file_path)

    def find_yaml_file(self, mcap_path):
        directory = os.path.dirname(mcap_path)
        mcap_basename = os.path.splitext(os.path.basename(mcap_path))[0]
        metadata_yaml = os.path.join(directory, "metadata.yaml")
        if os.path.exists(metadata_yaml): return metadata_yaml
        matching_yaml = os.path.join(directory, f"{mcap_basename}.yaml")
        if os.path.exists(matching_yaml): return matching_yaml
        return None

    def process_dropped_path(self, path):
        """Process a dropped file or folder path."""
        self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid red;")
        if os.path.isfile(path) and path.endswith(".mcap"):
            if self.find_yaml_file(path):
                self.load_bag_from_path(path)
            else:
                self.bag_path_label.setText("Error: Corresponding metadata.yaml or matching .yaml not found.")
        elif os.path.isdir(path):
            mcap_files = [f for f in os.listdir(path) if f.endswith(".mcap")]
            if len(mcap_files) == 1:
                self.process_dropped_path(os.path.join(path, mcap_files[0]))
            elif not mcap_files:
                self.bag_path_label.setText("Error: No .mcap file found in the dropped folder.")
            else:
                self.bag_path_label.setText("Error: Multiple .mcap files found. Please drop one.")
        else:
            self.bag_path_label.setText("Error: Please drop a valid .mcap file or folder.")


    def load_bag_from_path(self, file_path):
        """Load bag from a specific file path."""
        try:
            self.bag_file = file_path
            self.bag_path_label.setText(file_path)
            self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid gray;")
            ros_version = self.ros_version_combo.currentText()
            self.topics = get_topic_info(file_path, ros_version)
            self.update_topic_widgets()
        except Exception as e:
            self.bag_path_label.setText(f"Error loading bag: {str(e)}")
            self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid red;")

    def update_topic_widgets(self):
        self.topic_list_widget.clear()
        self.image_topic_combo.clear()
        self.pointcloud_topic_combo.clear()
        self.pointcloud2_topic_combo.clear()
        self.camerainfo_topic_combo.clear()

        topic_types = {
            "image": [t for t, m, _ in self.topics if m in ["sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage"]],
            "pointcloud": [t for t, m, _ in self.topics if m == "sensor_msgs/msg/PointCloud2"],
            "camerainfo": [t for t, m, _ in self.topics if m == "sensor_msgs/msg/CameraInfo"],
        }

        for topic, msgtype, msgcount in self.topics:
            self.topic_list_widget.addItem(f"{topic} ({msgtype}) - {msgcount} messages")

        self.image_topic_combo.addItems(topic_types["image"])
        self.pointcloud_topic_combo.addItems(topic_types["pointcloud"])
        self.pointcloud2_topic_combo.addItems(topic_types["pointcloud"])
        self.camerainfo_topic_combo.addItems(topic_types["camerainfo"])
        self.update_proceed_button_state()

    def auto_select_camera_info(self, index):
        if index == -1:
            return
        image_topic = self.image_topic_combo.currentText()
        base_path = image_topic.rsplit('/', 1)[0]
        possible_topics = list(dict.fromkeys([
            f"{base_path}/camera_info",
            image_topic.replace("/image_raw", "/camera_info"),
            image_topic.replace("/image_color", "/camera_info"),
            image_topic.replace("/image_rect_color", "/camera_info"),
        ]))
        for topic in possible_topics:
            if (found_index := self.camerainfo_topic_combo.findText(topic, Qt.MatchExactly)) != -1:
                self.camerainfo_topic_combo.setCurrentIndex(found_index)
                return

    def validate_lidar_topic_selection(self):
        if self.calibration_type != "LiDAR2LiDAR": return
        source_topic = self.pointcloud_topic_combo.currentText()
        target_topic = self.pointcloud2_topic_combo.currentText()
        if source_topic and source_topic == target_topic:
            self.pointcloud2_topic_combo.blockSignals(True)
            for i in range(self.pointcloud2_topic_combo.count()):
                if self.pointcloud2_topic_combo.itemText(i) != source_topic:
                    self.pointcloud2_topic_combo.setCurrentIndex(i)
                    break
            self.pointcloud2_topic_combo.blockSignals(False)
        self.update_proceed_button_state()

    def update_proceed_button_state(self):
        if self.calibration_type == "LiDAR2Cam":
            is_valid = all([self.image_topic_combo.currentText(), self.pointcloud_topic_combo.currentText(), self.camerainfo_topic_combo.currentText()])
        else:
            is_valid = all([self.pointcloud_topic_combo.currentText(), self.pointcloud2_topic_combo.currentText(), self.pointcloud_topic_combo.currentText() != self.pointcloud2_topic_combo.currentText()])
        self.proceed_button.setEnabled(is_valid)

    def process_rosbag_data(self):
        """Read and process required data from the rosbag in a worker thread."""
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.proceed_button.setEnabled(False)
        self.repaint()

        # This dictionary maps all available topic names to their types
        topic_types = {topic: msgtype for topic, msgtype, _ in self.topics}
        tf_topics = [topic for topic in topic_types if "tf" in topic.lower() and "TFMessage" in topic_types[topic]]

        if self.calibration_type == "LiDAR2Cam":
            selected_topics_data = {
                "calibration_type": self.calibration_type,
                "image_topic": self.image_topic_combo.currentText(),
                "pointcloud_topic": self.pointcloud_topic_combo.currentText(),
                "camerainfo_topic": self.camerainfo_topic_combo.currentText(),
                "tf_topics": tf_topics,
            }
        else:  # LiDAR2LiDAR
            selected_topics_data = {
                "calibration_type": self.calibration_type,
                "pointcloud_topic": self.pointcloud_topic_combo.currentText(),
                "pointcloud2_topic": self.pointcloud2_topic_combo.currentText(),
                "tf_topics": tf_topics,
            }

        # --- THIS IS THE FIX ---
        # Correctly build the topics_to_read dictionary.
        # The KEYS should be the actual topic names (e.g., '/drivers/lidar/points')
        # and the VALUES should be their message types.
        topics_to_read = {}
        for key, topic_name in selected_topics_data.items():
            # Find all keys that represent a user-selected topic
            if key.endswith('_topic') and topic_name:
                # Use the topic_name (the value) as the key for our new dict
                if topic_name in topic_types:
                    topics_to_read[topic_name] = topic_types[topic_name]

        # Add all found TF topics to the list of topics to be read
        for tf_topic in tf_topics:
            if tf_topic in topic_types:
                topics_to_read[tf_topic] = topic_types[tf_topic]

        # The worker receives the correctly structured dictionary
        self.processing_worker = RosbagProcessingWorker(
            bag_file=self.bag_file,
            topics_to_read=topics_to_read,
            selected_topics_data=selected_topics_data,
            total_messages=get_total_message_count(self.bag_file, self.ros_version_combo.currentText()),
            frame_samples=self.frame_count_spinbox.value(),
            topic_message_counts={name: count for name, _, count in self.topics},
            ros_version=self.ros_version_combo.currentText(),
            sync_tolerance=0.05
        )
        self.processing_worker.progress_updated.connect(self.update_processing_progress)
        self.processing_worker.processing_finished.connect(self.on_processing_finished)
        self.processing_worker.processing_failed.connect(self.on_processing_failed)
        self.processing_worker.start()

    def update_processing_progress(self, value, message):
        self.progress_bar.setValue(value)
        self.progress_bar.setFormat(message)

    def on_processing_finished(self, raw_messages: Dict, topic_types: Dict, selected_topics_data: Dict):
        """
        Handles completion of rosbag processing. Correctly routes to frame selection
        for LiDAR2Cam or directly to transform selection for LiDAR2LiDAR.
        This method is now robust against different return formats from the worker.
        """
        print("[DEBUG] Rosbag processing completed successfully.")
        self.progress_bar.setVisible(False)
        self.proceed_button.setEnabled(True)

        self.topic_types = topic_types
        self.selected_topics_data = selected_topics_data

        self.tf_messages = {
            topic: raw_messages.get(topic)
            for topic in selected_topics_data.get("tf_topics", []) if topic in raw_messages
        }

        # Check if the worker returned multiple frame samples or a single synchronized message set.
        if "frame_samples" in raw_messages and raw_messages["frame_samples"]:
            print("[DEBUG] Processing multi-frame samples.")
            self.frame_samples = raw_messages["frame_samples"]

            if self.calibration_type == "LiDAR2Cam":
                pointcloud_topic = selected_topics_data["pointcloud_topic"]
                camerainfo_topic = selected_topics_data["camerainfo_topic"]

                self.lidar_frame = self.extract_frame_id(self.frame_samples[pointcloud_topic][0]["data"])
                self.camera_frame = self.extract_frame_id(self.frame_samples[camerainfo_topic][0]["data"])

                self.frame_selection_widget.set_frame_samples(self.frame_samples, selected_topics_data["image_topic"])
                self.stacked_widget.setCurrentIndex(3) # Switch to Frame Selection View

            else: # LiDAR2LiDAR multi-frame (take the first)
                pointcloud_topic = selected_topics_data["pointcloud_topic"]
                pointcloud2_topic = selected_topics_data["pointcloud2_topic"]

                if not self.frame_samples.get(pointcloud_topic) or not self.frame_samples.get(pointcloud2_topic):
                    self.on_processing_failed("No synchronized message pairs found for LiDAR topics.")
                    return

                first_pc1 = self.frame_samples[pointcloud_topic][0]['data']
                first_pc2 = self.frame_samples[pointcloud2_topic][0]['data']
                self.prepare_for_transform_view(topic_types, selected_topics_data, first_pc1, first_pc2)

        else:
            # Fallback for workers that return a single message set (flat dictionary).
            print("[DEBUG] Processing single message set (no 'frame_samples' key found).")
            if self.calibration_type == "LiDAR2LiDAR":
                pointcloud_topic = selected_topics_data["pointcloud_topic"]
                pointcloud2_topic = selected_topics_data["pointcloud2_topic"]

                pc1_msg = raw_messages.get(pointcloud_topic)
                pc2_msg = raw_messages.get(pointcloud2_topic)

                if not pc1_msg or not pc2_msg:
                    self.on_processing_failed("Synchronized LiDAR messages not found in bag.")
                    return
                self.prepare_for_transform_view(topic_types, selected_topics_data, pc1_msg, pc2_msg)
            else: # Fallback for LiDAR2Cam single frame
                image_topic = selected_topics_data["image_topic"]
                pointcloud_topic = selected_topics_data["pointcloud_topic"]
                camerainfo_topic = selected_topics_data["camerainfo_topic"]

                self.lidar_frame = self.extract_frame_id(raw_messages[pointcloud_topic])
                self.camera_frame = self.extract_frame_id(raw_messages[camerainfo_topic])

                self.selected_topics = {
                    "image_topic": image_topic,
                    "pointcloud_topic": pointcloud_topic,
                    "camerainfo_topic": camerainfo_topic,
                    "topic_types": topic_types,
                    "raw_messages": {topic: raw_messages.get(topic) for topic in [image_topic, pointcloud_topic, camerainfo_topic]},
                    "tf_messages": self.tf_messages,
                }
                self.tf_title_label.setText(f"Select Initial Transformation: {self.lidar_frame} → {self.camera_frame}")
                self.load_tf_topics_in_transform_view()
                self.stacked_widget.setCurrentIndex(2) # Switch to Transform View

        if hasattr(self, "processing_worker") and self.processing_worker:
            self.processing_worker.deleteLater()
            self.processing_worker = None

    def prepare_for_transform_view(self, topic_types, selected_topics_data, pc1_msg, pc2_msg):
        """Helper function to set up data structures and switch to the transform view for LiDAR2LiDAR."""
        pointcloud_topic = selected_topics_data["pointcloud_topic"]
        pointcloud2_topic = selected_topics_data["pointcloud2_topic"]

        self.lidar_frame = self.extract_frame_id(pc1_msg)
        self.lidar2_frame = self.extract_frame_id(pc2_msg)

        self.selected_topics = {
            "calibration_type": "LiDAR2LiDAR",
            "pointcloud_topic": pointcloud_topic,
            "pointcloud2_topic": pointcloud2_topic,
            "topic_types": topic_types,
            "raw_messages": {
                pointcloud_topic: pc1_msg,
                pointcloud2_topic: pc2_msg,
            },
            "tf_messages": self.tf_messages
        }
        self.tf_title_label.setText(f"Select Initial Transformation: {self.lidar_frame} → {self.lidar2_frame}")
        self.load_tf_topics_in_transform_view()
        self.stacked_widget.setCurrentIndex(2) # Switch to Transform View

    def on_frame_selected(self, frame_index: int):
        print(f"[DEBUG] Frame {frame_index + 1} selected for LiDAR2Cam calibration.")
        image_topic = self.selected_topics_data["image_topic"]
        pointcloud_topic = self.selected_topics_data["pointcloud_topic"]
        camerainfo_topic = self.selected_topics_data["camerainfo_topic"]
        self.selected_topics = {
            "image_topic": image_topic,
            "pointcloud_topic": pointcloud_topic,
            "camerainfo_topic": camerainfo_topic,
            "topic_types": self.topic_types,
            "raw_messages": {
                image_topic: self.frame_samples[image_topic][frame_index]["data"],
                pointcloud_topic: self.frame_samples[pointcloud_topic][frame_index]["data"],
                camerainfo_topic: self.frame_samples[camerainfo_topic][frame_index]["data"],
            },
            "tf_messages": self.tf_messages,
        }
        self.tf_title_label.setText(f"Select Initial Transformation: {self.lidar_frame} → {self.camera_frame}")
        self.load_tf_topics_in_transform_view()
        self.stacked_widget.setCurrentIndex(2)

    def on_processing_failed(self, error_message):
        print(f"[ERROR] Rosbag processing failed: {error_message}")
        self.progress_bar.setFormat(f"Error: {error_message}")
        self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: red; }")
        self.proceed_button.setEnabled(True)
        if hasattr(self, "processing_worker") and self.processing_worker:
            self.processing_worker.deleteLater()
            self.processing_worker = None

    def extract_frame_id(self, msg):
        return getattr(getattr(msg, "header", None), "frame_id", "unknown_frame")

    def proceed_to_calibration(self, initial_transform):
        for i in range(self.stacked_widget.count() - 1, 4, -1):
            widget = self.stacked_widget.widget(i)
            self.stacked_widget.removeWidget(widget)
            widget.deleteLater()

        if self.calibration_type == "LiDAR2LiDAR":
            self.proceed_to_lidar_calibration_with_transform(initial_transform)
            return

        image_msg = convert_to_mock(self.selected_topics["raw_messages"][self.selected_topics["image_topic"]], self.selected_topics["topic_types"][self.selected_topics["image_topic"]])
        pointcloud_msg = convert_to_mock(self.selected_topics["raw_messages"][self.selected_topics["pointcloud_topic"]], self.selected_topics["topic_types"][self.selected_topics["pointcloud_topic"]])
        camerainfo_msg = convert_to_mock(self.selected_topics["raw_messages"][self.selected_topics["camerainfo_topic"]], self.selected_topics["topic_types"][self.selected_topics["camerainfo_topic"]])
        self.calibration_widget = CalibrationWidget(image_msg, pointcloud_msg, camerainfo_msg, ros_utils, initial_transform)
        self.calibration_widget.calibration_completed.connect(self.show_calibration_results)
        self.stacked_widget.setCurrentIndex(self.stacked_widget.addWidget(self.calibration_widget))

    def proceed_to_lidar_calibration_with_transform(self, initial_transform):
        raw_msgs = self.selected_topics["raw_messages"]
        topic_types = self.selected_topics["topic_types"]
        pc_topic1 = self.selected_topics["pointcloud_topic"]
        pc_topic2 = self.selected_topics["pointcloud2_topic"]

        pointcloud_msg1 = convert_to_mock(raw_msgs[pc_topic1], topic_types[pc_topic1])
        pointcloud_msg2 = convert_to_mock(raw_msgs[pc_topic2], topic_types[pc_topic2])

        import threading
        threading.Thread(target=self._run_lidar_calibration_thread, args=(pointcloud_msg1, pointcloud_msg2, initial_transform), daemon=True).start()

    def _run_lidar_calibration_thread(self, pc1, pc2, initial_transform):
        try:
            launch_lidar2lidar_calibration(pc1, pc2, initial_transform, self._on_lidar_calibration_completed)
        except Exception as e:
            print(f"[ERROR] LiDAR calibration thread failed: {e}")

    def _on_lidar_calibration_completed(self, final_transform: np.ndarray):
        print("[DEBUG] LiDAR calibration completed via callback.")
        self.calibration_completed.emit(final_transform)

    def go_back_to_calibration(self):
        for i in range(self.stacked_widget.count()):
            if isinstance(self.stacked_widget.widget(i), CalibrationWidget):
                self.stacked_widget.setCurrentIndex(i)
                return

    def get_results_view_index(self):
        return 4

    def load_tf_topics_in_transform_view(self):
        self.tf_topic_combo.clear()
        tf_messages = self.selected_topics.get("tf_messages", self.tf_messages)
        if tf_messages:
            tf_topics = list(tf_messages.keys())
            self.tf_topic_combo.addItems(tf_topics)
            tf_static = next((t for t in tf_topics if "tf_static" in t), tf_topics[0] if tf_topics else None)
            if tf_static:
                self.tf_topic_combo.setCurrentText(tf_static)
                self.load_tf_tree_from_preloaded()
        else:
            self.tf_topic_combo.addItem("No TF topics found")
            self.load_tf_button.setEnabled(False)

    def on_tf_topic_changed(self):
        self.tf_tree = {}
        self.tf_info_text.setPlainText("Select 'Load TF Tree' to load transformations.")
        self.show_graph_button.setEnabled(False)

    def load_tf_tree(self):
        self.load_tf_tree_from_preloaded()

    def load_tf_tree_from_preloaded(self):
        topic_name = self.tf_topic_combo.currentText()
        tf_messages = self.selected_topics.get("tf_messages", self.tf_messages)
        if not topic_name or topic_name not in tf_messages: return

        self.tf_tree = self.parse_preloaded_tf_message(tf_messages[topic_name])
        self.update_tf_info_display()
        self.try_find_transform()
        self.show_graph_button.setEnabled(bool(self.tf_tree))


    def parse_preloaded_tf_message(self, msg_data) -> Dict[str, Dict]:
        tf_tree = {}
        for transform_stamped in self.deserialize_tf_message(msg_data).transforms:
            parent = transform_stamped.header.frame_id
            child = transform_stamped.child_frame_id
            tf_tree.setdefault(parent, {})[child] = {"transform": ros_utils.transform_to_numpy(transform_stamped.transform)}
        return tf_tree

    def deserialize_tf_message(self, msg_data) -> ros_utils.TFMessage:
        """
        Convert raw rosbag TF message data to our internal mock TFMessage format.
        This version is corrected to handle metadata fields from the rosbags library.
        """
        if not hasattr(msg_data, "transforms"):
            return ros_utils.TFMessage(transforms=[])

        transforms = []
        for tf in msg_data.transforms:
            # Get the translation and rotation objects
            translation_obj = tf.transform.translation
            rotation_obj = tf.transform.rotation

            # Instead of using **vars(), we explicitly access the x, y, z, w attributes.
            # This is safer and ignores any extra metadata like '__msgtype__'.
            new_transform = ros_utils.Transform(
                translation=ros_utils.Vector3(
                    x=translation_obj.x,
                    y=translation_obj.y,
                    z=translation_obj.z
                ),
                rotation=ros_utils.Quaternion(
                    x=rotation_obj.x,
                    y=rotation_obj.y,
                    z=rotation_obj.z,
                    w=rotation_obj.w
                ),
            )

            new_stamped = ros_utils.TransformStamped(
                header=ros_utils.Header(frame_id=tf.header.frame_id),
                child_frame_id=tf.child_frame_id,
                transform=new_transform
            )
            transforms.append(new_stamped)

        return ros_utils.TFMessage(transforms=transforms)

    def try_find_transform(self):
        if not self.tf_tree:
            return
        source = self.lidar_frame
        target = self.camera_frame if self.calibration_type == "LiDAR2Cam" else self.lidar2_frame
        if (transform_matrix := self.find_transform_path(source, target)) is not None:
            self.current_transform = transform_matrix
            self.update_transform_display()
            self.update_manual_inputs_from_matrix()

    def find_transform_path(self, from_frame: str, to_frame: str) -> Optional[np.ndarray]:
        if from_frame == to_frame: return np.eye(4)
        if not self.tf_tree: return None

        from collections import deque
        q = deque([(from_frame, np.eye(4))])
        visited = {from_frame}

        adj = {frame: [] for frame in self._get_all_tf_frames()}
        for p, children in self.tf_tree.items():
            for c, data in children.items():
                adj[p].append((c, data["transform"]))
                adj[c].append((p, np.linalg.inv(data["transform"])))

        while q:
            curr_frame, T = q.popleft()
            if curr_frame == to_frame: return T
            for neighbor, t in adj.get(curr_frame, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    q.append((neighbor, T @ t))
        return None

    def find_transformation_path_frames(self, from_frame: str, to_frame: str) -> Optional[List[str]]:
        if from_frame == to_frame:
            return [from_frame]
        if not self.tf_tree:
            return None

        from collections import deque
        q = deque([(from_frame, [from_frame])])
        visited = {from_frame}

        adj = {frame: [] for frame in self._get_all_tf_frames()}
        for p, children in self.tf_tree.items():
            for c in children:
                adj[p].append(c)
                adj[c].append(p)

        while q:
            curr_frame, path = q.popleft()
            if curr_frame == to_frame: return path
            for neighbor in adj.get(curr_frame, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    q.append((neighbor, path + [neighbor]))
        return None

    def update_tf_info_display(self):
        if not self.tf_tree:
            self.tf_info_text.setPlainText("No TF data available.")
            return
        source = self.lidar_frame
        target = self.camera_frame if self.calibration_type == "LiDAR2Cam" else self.lidar2_frame
        path_frames = self.find_transformation_path_frames(source, target)
        path_info = f"\n✓ Found transformation path: {' → '.join(path_frames)}" if path_frames else f"\n✗ No transformation found: {source} → {target}"
        self.tf_info_text.setPlainText(f"Available transformations: {sum(len(c) for c in self.tf_tree.values())}{path_info}")

    def show_tf_graph(self):
        if not self.tf_tree: return
        source = self.lidar_frame
        target = self.camera_frame if self.calibration_type == "LiDAR2Cam" else self.lidar2_frame
        path_frames = self.find_transformation_path_frames(source, target)
        self.tf_graph_window = TFGraphWidget(self.tf_tree, source, target, path_frames, parent=self)
        self.tf_graph_window.show()

    def update_manual_transform(self):
        try:
            from scipy.spatial.transform import Rotation
            rot = Rotation.from_euler('xyz', [float(self.rx_input.text()), float(self.ry_input.text()), float(self.rz_input.text())])
            self.current_transform = np.eye(4, dtype=np.float64)
            self.current_transform[:3, :3] = rot.as_matrix()
            self.current_transform[:3, 3] = [float(self.tx_input.text()), float(self.ty_input.text()), float(self.tz_input.text())]
            self.update_transform_display()
        except ValueError as e:
            print(f"[ERROR] Invalid manual transform input: {e}")

    def update_manual_inputs_from_matrix(self):
        trans = tf.translation_from_matrix(self.current_transform)
        euler = tf.euler_from_matrix(self.current_transform)
        self.tx_input.setText(f"{trans[0]:.6f}")
        self.ty_input.setText(f"{trans[1]:.6f}")
        self.tz_input.setText(f"{trans[2]:.6f}")
        self.rx_input.setText(f"{euler[0]:.6f}")
        self.ry_input.setText(f"{euler[1]:.6f}")
        self.rz_input.setText(f"{euler[2]:.6f}")

    def use_identity_transform(self):
        self.current_transform = np.eye(4, dtype=np.float64)
        self.update_transform_display()
        self.update_manual_inputs_from_matrix()

    def update_transform_display(self):
        matrix_str = "\n".join(["  ".join([f"{val:8.4f}" for val in row]) for row in self.current_transform])
        trans = tf.translation_from_matrix(self.current_transform)
        rpy = tf.euler_from_matrix(self.current_transform)

        combined_display = f"Transformation Matrix:\n{matrix_str}\n\nTranslation and Rotation:\nXYZ: [{trans[0]:.4f}, {trans[1]:.4f}, {trans[2]:.4f}]\nRPY: [{rpy[0]:.4f}, {rpy[1]:.4f}, {rpy[2]:.4f}]"
        self.transform_display.setPlainText(combined_display)

    def confirm_transformation(self):
        self.proceed_to_calibration(self.current_transform)

    def show_calibration_results(self, calibration_results):
        print("[DEBUG] show_calibration_results called on main thread.")
        print(f"[DEBUG] Received calibration_results: {calibration_results}")
        print(f"[DEBUG] Results type: {type(calibration_results)}")

        # Handle both old format (direct numpy array) and new format (dictionary)
        if isinstance(calibration_results, dict):
            calibrated_transform = calibration_results['master_to_camera']
        elif isinstance(calibration_results, np.ndarray):
            calibrated_transform = calibration_results
        else:
            print(f"[ERROR] Unexpected calibration result type: {type(calibration_results)}")
            return

        self.calibrated_transform = calibrated_transform
        if self.calibration_type == "LiDAR2Cam":
            self.calibrated_transform = np.linalg.inv(calibrated_transform)
            self.original_source_frame = self.lidar_frame
            self.original_target_frame = self.camera_frame
        else:
            self.original_source_frame = self.lidar_frame
            self.original_target_frame = self.lidar2_frame
        self.populate_results_view()
        self.stacked_widget.setCurrentIndex(self.get_results_view_index())

    def _get_all_tf_frames(self) -> List[str]:
        frames = set(self.tf_tree.keys())
        for children in self.tf_tree.values():
            frames.update(children.keys())
        return sorted(list(frames))

    def populate_results_view(self):
        self.display_transform_urdf(self.calibration_result_display, self.calibrated_transform, self.original_source_frame, self.original_target_frame)
        all_frames = self._get_all_tf_frames()
        self.source_frame_combo.blockSignals(True)
        self.target_frame_combo.blockSignals(True)
        self.source_frame_combo.clear()
        self.source_frame_combo.addItems(all_frames)
        self.target_frame_combo.clear()
        self.target_frame_combo.addItems(all_frames)
        self.source_frame_combo.setCurrentText(self.original_source_frame)
        self.target_frame_combo.setCurrentText(self.original_target_frame)
        self.source_frame_combo.blockSignals(False)
        self.target_frame_combo.blockSignals(False)
        self.update_target_transform()

    def update_target_transform(self):
        new_source = self.source_frame_combo.currentText()
        new_target = self.target_frame_combo.currentText()
        if not new_source or not new_target:
            return
        path_frames = self.find_transformation_path_frames(new_source, new_target)
        self.chain_display.setPlainText(" → ".join(path_frames) if path_frames else f"No static path found between {new_source} and {new_target}.")
        self.update_embedded_graph(new_source, new_target)

        T_orig_src_to_new_src = self.find_transform_path(self.original_source_frame, new_source)
        T_orig_tgt_to_new_tgt = self.find_transform_path(self.original_target_frame, new_target)

        if T_orig_src_to_new_src is None or T_orig_tgt_to_new_tgt is None:
            self.final_transform_display.setPlainText("Error: Cannot find path from original frames in TF tree.")
            return

        T_new_src_to_orig_src = np.linalg.inv(T_orig_src_to_new_src)
        final_transform = T_new_src_to_orig_src @ self.calibrated_transform @ T_orig_tgt_to_new_tgt
        self.display_transform_urdf(self.final_transform_display, final_transform, new_source, new_target)
        self.current_final_transform = final_transform

    def display_transform_urdf(self, text_widget: QTextEdit, transform: np.ndarray, parent: str, child: str):
        from . import tf_transformations as tf
        if parent == child:
            text_widget.setPlainText("Source and target frames cannot be the same.")
            return
        matrix_str = "\n".join(["  ".join([f"{val:8.4f}" for val in row]) for row in transform])
        xyz = tf.translation_from_matrix(transform)
        rpy = tf.euler_from_matrix(transform)
        joint_name = f"joint_{parent.replace('/', '_')}_to_{child.replace('/', '_')}"
        urdf = f'<joint name="{joint_name}" type="fixed">\n  <parent link="{parent}" />\n  <child link="{child}" />\n  <origin xyz="{" ".join(map(str, xyz))}" rpy="{" ".join(map(str, rpy))}" />\n</joint>'
        text_widget.setPlainText(f"Transform: {parent} → {child}\nMatrix:\n{matrix_str}\n\nURDF Snippet:\n{urdf}")

    def update_embedded_graph(self, source_frame: str, target_frame: str):
        self.init_graph_placeholder()
        if not self.tf_tree:
            return
        try:
            path_frames = self.find_transformation_path_frames(source_frame, target_frame)
            graph_widget = TFGraphWidget(self.tf_tree, source_frame, target_frame, path_frames)
            self.graph_container.layout().addWidget(graph_widget)
        except Exception as e:
            print(f"[ERROR] Failed to create or embed TF graph: {e}")

    def export_calibration_result(self):
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Target Transform", "", "Text Files (*.txt);;All Files (*)")
        if file_path:
            with open(file_path, "w") as f:
                f.write(self.final_transform_display.toPlainText())

    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls(): 
            event.acceptProposedAction()

    def dropEvent(self, event: QDropEvent):
        if event.mimeData().hasUrls():
            self.process_dropped_path(event.mimeData().urls()[0].toLocalFile())
            event.acceptProposedAction()
