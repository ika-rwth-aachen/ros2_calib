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
from PySide6.QtCore import Qt
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
    QSizePolicy,
    QSpinBox,
    QStackedWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from . import ros_utils
from .bag_handler import (
    RosbagProcessingWorker,
    convert_to_mock,
    get_topic_info,
    get_total_message_count,
)
from .calibration_widget import CalibrationWidget
from .common import UIStyles
from .frame_selection_widget import FrameSelectionWidget


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ros2_calib - LiDAR Camera Calibration")
        self.setGeometry(100, 100, 1800, 800)
        self.setAcceptDrops(True)

        # Initialize data containers
        self.topics = {}
        self.bag_file = None
        self.selected_topics = {}
        self.tf_tree = {}
        self.current_transform = np.eye(4)

        # Set up stacked widget for multiple views
        self.stacked_widget = QStackedWidget()
        self.setCentralWidget(self.stacked_widget)

        # Create and add the different views
        self.setup_load_view()
        self.setup_transform_view()
        self.setup_frame_selection_view()
        self.setup_results_view()

        # Start with the load view
        self.stacked_widget.setCurrentIndex(0)

    def setup_load_view(self):
        self.load_widget = QWidget()
        self.load_layout = QVBoxLayout(self.load_widget)

        # Top section for loading
        load_section_layout = QHBoxLayout()
        self.load_layout.addLayout(load_section_layout)

        self.load_bag_button = QPushButton("Load Rosbag")
        self.load_bag_button.clicked.connect(self.load_bag)
        load_section_layout.addWidget(self.load_bag_button)

        # Subtle drag & drop indicator
        drag_drop_label = QLabel("or Drag & Drop")
        drag_drop_label.setStyleSheet(
            "color: #666; font-size: 11px; font-style: italic; padding: 5px;"
        )
        load_section_layout.addWidget(drag_drop_label)

        self.bag_path_label = QLabel("No rosbag loaded.")
        self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid gray; color: white;")
        load_section_layout.addWidget(self.bag_path_label)

        # Topic list at the top for better visibility of long topic names
        topic_list_group = QGroupBox("Available Topics")
        topic_list_group.setMinimumHeight(600)  # Ensure it has a minimum height
        topic_list_layout = QVBoxLayout(topic_list_group)

        self.topic_list_widget = QListWidget()
        topic_list_layout.addWidget(self.topic_list_widget)

        self.load_layout.addWidget(topic_list_group)

        # Calibration topic selection below, with full width for combo boxes
        selection_group = QGroupBox("Topic Selection for Calibration")
        calib_topic_layout = QGridLayout(selection_group)

        # Image topic selection
        calib_topic_layout.addWidget(QLabel("Image Topic:"), 0, 0)
        self.image_topic_combo = QComboBox()
        self.image_topic_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.image_topic_combo.currentIndexChanged.connect(self.auto_select_camera_info)
        calib_topic_layout.addWidget(self.image_topic_combo, 0, 1, 1, 2)  # Span 2 columns

        # CameraInfo topic selection
        calib_topic_layout.addWidget(QLabel("CameraInfo Topic:"), 1, 0)
        self.camerainfo_topic_combo = QComboBox()
        self.camerainfo_topic_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        calib_topic_layout.addWidget(self.camerainfo_topic_combo, 1, 1, 1, 2)  # Span 2 columns

        # PointCloud2 topic selection
        calib_topic_layout.addWidget(QLabel("PointCloud2 Topic:"), 2, 0)
        self.pointcloud_topic_combo = QComboBox()
        self.pointcloud_topic_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        calib_topic_layout.addWidget(self.pointcloud_topic_combo, 2, 1, 1, 2)  # Span 2 columns

        # Frame count selection
        calib_topic_layout.addWidget(QLabel("Frame Samples:"), 3, 0)
        self.frame_count_spinbox = QSpinBox()
        self.frame_count_spinbox.setMinimum(3)
        self.frame_count_spinbox.setMaximum(10)
        self.frame_count_spinbox.setValue(6)
        self.frame_count_spinbox.setSuffix(" frames")
        self.frame_count_spinbox.setToolTip(
            "Number of uniformly sampled frames to choose from during calibration"
        )
        calib_topic_layout.addWidget(self.frame_count_spinbox, 3, 1, 1, 2)  # Span 2 columns

        # Proceed button
        self.proceed_button = QPushButton("Proceed to Transform Selection")
        self.proceed_button.setEnabled(False)
        self.proceed_button.clicked.connect(self.proceed_to_transform_selection)
        self.proceed_button.setStyleSheet("font-weight: bold; padding: 10px;")
        calib_topic_layout.addWidget(self.proceed_button, 4, 0, 1, 3)  # Span all columns

        # Progress bar for rosbag reading
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)  # Initially hidden
        self.progress_bar.setTextVisible(True)
        calib_topic_layout.addWidget(self.progress_bar, 4, 0, 1, 3)  # Span all columns

        self.load_layout.addWidget(selection_group)

        # Add stretch to push everything to the top
        self.load_layout.addStretch()

        # Add the load view to the stacked widget
        self.stacked_widget.addWidget(self.load_widget)

    def setup_transform_view(self):
        self.transform_widget = QWidget()
        self.transform_layout = QVBoxLayout(self.transform_widget)

        # Title
        self.tf_title_label = QLabel()
        self.tf_title_label.setStyleSheet("font-size: 14px; font-weight: bold; margin: 10px;")
        self.transform_layout.addWidget(self.tf_title_label)

        # Back button
        back_button_layout = QHBoxLayout()
        self.back_button = QPushButton("← Back to Topic Selection")
        self.back_button.clicked.connect(self.go_back_to_load_view)
        back_button_layout.addWidget(self.back_button)
        back_button_layout.addStretch()
        self.transform_layout.addLayout(back_button_layout)

        # TF Source Selection
        tf_group = QGroupBox("Transform Source")
        tf_layout = QVBoxLayout(tf_group)

        # Dropdown for tf topics
        tf_topic_layout = QHBoxLayout()
        tf_topic_layout.addWidget(QLabel("TF Topic:"))
        self.tf_topic_combo = QComboBox()
        self.tf_topic_combo.currentTextChanged.connect(self.on_tf_topic_changed)
        tf_topic_layout.addWidget(self.tf_topic_combo)

        self.load_tf_button = QPushButton("Load TF Tree")
        self.load_tf_button.clicked.connect(self.load_tf_tree)
        tf_topic_layout.addWidget(self.load_tf_button)

        tf_layout.addLayout(tf_topic_layout)

        # TF Tree visualization placeholder
        self.show_graph_button = QPushButton("Show TF Tree Graph")
        self.show_graph_button.clicked.connect(self.show_tf_graph)
        self.show_graph_button.setEnabled(False)
        tf_layout.addWidget(self.show_graph_button)

        # TF info display
        self.tf_info_text = QTextEdit()
        self.tf_info_text.setMaximumHeight(150)
        self.tf_info_text.setPlainText("No TF data loaded.")
        tf_layout.addWidget(QLabel("TF Tree Information:"))
        tf_layout.addWidget(self.tf_info_text)

        self.transform_layout.addWidget(tf_group)

        # Manual Transform Input
        manual_group = QGroupBox("Manual Transform Input")
        manual_layout = QGridLayout(manual_group)

        # Translation inputs
        manual_layout.addWidget(QLabel("Translation (x, y, z):"), 0, 0)
        self.tx_input = QLineEdit("0.0")
        self.ty_input = QLineEdit("0.0")
        self.tz_input = QLineEdit("0.0")
        manual_layout.addWidget(self.tx_input, 0, 1)
        manual_layout.addWidget(self.ty_input, 0, 2)
        manual_layout.addWidget(self.tz_input, 0, 3)

        # Rotation inputs (Euler angles)
        manual_layout.addWidget(QLabel("Rotation (roll, pitch, yaw):"), 1, 0)
        self.rx_input = QLineEdit("0.0")
        self.ry_input = QLineEdit("0.0")
        self.rz_input = QLineEdit("0.0")
        manual_layout.addWidget(self.rx_input, 1, 1)
        manual_layout.addWidget(self.ry_input, 1, 2)
        manual_layout.addWidget(self.rz_input, 1, 3)

        # Update transform button
        self.update_manual_button = QPushButton("Update Transform from Manual Input")
        self.update_manual_button.clicked.connect(self.update_manual_transform)
        manual_layout.addWidget(self.update_manual_button, 2, 0, 1, 4)

        self.transform_layout.addWidget(manual_group)

        # Current Transform Display
        transform_group = QGroupBox("Current Transformation Matrix")
        transform_layout = QVBoxLayout(transform_group)

        self.transform_display = QTextEdit()
        self.transform_display.setMaximumHeight(120)
        self.transform_display.setFont("monospace")
        transform_layout.addWidget(self.transform_display)

        # Translation and Rotation display
        self.translation_rotation_display = QTextEdit()
        self.translation_rotation_display.setMaximumHeight(60)
        self.translation_rotation_display.setFont("monospace")
        transform_layout.addWidget(self.translation_rotation_display)

        self.transform_layout.addWidget(transform_group)

        # Action buttons
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

        # Initialize display
        self.update_transform_display()

        # Add the transform view to the stacked widget
        self.stacked_widget.addWidget(self.transform_widget)

    def setup_frame_selection_view(self):
        """Setup the frame selection view."""
        self.frame_selection_widget = FrameSelectionWidget(self)
        self.frame_selection_widget.frame_selected.connect(self.on_frame_selected)

        # Add to stacked widget
        self.stacked_widget.addWidget(self.frame_selection_widget)

    def setup_results_view(self):
        """Setup the calibration results view with TF graph and URDF integration."""
        self.results_widget = QWidget()
        self.results_layout = QVBoxLayout(self.results_widget)

        # Title
        self.results_title_label = QLabel("Calibration Results")
        self.results_title_label.setStyleSheet("font-size: 16px; font-weight: bold; margin: 10px;")
        self.results_layout.addWidget(self.results_title_label)

        # Back button
        back_layout = QHBoxLayout()
        self.results_back_button = QPushButton("← Back to Calibration")
        self.results_back_button.clicked.connect(self.go_back_to_calibration)
        back_layout.addWidget(self.results_back_button)
        back_layout.addStretch()
        self.results_layout.addLayout(back_layout)

        # Single column layout - Frame selection at top
        frame_selection_widget = QWidget()
        frame_selection_layout = QFormLayout(frame_selection_widget)

        # Source frame
        self.source_frame_label = QLabel()
        frame_selection_layout.addRow("Source Frame:", self.source_frame_label)

        # Target frame
        self.target_frame_combo = QComboBox()
        self.target_frame_combo.currentTextChanged.connect(self.update_transform_chain)
        frame_selection_layout.addRow("Target Frame:", self.target_frame_combo)

        self.results_layout.addWidget(frame_selection_widget)

        # Transform chain display
        chain_label = QLabel("Transformation Chain:")
        chain_label.setStyleSheet("font-weight: bold; margin-top: 10px;")
        self.results_layout.addWidget(chain_label)
        self.chain_display = QTextEdit()
        self.chain_display.setMaximumHeight(60)
        self.results_layout.addWidget(self.chain_display)

        # Embedded TF graph visualization
        self.graph_container = QWidget()
        self.graph_container.setMinimumHeight(200)
        self.graph_container.setMaximumHeight(300)

        # Initialize with placeholder
        self.init_graph_placeholder()

        self.results_layout.addWidget(self.graph_container)

        # Two column layout for results
        results_content_layout = QHBoxLayout()

        # Left column: Calibration Results
        left_group = QGroupBox("Calibration Results")
        left_layout = QVBoxLayout(left_group)

        self.calibration_result_display = QTextEdit()
        self.calibration_result_display.setMaximumHeight(400)
        self.calibration_result_display.setFont("monospace")
        left_layout.addWidget(self.calibration_result_display)

        results_content_layout.addWidget(left_group, 1)

        # Right column: Target Transform
        right_group = QGroupBox("Target Transform")
        right_layout = QVBoxLayout(right_group)

        self.final_transform_display = QTextEdit()
        self.final_transform_display.setMaximumHeight(400)
        self.final_transform_display.setFont("monospace")
        right_layout.addWidget(self.final_transform_display)

        results_content_layout.addWidget(right_group, 1)

        self.results_layout.addLayout(results_content_layout)

        # Export button centered below the two columns
        export_layout = QHBoxLayout()
        export_layout.addStretch()
        self.export_calibration_button = QPushButton("Export Calibration")
        self.export_calibration_button.clicked.connect(self.export_calibration_result)
        export_layout.addWidget(self.export_calibration_button)
        export_layout.addStretch()

        self.results_layout.addLayout(export_layout)

        # Add the results view to the stacked widget
        self.stacked_widget.addWidget(self.results_widget)

    def init_graph_placeholder(self):
        """Initialize the graph container with a placeholder."""
        layout = QVBoxLayout(self.graph_container)
        placeholder = QLabel("TF Graph will appear here when data is available")
        placeholder.setAlignment(Qt.AlignCenter)
        placeholder.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(placeholder)

    def load_bag(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open Rosbag", "", "MCAP Rosbag (*.mcap)")
        if file_path:
            self.load_bag_from_path(file_path)

    def find_yaml_file(self, mcap_path):
        """Find the corresponding YAML file for an MCAP file.

        Looks for either:
        1. metadata.yaml in the same directory
        2. A YAML file with the same base name as the MCAP file

        Returns the path to the YAML file if found, None otherwise.
        """
        directory = os.path.dirname(mcap_path)
        mcap_basename = os.path.basename(mcap_path).replace(".mcap", "")

        # Check for metadata.yaml first
        metadata_yaml = os.path.join(directory, "metadata.yaml")
        if os.path.exists(metadata_yaml):
            return metadata_yaml

        # Check for matching filename yaml
        matching_yaml = os.path.join(directory, f"{mcap_basename}.yaml")
        if os.path.exists(matching_yaml):
            return matching_yaml

        return None

    def process_dropped_path(self, path):
        """Process a dropped file or folder path."""
        if os.path.isfile(path):
            # Direct .mcap file dropped
            if path.endswith(".mcap"):
                # Check if corresponding .yaml file exists
                yaml_path = self.find_yaml_file(path)
                if yaml_path:
                    self.load_bag_from_path(path)
                else:
                    directory = os.path.dirname(path)
                    mcap_basename = os.path.basename(path).replace(".mcap", "")
                    self.bag_path_label.setText(
                        f"Error: No YAML file found! Expected 'metadata.yaml' or '{mcap_basename}.yaml' in {os.path.basename(directory)}/"
                    )
                    self.bag_path_label.setStyleSheet(
                        "padding: 5px; border: 1px solid red; color: red;"
                    )
            else:
                self.bag_path_label.setText("Error: Only .mcap files are supported!")
                self.bag_path_label.setStyleSheet(
                    "padding: 5px; border: 1px solid red; color: red;"
                )
        elif os.path.isdir(path):
            # Folder dropped - look for .mcap files
            mcap_files = [f for f in os.listdir(path) if f.endswith(".mcap")]
            if len(mcap_files) == 1:
                mcap_path = os.path.join(path, mcap_files[0])
                yaml_path = self.find_yaml_file(mcap_path)
                if yaml_path:
                    self.load_bag_from_path(mcap_path)
                else:
                    mcap_basename = mcap_files[0].replace(".mcap", "")
                    self.bag_path_label.setText(
                        f"Error: No YAML file found! Expected 'metadata.yaml' or '{mcap_basename}.yaml' in folder!"
                    )
                    self.bag_path_label.setStyleSheet(
                        "padding: 5px; border: 1px solid red; color: red;"
                    )
            elif len(mcap_files) == 0:
                self.bag_path_label.setText("Error: No .mcap file found in folder!")
                self.bag_path_label.setStyleSheet(
                    "padding: 5px; border: 1px solid red; color: red;"
                )
            else:
                self.bag_path_label.setText(
                    "Error: Multiple .mcap files found! Please select one."
                )
                self.bag_path_label.setStyleSheet(
                    "padding: 5px; border: 1px solid red; color: red;"
                )
        else:
            self.bag_path_label.setText("Error: Invalid path!")
            self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid red; color: red;")

    def load_bag_from_path(self, file_path):
        """Load bag from a specific file path."""
        try:
            self.bag_file = file_path
            self.bag_path_label.setText(file_path)
            self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid gray; color: white;")
            self.topics = get_topic_info(file_path)
            self.update_topic_widgets()
        except Exception as e:
            self.bag_path_label.setText(f"Error loading bag: {str(e)}")
            self.bag_path_label.setStyleSheet("padding: 5px; border: 1px solid red; color: red;")

    def update_topic_widgets(self):
        self.topic_list_widget.clear()
        self.image_topic_combo.clear()
        self.pointcloud_topic_combo.clear()
        self.camerainfo_topic_combo.clear()

        image_topics = []
        pointcloud_topics = []
        camerainfo_topics = []

        for topic, msgtype, msgcount in self.topics:
            self.topic_list_widget.addItem(f"{topic} ({msgtype}) - {msgcount} messages")
            if msgtype in ["sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage"]:
                image_topics.append(topic)
            elif msgtype == "sensor_msgs/msg/PointCloud2":
                pointcloud_topics.append(topic)
            elif msgtype == "sensor_msgs/msg/CameraInfo":
                camerainfo_topics.append(topic)

        self.image_topic_combo.addItems(image_topics)
        self.pointcloud_topic_combo.addItems(pointcloud_topics)
        self.camerainfo_topic_combo.addItems(camerainfo_topics)

        if image_topics and pointcloud_topics and camerainfo_topics:
            self.proceed_button.setEnabled(True)

    def auto_select_camera_info(self, index):
        if index == -1:  # No item selected
            return

        image_topic = self.image_topic_combo.currentText()

        # --- New, more robust logic ---
        parts = image_topic.split("/")
        base_path = None
        # Find the part of the topic name that contains 'image' and construct the base path.
        for i, part in enumerate(parts):
            if "image" in part:
                base_path = "/".join(parts[:i])
                break

        possible_info_topics = []
        if base_path:
            # Handle cases like /camera/image_raw -> /camera/camera_info
            possible_info_topics.append(f"{base_path}/camera_info")

        # --- Fallback to old logic ---
        possible_info_topics.extend(
            [
                image_topic.replace("/image_raw", "/camera_info"),
                image_topic.replace("/image_color", "/camera_info"),
                image_topic.replace("/image_rect", "/camera_info"),
                image_topic.replace("/image_rect_color", "/camera_info"),
                # Handle compressed topics
                image_topic.replace("/compressed", "").replace("/image_rect_color", "/camera_info"),
                image_topic.replace("/compressed", "").replace("/image_raw", "/camera_info"),
                image_topic.replace("/compressed", "").replace("/image_color", "/camera_info"),
            ]
        )

        # Remove duplicates while preserving order
        possible_info_topics = list(dict.fromkeys(possible_info_topics))

        for topic in possible_info_topics:
            found_index = self.camerainfo_topic_combo.findText(topic, Qt.MatchExactly)
            if found_index != -1:
                self.camerainfo_topic_combo.setCurrentIndex(found_index)
                return

    def proceed_to_transform_selection(self):
        """Proceed to transformation selection window with optimized message reading."""
        # Show progress bar and disable proceed button
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Preparing to read rosbag...")
        self.proceed_button.setEnabled(False)

        # Force UI update
        self.repaint()

        image_topic = self.image_topic_combo.currentText()
        pointcloud_topic = self.pointcloud_topic_combo.currentText()
        camerainfo_topic = self.camerainfo_topic_combo.currentText()

        # Create topic type lookup dictionary
        self.progress_bar.setValue(10)
        self.progress_bar.setFormat("Analyzing topics...")
        self.repaint()

        topic_types = {}
        for topic, msgtype, _ in self.topics:
            topic_types[topic] = msgtype

        # Find available TF topics
        tf_topics = []
        for topic, msgtype, _ in self.topics:
            if "tf" in topic.lower() and "TFMessage" in msgtype:
                tf_topics.append(topic)

        print(f"[DEBUG] Found TF topics: {tf_topics}")

        # Read all messages in one pass for efficiency
        topics_to_read = {
            image_topic: topic_types[image_topic],
            pointcloud_topic: topic_types[pointcloud_topic],
            camerainfo_topic: topic_types[camerainfo_topic],
        }

        # Add TF topics to the read list
        for tf_topic in tf_topics:
            topics_to_read[tf_topic] = topic_types[tf_topic]

        self.progress_bar.setValue(20)
        self.progress_bar.setFormat(f"Reading {len(topics_to_read)} topics from rosbag...")
        self.repaint()

        print(f"[DEBUG] Reading all messages in single pass: {list(topics_to_read.keys())}")

        # Prepare selected topics data
        selected_topics_data = {
            "image_topic": image_topic,
            "pointcloud_topic": pointcloud_topic,
            "camerainfo_topic": camerainfo_topic,
            "tf_topics": tf_topics,
        }

        print(f"[DEBUG] Starting threaded processing of {len(topics_to_read)} topics")

        # Get total message count for progress tracking
        total_messages = get_total_message_count(self.bag_file)
        print(f"[DEBUG] Total messages in bag: {total_messages}")

        # Get frame count from UI
        frame_samples = self.frame_count_spinbox.value()

        # Create topic message counts dictionary from self.topics
        topic_message_counts = {}
        for topic_name, msg_type, msg_count in self.topics:
            topic_message_counts[topic_name] = msg_count

        # Create and start worker thread
        self.processing_worker = RosbagProcessingWorker(
            self.bag_file,
            topics_to_read,
            selected_topics_data,
            total_messages,
            frame_samples,
            topic_message_counts,
        )
        self.processing_worker.progress_updated.connect(self.update_processing_progress)
        self.processing_worker.processing_finished.connect(self.on_processing_finished)
        self.processing_worker.processing_failed.connect(self.on_processing_failed)
        self.processing_worker.start()

    def update_processing_progress(self, value, message):
        """Update progress bar from worker thread."""
        self.progress_bar.setValue(value)
        self.progress_bar.setFormat(message)

    def on_processing_finished(self, raw_messages, topic_types, selected_topics_data):
        """Handle completion of rosbag processing."""
        print("[DEBUG] Rosbag processing completed successfully")

        # Check if we have frame samples (multiple frames)
        has_frame_samples = "frame_samples" in raw_messages and self.frame_count_spinbox.value() > 1

        if has_frame_samples:
            # Store all frame samples for selection
            self.frame_samples = raw_messages["frame_samples"]
            self.selected_topics_data = selected_topics_data
            self.topic_types = topic_types

            # Extract frame info from first sample for display
            image_topic = selected_topics_data["image_topic"]
            pointcloud_topic = selected_topics_data["pointcloud_topic"]
            camerainfo_topic = selected_topics_data["camerainfo_topic"]

            # Get first sample to extract frame IDs
            first_image = (
                self.frame_samples.get(image_topic, [{}])[0].get("data")
                if self.frame_samples.get(image_topic)
                else None
            )
            first_pointcloud = (
                self.frame_samples.get(pointcloud_topic, [{}])[0].get("data")
                if self.frame_samples.get(pointcloud_topic)
                else None
            )
            first_camerainfo = (
                self.frame_samples.get(camerainfo_topic, [{}])[0].get("data")
                if self.frame_samples.get(camerainfo_topic)
                else None
            )

            if first_pointcloud:
                self.lidar_frame = self.extract_frame_id(first_pointcloud)
            if first_camerainfo:
                self.camera_frame = self.extract_frame_id(first_camerainfo)

            # Store tf messages
            self.tf_messages = {
                topic: raw_messages.get(topic)
                for topic in selected_topics_data["tf_topics"]
                if topic in raw_messages
            }

            # Go to frame selection view (index 2)
            self.frame_selection_widget.set_frame_samples(self.frame_samples, image_topic)
            self.stacked_widget.setCurrentIndex(2)

        else:
            # Original single frame logic
            # Store processed data
            self.selected_topics = {
                "image_topic": selected_topics_data["image_topic"],
                "pointcloud_topic": selected_topics_data["pointcloud_topic"],
                "camerainfo_topic": selected_topics_data["camerainfo_topic"],
                "topic_types": topic_types,
                "raw_messages": raw_messages,
                "tf_messages": {
                    topic: raw_messages.get(topic)
                    for topic in selected_topics_data["tf_topics"]
                    if topic in raw_messages
                },
            }

            # Extract frame IDs for transformation lookup
            lidar_frame = self.extract_frame_id(
                raw_messages[selected_topics_data["pointcloud_topic"]]
            )
            camera_frame = self.extract_frame_id(
                raw_messages[selected_topics_data["camerainfo_topic"]]
            )

            # Store frame information and switch to transform view
            self.lidar_frame = lidar_frame
            self.camera_frame = camera_frame

            # Update title in transform view
            self.tf_title_label.setText(
                f"Select Initial Transformation: {self.lidar_frame} → {self.camera_frame}"
            )

            # Load TF topics in the transform view
            self.load_tf_topics_in_transform_view()

            # Switch to transform view (index 1)
            self.stacked_widget.setCurrentIndex(1)

        # Hide progress bar and re-enable button
        self.progress_bar.setVisible(False)
        self.proceed_button.setEnabled(True)

        # Clean up worker
        self.processing_worker.deleteLater()
        self.processing_worker = None

    def on_frame_selected(self, frame_index):
        """Handle frame selection and proceed to transform view."""
        print(f"[DEBUG] Frame {frame_index + 1} selected")

        # Get the selected frame data
        image_topic = self.selected_topics_data["image_topic"]
        pointcloud_topic = self.selected_topics_data["pointcloud_topic"]
        camerainfo_topic = self.selected_topics_data["camerainfo_topic"]

        # Extract selected frame data
        selected_image = self.frame_samples[image_topic][frame_index]["data"]
        selected_pointcloud = self.frame_samples[pointcloud_topic][frame_index]["data"]
        selected_camerainfo = self.frame_samples[camerainfo_topic][frame_index]["data"]

        # Create the selected_topics structure as if single frame was processed
        self.selected_topics = {
            "image_topic": image_topic,
            "pointcloud_topic": pointcloud_topic,
            "camerainfo_topic": camerainfo_topic,
            "topic_types": self.topic_types,
            "raw_messages": {
                image_topic: selected_image,
                pointcloud_topic: selected_pointcloud,
                camerainfo_topic: selected_camerainfo,
            },
            "tf_messages": self.tf_messages,
        }

        # Update title in transform view
        self.tf_title_label.setText(
            f"Select Initial Transformation: {self.lidar_frame} → {self.camera_frame}"
        )

        # Load TF topics in the transform view
        self.load_tf_topics_in_transform_view()

        # Switch to transform view (index 1)
        self.stacked_widget.setCurrentIndex(1)

    def on_processing_failed(self, error_message):
        """Handle rosbag processing failure."""
        print(f"[ERROR] Rosbag processing failed: {error_message}")

        # Show error and reset UI
        self.progress_bar.setFormat(f"Error: {error_message}")
        self.progress_bar.setVisible(True)  # Keep visible to show error
        self.proceed_button.setEnabled(True)

        # Clean up worker
        if hasattr(self, "processing_worker") and self.processing_worker:
            self.processing_worker.deleteLater()
            self.processing_worker = None

    def extract_frame_id(self, msg):
        """Extract frame_id from message header."""
        if hasattr(msg, "header") and hasattr(msg.header, "frame_id"):
            return msg.header.frame_id
        return "unknown_frame"

    def proceed_to_calibration(self, initial_transform):
        """Proceed to calibration with selected transformation."""
        # Close transformation widget
        if hasattr(self, "transform_widget"):
            self.transform_widget.close()

        # Convert messages to mock objects
        image_topic = self.selected_topics["image_topic"]
        pointcloud_topic = self.selected_topics["pointcloud_topic"]
        camerainfo_topic = self.selected_topics["camerainfo_topic"]
        topic_types = self.selected_topics["topic_types"]
        raw_messages = self.selected_topics["raw_messages"]

        image_msg = convert_to_mock(raw_messages[image_topic], topic_types[image_topic])
        pointcloud_msg = convert_to_mock(
            raw_messages[pointcloud_topic], topic_types[pointcloud_topic]
        )
        camerainfo_msg = convert_to_mock(
            raw_messages[camerainfo_topic], topic_types[camerainfo_topic]
        )

        # Create calibration widget with initial transformation
        self.calibration_widget = CalibrationWidget(
            image_msg, pointcloud_msg, camerainfo_msg, ros_utils, initial_transform
        )

        # Connect the calibration completion signal
        self.calibration_widget.calibration_completed.connect(self.show_calibration_results)

        # Add calibration widget to stacked widget and switch to it
        self.stacked_widget.addWidget(
            self.calibration_widget
        )  # This will be index 4 (after frame selection was added)
        calibration_index = (
            self.stacked_widget.count() - 1
        )  # Get the actual index of the calibration widget
        self.stacked_widget.setCurrentIndex(calibration_index)  # Switch to calibration view
        print(f"[DEBUG] Switched to calibration view at index {calibration_index}")

    # Transformation View Methods

    def go_back_to_load_view(self):
        """Go back to the load view."""
        self.stacked_widget.setCurrentIndex(0)

    def go_back_to_calibration(self):
        """Go back to the calibration view."""
        # Find the calibration widget in the stacked widget
        for i in range(self.stacked_widget.count()):
            widget = self.stacked_widget.widget(i)
            if hasattr(widget, "calibration_completed"):  # CalibrationWidget has this signal
                self.stacked_widget.setCurrentIndex(i)
                print(f"[DEBUG] Switched back to calibration view at index {i}")
                return
        print("[ERROR] Could not find calibration widget to go back to")

    def get_results_view_index(self):
        """Get the index of the results view in the stacked widget."""
        # The results view should be the one with results_layout
        for i in range(self.stacked_widget.count()):
            widget = self.stacked_widget.widget(i)
            if widget == self.results_widget:
                return i
        print("[ERROR] Could not find results widget")
        return 3  # Fallback to expected index

    def load_tf_topics_in_transform_view(self):
        """Load TF topics from preloaded data into transform view."""
        self.tf_topic_combo.clear()

        if self.selected_topics.get("tf_messages"):
            tf_topics = list(self.selected_topics["tf_messages"].keys())
            self.tf_topic_combo.addItems(tf_topics)

            if tf_topics:
                print(f"[DEBUG] Loaded TF topics in transform view: {tf_topics}")
                # Automatically process the first tf_static topic if available
                tf_static_topics = [topic for topic in tf_topics if "tf_static" in topic]
                if tf_static_topics:
                    self.tf_topic_combo.setCurrentText(tf_static_topics[0])
                    # Process immediately since we already have the data
                    self.load_tf_tree_from_preloaded()
                else:
                    self.tf_topic_combo.setCurrentIndex(0)
            else:
                self.tf_topic_combo.addItem("No TF topics found")
                self.load_tf_button.setEnabled(False)
        else:
            self.tf_topic_combo.addItem("No TF topics found")
            self.load_tf_button.setEnabled(False)

    def on_tf_topic_changed(self):
        """Reset TF tree when topic selection changes."""
        self.tf_tree = {}
        self.tf_info_text.setPlainText("Select 'Load TF Tree' to load transformations.")
        self.show_graph_button.setEnabled(False)

    def load_tf_tree(self):
        """Load TF tree from selected topic."""
        topic_name = self.tf_topic_combo.currentText()
        if not topic_name or "No TF topics" in topic_name or "Error" in topic_name:
            return

        # Use preloaded data if available
        if (
            self.selected_topics.get("tf_messages")
            and topic_name in self.selected_topics["tf_messages"]
        ):
            self.load_tf_tree_from_preloaded()
        else:
            self.tf_info_text.setPlainText("No preloaded TF data available.")

    def load_tf_tree_from_preloaded(self):
        """Load TF tree from preloaded message data."""
        topic_name = self.tf_topic_combo.currentText()
        tf_messages = self.selected_topics.get("tf_messages", {})

        if not topic_name or topic_name not in tf_messages:
            return

        try:
            print(f"[DEBUG] Loading TF tree from preloaded data for topic: {topic_name}")

            # Get the preloaded message data
            msg_data = tf_messages[topic_name]
            self.tf_tree = self.parse_preloaded_tf_message(topic_name, msg_data)

            self.update_tf_info_display()
            self.try_find_transform()

            if self.tf_tree:
                self.show_graph_button.setEnabled(True)

        except Exception as e:
            self.tf_info_text.setPlainText(f"Error loading preloaded TF tree: {str(e)}")
            print(f"[DEBUG] Error in load_tf_tree_from_preloaded: {str(e)}")

    def parse_preloaded_tf_message(self, topic_name: str, msg_data) -> Dict[str, Dict]:
        """Parse preloaded TF message."""
        tf_tree = {}

        # Convert rosbag message to our TFMessage format
        tf_msg = self.deserialize_tf_message(msg_data)

        for transform_stamped in tf_msg.transforms:
            parent_frame = transform_stamped.header.frame_id
            child_frame = transform_stamped.child_frame_id
            transform = transform_stamped.transform

            # Store the transformation matrix
            transform_matrix = ros_utils.transform_to_numpy(transform)

            if parent_frame not in tf_tree:
                tf_tree[parent_frame] = {}

            tf_tree[parent_frame][child_frame] = {
                "transform": transform_matrix,
                "transform_msg": transform,
            }

        print(
            f"[DEBUG] Built TF tree from preloaded data with {len(tf_tree)} parent frames and {len(tf_msg.transforms)} transforms"
        )
        return tf_tree

    def deserialize_tf_message(self, msg_data) -> ros_utils.TFMessage:
        """Convert rosbag message data to our TFMessage format."""
        transforms = []

        # Handle rosbag TFMessage format
        if hasattr(msg_data, "transforms"):
            for tf_stamped in msg_data.transforms:
                # Extract header information
                frame_id = getattr(tf_stamped.header, "frame_id", "unknown")
                child_frame_id = getattr(tf_stamped, "child_frame_id", "unknown")

                # Extract transform data
                translation = tf_stamped.transform.translation
                rotation = tf_stamped.transform.rotation

                header = ros_utils.Header(frame_id=frame_id)
                transform = ros_utils.Transform(
                    translation=ros_utils.Vector3(
                        x=translation.x, y=translation.y, z=translation.z
                    ),
                    rotation=ros_utils.Quaternion(
                        x=rotation.x, y=rotation.y, z=rotation.z, w=rotation.w
                    ),
                )
                transforms.append(
                    ros_utils.TransformStamped(
                        header=header, child_frame_id=child_frame_id, transform=transform
                    )
                )

        return ros_utils.TFMessage(transforms=transforms)

    def try_find_transform(self):
        """Try to find transformation between lidar and camera frames."""
        if not self.tf_tree:
            return

        transform_matrix = self.find_transform_path(self.lidar_frame, self.camera_frame)
        if transform_matrix is not None:
            self.current_transform = transform_matrix
            self.update_transform_display()
            self.update_manual_inputs_from_matrix()

    def find_transform_path(self, from_frame: str, to_frame: str) -> Optional[np.ndarray]:
        """Find transformation path between two frames in the TF tree using graph traversal."""
        if from_frame == to_frame:
            return np.eye(4)  # Identity for same frame

        # Build adjacency list for bidirectional search
        graph = {}
        for parent_frame, children in self.tf_tree.items():
            if parent_frame not in graph:
                graph[parent_frame] = []
            for child_frame, data in children.items():
                if child_frame not in graph:
                    graph[child_frame] = []
                # Add bidirectional edges
                graph[parent_frame].append(
                    (child_frame, data["transform"], False)
                )  # False = forward
                graph[child_frame].append((parent_frame, data["transform"], True))  # True = inverse

        # BFS to find shortest path
        from collections import deque

        if from_frame not in graph or to_frame not in graph:
            return None

        queue = deque([(from_frame, np.eye(4), [from_frame])])
        visited = {from_frame}

        while queue:
            current_frame, current_transform, path = queue.popleft()

            if current_frame == to_frame:
                return current_transform

            for neighbor, edge_transform, is_inverse in graph.get(current_frame, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    # Apply transformation
                    if is_inverse:
                        new_transform = np.dot(current_transform, np.linalg.inv(edge_transform))
                    else:
                        new_transform = np.dot(current_transform, edge_transform)

                    new_path = path + [neighbor]
                    queue.append((neighbor, new_transform, new_path))

        return None

    def find_transformation_path_frames(
        self, from_frame: str, to_frame: str
    ) -> Optional[List[str]]:
        """Find the sequence of frames in the transformation path."""
        if from_frame == to_frame:
            return [from_frame]

        # Build adjacency list
        graph = {}
        for parent_frame, children in self.tf_tree.items():
            if parent_frame not in graph:
                graph[parent_frame] = []
            for child_frame in children.keys():
                if child_frame not in graph:
                    graph[child_frame] = []
                graph[parent_frame].append(child_frame)
                graph[child_frame].append(parent_frame)

        # BFS to find shortest path
        from collections import deque

        if from_frame not in graph or to_frame not in graph:
            return None

        queue = deque([(from_frame, [from_frame])])
        visited = {from_frame}

        while queue:
            current_frame, path = queue.popleft()

            if current_frame == to_frame:
                return path

            for neighbor in graph.get(current_frame, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    new_path = path + [neighbor]
                    queue.append((neighbor, new_path))

        return None

    def update_tf_info_display(self):
        """Update the TF tree information display."""
        if not self.tf_tree:
            self.tf_info_text.setPlainText("No TF data available.")
            return

        info_text = "Available transformations:\n"
        for parent_frame, children in self.tf_tree.items():
            for child_frame in children.keys():
                info_text += f"  {parent_frame} → {child_frame}\n"

        # Check for transformation path
        transform_found = self.find_transform_path(self.lidar_frame, self.camera_frame)
        path_frames = self.find_transformation_path_frames(self.lidar_frame, self.camera_frame)

        if transform_found is not None and path_frames:
            info_text += (
                f"\n✓ Found transformation path: {self.lidar_frame} → {self.camera_frame}\n"
            )
            if len(path_frames) > 2:
                path_str = " → ".join(path_frames)
                info_text += f"  Path: {path_str}\n"
                info_text += f"  ({len(path_frames) - 1} hops through {len(path_frames) - 2} intermediate frames)"
            else:
                info_text += "  (Direct transformation)"
        else:
            info_text += f"\n✗ No transformation found: {self.lidar_frame} → {self.camera_frame}"

        self.tf_info_text.setPlainText(info_text)

    def show_tf_graph(self):
        """Show TF tree as a node graph."""
        if not self.tf_tree:
            return

        # Import the graph widget class from the original transformation_widget
        from .transformation_widget import TFGraphWidget

        # Find transformation path to highlight in graph
        path_frames = self.find_transformation_path_frames(self.lidar_frame, self.camera_frame)

        self.tf_graph_widget = TFGraphWidget(
            self.tf_tree, self.lidar_frame, self.camera_frame, path_frames
        )
        self.tf_graph_widget.show()

    def update_manual_transform(self):
        """Update transform from manual input fields."""
        try:
            tx = float(self.tx_input.text())
            ty = float(self.ty_input.text())
            tz = float(self.tz_input.text())
            rx = float(self.rx_input.text())
            ry = float(self.ry_input.text())
            rz = float(self.rz_input.text())

            # Create transformation matrix from translation and Euler angles
            from . import tf_transformations as tf

            translation_matrix = tf.translation_matrix([tx, ty, tz])
            rotation_matrix = tf.euler_matrix(rx, ry, rz)
            self.current_transform = np.dot(translation_matrix, rotation_matrix)

            self.update_transform_display()

        except ValueError as e:
            self.tf_info_text.setPlainText(f"Error parsing manual input: {str(e)}")

    def update_manual_inputs_from_matrix(self):
        """Update manual input fields from current transformation matrix."""
        try:
            from . import tf_transformations as tf

            translation = tf.translation_from_matrix(self.current_transform)
            euler = tf.euler_from_matrix(self.current_transform)

            self.tx_input.setText(f"{translation[0]:.6f}")
            self.ty_input.setText(f"{translation[1]:.6f}")
            self.tz_input.setText(f"{translation[2]:.6f}")
            self.rx_input.setText(f"{euler[0]:.6f}")
            self.ry_input.setText(f"{euler[1]:.6f}")
            self.rz_input.setText(f"{euler[2]:.6f}")

        except Exception:
            pass  # Ignore errors in updating input fields

    def use_identity_transform(self):
        """Set current transform to identity matrix."""
        self.current_transform = np.eye(4)
        self.update_transform_display()
        self.update_manual_inputs_from_matrix()

    def update_transform_display(self):
        """Update the transformation matrix display."""
        display_text = "Transformation Matrix (4x4):\n"
        for i in range(4):
            row_text = "  ".join(f"{self.current_transform[i, j]:8.4f}" for j in range(4))
            display_text += f"[{row_text}]\n"

        self.transform_display.setPlainText(display_text)

        # Update translation and rotation display
        from scipy.spatial.transform import Rotation

        from . import tf_transformations as tf

        try:
            translation = tf.translation_from_matrix(self.current_transform)
            rpy = Rotation.from_matrix(self.current_transform[:3, :3]).as_euler(
                "xyz", degrees=False
            )

            translation_text = (
                f"XYZ: [{translation[0]:.6f}, {translation[1]:.6f}, {translation[2]:.6f}]"
            )
            rotation_text = f"RPY: [{rpy[0]:.6f}, {rpy[1]:.6f}, {rpy[2]:.6f}]"

            self.translation_rotation_display.setPlainText(f"{translation_text}\n{rotation_text}")
        except Exception:
            self.translation_rotation_display.setPlainText(
                "Translation: [0.0, 0.0, 0.0]\nRPY: [0.0, 0.0, 0.0]"
            )

    def confirm_transformation(self):
        """Confirm and proceed to calibration with selected transformation."""
        # Invert the transformation for LiDAR-to-camera projection
        # The transformation represents camera-to-lidar, but we need lidar-to-camera
        inverted_transform = np.linalg.inv(self.current_transform)
        print(f"[DEBUG] Original transform:\n{self.current_transform}")
        print(f"[DEBUG] Inverted transform for projection:\n{inverted_transform}")

        # Proceed directly to calibration
        self.proceed_to_calibration(inverted_transform)

    # Calibration Results View Methods

    def show_calibration_results(self, calibrated_transform):
        """Show calibration results and TF integration options."""
        self.calibrated_transform = calibrated_transform

        # Switch to results view (index 3 after frame selection was added)
        results_index = self.get_results_view_index()
        self.stacked_widget.setCurrentIndex(results_index)  # Results view
        print(f"[DEBUG] Switched to results view at index {results_index}")

        # Populate the results view
        self.populate_results_view()

    def populate_results_view(self):
        """Populate the results view with calibration data."""
        # Set source frame
        self.source_frame_label.setText(self.lidar_frame)

        # Find all frames in the transformation chain from LiDAR to camera optical frame
        chain_frames = self.find_frames_in_lidar_to_camera_chain()
        # Filter out the source frame (LiDAR frame) since it can't be both source and target
        valid_target_frames = [frame for frame in chain_frames if frame != self.lidar_frame]
        self.target_frame_combo.clear()
        self.target_frame_combo.addItems(valid_target_frames)

        # Try to set the original camera frame as default
        camera_index = self.target_frame_combo.findText(self.camera_frame)
        if camera_index >= 0:
            self.target_frame_combo.setCurrentIndex(camera_index)

        # Display calibrated transform
        self.display_calibrated_transform()

        # Update transform chain
        self.update_transform_chain()

        # Show embedded graph
        self.update_embedded_graph()

    def find_connected_frames(self, frame):
        """Find all frames connected to the given frame through valid TF paths."""
        if not self.tf_tree:
            return [frame]

        # Use the existing find_transformation_path_frames method to check reachability
        all_frames = set()

        # Get all unique frame names from the TF tree
        for parent_frame, children in self.tf_tree.items():
            all_frames.add(parent_frame)
            all_frames.update(children.keys())

        # Filter to only include frames that have a valid path from the given frame
        connected_frames = []
        for target_frame in all_frames:
            if target_frame == frame:
                connected_frames.append(target_frame)
            else:
                # Check if there's a valid transformation path
                path = self.find_transformation_path_frames(frame, target_frame)
                if path is not None:
                    connected_frames.append(target_frame)

        return sorted(connected_frames)

    def find_frames_in_lidar_to_camera_chain(self):
        """Find all frames that are on the transformation path from LiDAR to camera optical frame."""
        if not self.tf_tree:
            return [self.camera_frame]

        # Get the path from LiDAR to camera optical frame
        path_frames = self.find_transformation_path_frames(self.lidar_frame, self.camera_frame)

        if path_frames:
            # Return all frames in the path, but starting from camera side frames
            # This gives options like: camera_optical_frame, camera_center, camera_link, etc.
            # Reverse the path so camera frames come first (more intuitive for selection)
            return list(reversed(path_frames))
        else:
            # If no path found, just return the camera frame
            return [self.camera_frame]

    def _sanitize_frame_id(self, frame_id):
        """Convert frame ID to valid URDF link name by replacing / with _."""
        return frame_id.replace("/", "_").replace("-", "_")

    def display_calibrated_transform(self):
        """Display the calibrated transformation matrix."""
        display_text = f"{self.lidar_frame} → {self.camera_frame}:\n"
        for i in range(4):
            row_text = "  ".join(f"{self.calibrated_transform[i, j]:8.4f}" for j in range(4))
            display_text += f"[{row_text}]\n"

        # Add translation and rotation info
        from scipy.spatial.transform import Rotation

        tvec = self.calibrated_transform[:3, 3]
        rpy = Rotation.from_matrix(self.calibrated_transform[:3, :3]).as_euler("xyz", degrees=False)
        display_text += f"\nXYZ: [{tvec[0]:.6f}, {tvec[1]:.6f}, {tvec[2]:.6f}]"
        display_text += f"\nRPY: [{rpy[0]:.6f}, {rpy[1]:.6f}, {rpy[2]:.6f}]"

        # Add URDF snippet - sanitize only the joint name, keep original frame IDs for links
        joint_name = f"{self._sanitize_frame_id(self.lidar_frame)}_2_{self._sanitize_frame_id(self.camera_frame)}_calibrated"
        display_text += "\n\nURDF Joint:\n"
        display_text += f'<joint name="{joint_name}" type="fixed">\n'
        display_text += f'  <parent link="{self.lidar_frame}" />\n'
        display_text += f'  <child link="{self.camera_frame}" />\n'
        display_text += f'  <origin xyz="{tvec[0]:.6f} {tvec[1]:.6f} {tvec[2]:.6f}" rpy="{rpy[0]:.6f} {rpy[1]:.6f} {rpy[2]:.6f}" />\n'
        display_text += "</joint>"

        self.calibration_result_display.setPlainText(display_text)

    def update_transform_chain(self):
        """Update the transformation chain display."""
        target_frame = self.target_frame_combo.currentText()
        if not target_frame:
            return

        # Find path from LiDAR to target frame (going through the camera optical frame)
        lidar_to_target_path = self.find_transformation_path_frames(self.lidar_frame, target_frame)

        if lidar_to_target_path and len(lidar_to_target_path) > 1:
            chain_text = " → ".join(lidar_to_target_path)
            self.chain_display.setPlainText(f"Chain: {chain_text}")

            if target_frame == self.camera_frame:
                # Direct calibrated transform (LiDAR → Camera Optical)
                self.display_final_transform(self.calibrated_transform, target_frame)
            else:
                # Calculate transform: LiDAR → Target
                # This is: (LiDAR → Camera Optical) × (Camera Optical → Target)^-1
                # Because we want LiDAR → Target, but we go through camera optical

                # Find transform from target to camera optical frame
                target_to_optical = self.find_transform_path(target_frame, self.camera_frame)
                if target_to_optical is not None:
                    # Final transform: LiDAR → Target = LiDAR → Camera Optical × (Target → Camera Optical)^-1
                    target_to_optical_inv = np.linalg.inv(target_to_optical)
                    final_transform = np.dot(self.calibrated_transform, target_to_optical_inv)
                    self.display_final_transform(final_transform, target_frame)
                else:
                    self.final_transform_display.setPlainText("No transform path found.")
        else:
            self.chain_display.setPlainText(f"Direct: {target_frame}")
            self.display_final_transform(self.calibrated_transform, target_frame)

        # Update embedded graph
        self.update_embedded_graph()

    def update_embedded_graph(self):
        """Update the embedded graph visualization."""
        target_frame = self.target_frame_combo.currentText()
        if not target_frame or not self.tf_tree:
            # Show placeholder text
            self.show_graph_placeholder()
            return

        try:
            # Create embedded TF graph
            self.create_embedded_tf_graph(target_frame)
        except Exception as e:
            print(f"[DEBUG] Error creating embedded graph: {e}")
            self.show_graph_placeholder()

    def show_graph_placeholder(self):
        """Show placeholder text in graph container."""
        self.clear_graph_container()

        layout = QVBoxLayout(self.graph_container)
        placeholder = QLabel("TF Graph will appear here when data is available")
        placeholder.setAlignment(Qt.AlignCenter)
        placeholder.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(placeholder)

    def clear_graph_container(self):
        """Safely clear the graph container by recreating it."""
        # Store parent and properties
        parent = self.graph_container.parent()
        min_height = self.graph_container.minimumHeight()
        max_height = self.graph_container.maximumHeight()
        style_sheet = self.graph_container.styleSheet()

        # Remove old container from parent layout
        if parent and parent.layout():
            parent.layout().removeWidget(self.graph_container)

        # Delete old container
        self.graph_container.deleteLater()

        # Create new container
        self.graph_container = QWidget()
        self.graph_container.setMinimumHeight(min_height)
        self.graph_container.setMaximumHeight(max_height)
        self.graph_container.setStyleSheet(style_sheet)

        # Add back to parent layout
        if parent and parent.layout():
            # Find the position where the graph container should be (after chain display)
            layout = parent.layout()
            for i in range(layout.count()):
                if layout.itemAt(i).widget() == self.chain_display:
                    layout.insertWidget(i + 1, self.graph_container)
                    break
            else:
                # Fallback: add at end
                layout.addWidget(self.graph_container)

    def create_embedded_tf_graph(self, target_frame):
        """Create an embedded TF graph widget."""
        # Find all frames in the path from lidar to target
        lidar_to_target_path = self.find_transformation_path_frames(self.lidar_frame, target_frame)

        if not lidar_to_target_path:
            self.show_graph_placeholder()
            return

        # Filter TF tree to only include relevant transforms
        all_frames = set(lidar_to_target_path)
        filtered_tf_tree = {}
        for parent_frame, children in self.tf_tree.items():
            if parent_frame in all_frames:
                filtered_children = {
                    child: data for child, data in children.items() if child in all_frames
                }
                if filtered_children:
                    filtered_tf_tree[parent_frame] = filtered_children

        if filtered_tf_tree:
            try:
                from .transformation_widget import TFGraphWidget

                # Clear existing graph safely
                self.clear_graph_container()

                # Create new layout and graph
                layout = QVBoxLayout(self.graph_container)

                # Create a mini version of the TF graph
                graph_widget = TFGraphWidget(
                    filtered_tf_tree, self.lidar_frame, target_frame, lidar_to_target_path
                )

                # Get the graph widget and add it directly
                graph_qt_widget = graph_widget.graph.widget
                graph_qt_widget.setMaximumHeight(250)
                graph_qt_widget.setParent(self.graph_container)
                layout.addWidget(graph_qt_widget)

            except Exception as e:
                print(f"[DEBUG] Error creating TF graph widget: {e}")
                self.show_graph_placeholder()
        else:
            self.show_graph_placeholder()

    def display_final_transform(self, transform, target_frame):
        """Display the final transform for URDF integration."""
        display_text = f"{self.lidar_frame} → {target_frame}:\n"
        for i in range(4):
            row_text = "  ".join(f"{transform[i, j]:8.4f}" for j in range(4))
            display_text += f"[{row_text}]\n"

        # Add translation and rotation info
        from scipy.spatial.transform import Rotation

        tvec = transform[:3, 3]
        rpy = Rotation.from_matrix(transform[:3, :3]).as_euler("xyz", degrees=False)

        display_text += f"\nXYZ: [{tvec[0]:.6f}, {tvec[1]:.6f}, {tvec[2]:.6f}]"
        display_text += f"\nRPY: [{rpy[0]:.6f}, {rpy[1]:.6f}, {rpy[2]:.6f}]"

        # Add URDF snippet - sanitize only the joint name, keep original frame IDs for links
        joint_name = f"{self._sanitize_frame_id(self.lidar_frame)}_2_{self._sanitize_frame_id(target_frame)}_calibrated"
        display_text += "\n\nURDF Joint:\n"
        display_text += f'<joint name="{joint_name}" type="fixed">\n'
        display_text += f'  <parent link="{self.lidar_frame}" />\n'
        display_text += f'  <child link="{target_frame}" />\n'
        display_text += f'  <origin xyz="{tvec[0]:.6f} {tvec[1]:.6f} {tvec[2]:.6f}" rpy="{rpy[0]:.6f} {rpy[1]:.6f} {rpy[2]:.6f}" />\n'
        display_text += "</joint>"

        self.final_transform_display.setPlainText(display_text)
        self.current_final_transform = transform

    def export_calibration_result(self):
        """Export the complete calibration results including both text boxes."""
        from PySide6.QtWidgets import QFileDialog

        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Calibration Results", "", "Text Files (*.txt)"
        )
        if file_path:
            with open(file_path, "w") as f:
                f.write("# LiDAR-Camera Extrinsic Calibration Results\n")
                f.write("# Generated by ros2_calib\n\n")

                f.write("=" * 60 + "\n")
                f.write("CALIBRATION RESULTS\n")
                f.write("=" * 60 + "\n\n")
                f.write(self.calibration_result_display.toPlainText())
                f.write("\n\n")

                f.write("=" * 60 + "\n")
                f.write("TARGET TRANSFORM\n")
                f.write("=" * 60 + "\n\n")
                f.write(self.final_transform_display.toPlainText())
                f.write("\n")
            print(f"Calibration results saved to {file_path}")

    def dragEnterEvent(self, event: QDragEnterEvent):
        """Handle drag enter event for the main window."""
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event: QDropEvent):
        """Handle drop event for the main window."""
        urls = event.mimeData().urls()
        if urls:
            file_path = urls[0].toLocalFile()
            self.process_dropped_path(file_path)
            event.acceptProposedAction()
