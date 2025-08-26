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

import time
from collections import deque
from typing import Dict, List, Optional

import numpy as np
from NodeGraphQt import BaseNode, NodeGraph
from NodeGraphQt.constants import PipeLayoutEnum
from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QComboBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from . import ros_utils
from . import tf_transformations as tf
from .bag_handler import get_topic_info, read_messages


class TFFrameNode(BaseNode):
    """Custom node for TF frames."""

    __identifier__ = "tf"
    NODE_NAME = "TFFrameNode"

    def __init__(self):
        super().__init__()
        self.add_input("parent")
        self.add_output("child")


class TransformationWidget(QWidget):
    transformation_selected = Signal(np.ndarray)

    def __init__(
        self, bag_path: str, lidar_frame: str, camera_frame: str, preloaded_tf_messages: dict = None
    ):
        super().__init__()
        self.bag_path = bag_path
        self.lidar_frame = lidar_frame
        self.camera_frame = camera_frame
        self.tf_tree = {}
        self.current_transform = np.eye(4)  # Default identity matrix
        self.preloaded_tf_messages = preloaded_tf_messages or {}

        self.setWindowTitle("Initial Transformation Selection")
        self.setGeometry(200, 200, 800, 600)

        self.setup_ui()

        # If we have preloaded TF messages, populate the combo box and process them
        if self.preloaded_tf_messages:
            self.load_tf_topics_from_preloaded()
        else:
            self.load_tf_topics()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Title
        title_label = QLabel(
            f"Select Initial Transformation: {self.lidar_frame} → {self.camera_frame}"
        )
        title_label.setStyleSheet("font-size: 14px; font-weight: bold; margin: 10px;")
        layout.addWidget(title_label)

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

        layout.addWidget(tf_group)

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

        layout.addWidget(manual_group)

        # Current Transform Display
        transform_group = QGroupBox("Current Transformation Matrix")
        transform_layout = QVBoxLayout(transform_group)

        self.transform_display = QTextEdit()
        self.transform_display.setMaximumHeight(120)
        self.transform_display.setFont("monospace")
        transform_layout.addWidget(self.transform_display)

        layout.addWidget(transform_group)

        # Action buttons
        button_layout = QHBoxLayout()

        self.use_identity_button = QPushButton("Use Identity Transform")
        self.use_identity_button.clicked.connect(self.use_identity_transform)
        button_layout.addWidget(self.use_identity_button)

        button_layout.addStretch()

        self.confirm_button = QPushButton("Confirm Transformation")
        self.confirm_button.clicked.connect(self.confirm_transformation)
        self.confirm_button.setStyleSheet("font-weight: bold;")
        button_layout.addWidget(self.confirm_button)

        layout.addLayout(button_layout)

        # Initialize display
        self.update_transform_display()

    def load_tf_topics(self):
        """Load available TF topics from the bag."""
        try:
            topic_info = get_topic_info(self.bag_path)

            tf_topics = []
            for topic_name, topic_type, _ in topic_info:
                if "tf" in topic_name.lower() and "TFMessage" in topic_type:
                    tf_topics.append(topic_name)

            self.tf_topic_combo.addItems(tf_topics)

            if not tf_topics:
                self.tf_topic_combo.addItem("No TF topics found")
                self.load_tf_button.setEnabled(False)

        except Exception as e:
            self.tf_topic_combo.addItem(f"Error loading topics: {str(e)}")
            self.load_tf_button.setEnabled(False)

    def load_tf_topics_from_preloaded(self):
        """Load available TF topics from preloaded messages."""
        try:
            tf_topics = list(self.preloaded_tf_messages.keys())
            self.tf_topic_combo.addItems(tf_topics)

            if tf_topics:
                print(f"[DEBUG] Loaded preloaded TF topics: {tf_topics}")
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

        except Exception as e:
            self.tf_topic_combo.addItem(f"Error loading preloaded topics: {str(e)}")
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

        # Use preloaded data if available, otherwise read from rosbag
        if self.preloaded_tf_messages and topic_name in self.preloaded_tf_messages:
            self.load_tf_tree_from_preloaded()
        else:
            # Fallback to reading from rosbag
            try:
                print(f"[DEBUG] Starting TF tree loading for topic: {topic_name}")
                start_time = time.time()

                self.tf_tree = self.parse_tf_messages(topic_name)
                parse_time = time.time()
                print(f"[DEBUG] TF parsing took: {parse_time - start_time:.2f} seconds")

                self.update_tf_info_display()
                info_time = time.time()
                print(f"[DEBUG] Info display update took: {info_time - parse_time:.2f} seconds")

                self.try_find_transform()
                transform_time = time.time()
                print(f"[DEBUG] Transform finding took: {transform_time - info_time:.2f} seconds")

                if self.tf_tree:
                    self.show_graph_button.setEnabled(True)

                total_time = time.time()
                print(f"[DEBUG] Total TF tree loading took: {total_time - start_time:.2f} seconds")

            except Exception as e:
                self.tf_info_text.setPlainText(f"Error loading TF tree: {str(e)}")
                print(f"[DEBUG] Error in load_tf_tree: {str(e)}")

    def load_tf_tree_from_preloaded(self):
        """Load TF tree from preloaded message data - much faster!"""
        topic_name = self.tf_topic_combo.currentText()
        if not topic_name or topic_name not in self.preloaded_tf_messages:
            return

        try:
            print(f"[DEBUG] Loading TF tree from preloaded data for topic: {topic_name}")
            start_time = time.time()

            # Get the preloaded message data
            msg_data = self.preloaded_tf_messages[topic_name]
            self.tf_tree = self.parse_preloaded_tf_message(topic_name, msg_data)

            parse_time = time.time()
            print(f"[DEBUG] Preloaded TF parsing took: {parse_time - start_time:.2f} seconds")

            self.update_tf_info_display()
            info_time = time.time()
            print(f"[DEBUG] Info display update took: {info_time - parse_time:.2f} seconds")

            self.try_find_transform()
            transform_time = time.time()
            print(f"[DEBUG] Transform finding took: {transform_time - info_time:.2f} seconds")

            if self.tf_tree:
                self.show_graph_button.setEnabled(True)

            total_time = time.time()
            print(
                f"[DEBUG] Total preloaded TF tree loading took: {total_time - start_time:.2f} seconds"
            )

        except Exception as e:
            self.tf_info_text.setPlainText(f"Error loading preloaded TF tree: {str(e)}")
            print(f"[DEBUG] Error in load_tf_tree_from_preloaded: {str(e)}")

    def parse_preloaded_tf_message(self, topic_name: str, msg_data) -> Dict[str, Dict]:
        """Parse preloaded TF message - no rosbag reading needed!"""
        tf_tree = {}

        print(f"[DEBUG] Processing preloaded TF message for topic: {topic_name}")

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

    def parse_tf_messages(self, topic_name: str) -> Dict[str, Dict]:
        """Parse TF messages and build transformation tree from first tf_static message."""
        tf_tree = {}
        message_count = 0

        print(f"[DEBUG] Starting to read first message from topic: {topic_name}")
        read_start = time.time()

        # For tf_static, we only need the first message since it contains static transforms
        for timestamp, msg_data in read_messages(self.bag_path, [topic_name]):
            message_count += 1
            print(f"[DEBUG] Processing message {message_count}...")

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

            # For tf_static, we only need one message since all static transforms are in one message
            if "tf_static" in topic_name:
                print(
                    f"[DEBUG] Found tf_static message with {len(tf_msg.transforms)} transforms, stopping read."
                )
                break

        read_end = time.time()
        print(f"[DEBUG] Read {message_count} TF message(s) in {read_end - read_start:.2f} seconds")
        print(f"[DEBUG] Built TF tree with {len(tf_tree)} parent frames")

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
            translation_matrix = tf.translation_matrix([tx, ty, tz])
            rotation_matrix = tf.euler_matrix(rx, ry, rz)
            self.current_transform = np.dot(translation_matrix, rotation_matrix)

            self.update_transform_display()

        except ValueError as e:
            self.tf_info_text.setPlainText(f"Error parsing manual input: {str(e)}")

    def update_manual_inputs_from_matrix(self):
        """Update manual input fields from current transformation matrix."""
        try:
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

    def confirm_transformation(self):
        """Confirm and emit the selected transformation."""
        # Invert the transformation for LiDAR-to-camera projection
        # The transformation represents camera-to-lidar, but we need lidar-to-camera
        inverted_transform = np.linalg.inv(self.current_transform)
        print(f"[DEBUG] Original transform:\n{self.current_transform}")
        print(f"[DEBUG] Inverted transform for projection:\n{inverted_transform}")
        self.transformation_selected.emit(inverted_transform)
        self.close()


class TFGraphWidget(QWidget):
    """Widget to display TF tree as a node graph."""

    def __init__(
        self,
        tf_tree: Dict,
        lidar_frame: str,
        camera_frame: str,
        path_frames: Optional[List[str]] = None,
    ):
        super().__init__()
        self.tf_tree = tf_tree
        self.lidar_frame = lidar_frame
        self.camera_frame = camera_frame
        self.path_frames = path_frames or []

        self.setWindowTitle("TF Tree Visualization")
        self.setGeometry(300, 300, 1000, 700)

        self.setup_graph()

    def setup_graph(self):
        """Setup the node graph visualization."""
        layout = QVBoxLayout(self)

        # Create node graph
        self.graph = NodeGraph()

        # Configure graph layout and pipe style
        # self.graph.set_layout_direction(LayoutDirectionEnum.VERTICAL.value)
        self.graph.set_pipe_style(PipeLayoutEnum.STRAIGHT.value)

        # Register our custom node type first
        self.graph.register_node(TFFrameNode)

        # Create frame nodes using our custom type
        frame_nodes = {}
        path_set = set(self.path_frames)

        for parent_frame, children in self.tf_tree.items():
            if parent_frame not in frame_nodes:
                node = self.graph.create_node("tf.TFFrameNode", name=parent_frame)

                # Highlight nodes in the transformation path
                if parent_frame == self.lidar_frame:
                    node.set_color(50, 150, 50)  # Green for LiDAR frame
                elif parent_frame == self.camera_frame:
                    node.set_color(50, 50, 150)  # Blue for camera frame
                elif parent_frame in path_set:
                    node.set_color(150, 150, 50)  # Yellow for intermediate frames
                else:
                    node.set_color(80, 80, 80)  # Gray for other frames

                frame_nodes[parent_frame] = node

            for child_frame in children.keys():
                if child_frame not in frame_nodes:
                    node = self.graph.create_node("tf.TFFrameNode", name=child_frame)

                    # Highlight nodes in the transformation path
                    if child_frame == self.lidar_frame:
                        node.set_color(50, 150, 50)  # Green for LiDAR frame
                    elif child_frame == self.camera_frame:
                        node.set_color(50, 50, 150)  # Blue for camera frame
                    elif child_frame in path_set:
                        node.set_color(150, 150, 50)  # Yellow for intermediate frames
                    else:
                        node.set_color(80, 80, 80)  # Gray for other frames

                    frame_nodes[child_frame] = node

                # Connect parent to child
                parent_node = frame_nodes[parent_frame]
                child_node = frame_nodes[child_frame]

                # Connect the nodes using the pre-defined ports
                parent_node.output(0).connect_to(child_node.input(0))

        # Auto-layout the nodes with vertical arrangement
        self.graph.auto_layout_nodes()

        # Add info label
        info_label = QLabel()
        if self.path_frames and len(self.path_frames) > 1:
            path_str = " → ".join(self.path_frames)
            info_text = f"Transformation Path: {path_str}\n"
            info_text += f"🟢 {self.lidar_frame} (LiDAR)  🔵 {self.camera_frame} (Camera)  🟡 Intermediate frames"
        else:
            info_text = (
                f"No transformation path found between {self.lidar_frame} and {self.camera_frame}"
            )

        info_label.setText(info_text)
        info_label.setStyleSheet(
            "padding: 10px; background-color: #2a2a2a; color: white; border: 1px solid #555;"
        )
        layout.addWidget(info_label)

        # Add the graph widget to layout
        graph_widget = self.graph.widget
        layout.addWidget(graph_widget)

        # Fit to view
        self.graph.fit_to_selection()
