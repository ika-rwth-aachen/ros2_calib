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

import cv2
import numpy as np
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QImage, QPixmap
from PySide6.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from . import ros_utils
from .bag_handler import convert_to_mock


class FrameSelectionWidget(QWidget):
    """Widget for selecting which frame to use for calibration from multiple samples."""

    frame_selected = Signal(int)  # Emits the selected frame index

    def __init__(self, parent=None):
        super().__init__(parent)
        self.frame_samples = None
        self.image_topic = None
        self.selected_frame_index = 0

        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel("Frame Selection")
        header.setStyleSheet("font-size: 18px; font-weight: bold; padding: 10px;")
        layout.addWidget(header)

        instruction = QLabel("Select which frame to use for calibration:")
        instruction.setStyleSheet("color: #ccc; margin-bottom: 15px;")
        layout.addWidget(instruction)

        # Scrollable area for frame thumbnails
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        layout.addWidget(self.scroll_area)

        # Container for frame thumbnails
        self.frames_widget = QWidget()
        self.frames_layout = QGridLayout(self.frames_widget)
        self.scroll_area.setWidget(self.frames_widget)

        # Bottom controls
        bottom_layout = QHBoxLayout()

        # Back button
        self.back_button = QPushButton("← Back to Transform Selection")
        self.back_button.clicked.connect(self.go_back)
        bottom_layout.addWidget(self.back_button)

        bottom_layout.addStretch()

        # Proceed button
        self.proceed_button = QPushButton("Proceed to Calibration")
        self.proceed_button.setStyleSheet("font-weight: bold; padding: 10px;")
        self.proceed_button.clicked.connect(self.proceed_to_calibration)
        bottom_layout.addWidget(self.proceed_button)

        layout.addLayout(bottom_layout)

    def set_frame_samples(self, frame_samples, image_topic):
        """Set the frame samples and display them."""
        self.frame_samples = frame_samples
        self.image_topic = image_topic
        self.display_frames()

    def display_frames(self):
        """Display frame thumbnails for selection."""
        if not self.frame_samples or not self.image_topic:
            return

        # Clear existing frames
        for i in reversed(range(self.frames_layout.count())):
            self.frames_layout.itemAt(i).widget().setParent(None)

        image_samples = self.frame_samples.get(self.image_topic, [])

        if not image_samples:
            no_frames_label = QLabel("No image frames found!")
            no_frames_label.setAlignment(Qt.AlignCenter)
            no_frames_label.setStyleSheet("color: red; font-size: 14px; padding: 50px;")
            self.frames_layout.addWidget(no_frames_label, 0, 0)
            return

        # Display frames in a grid (3 columns)
        cols = 3
        for i, frame_data in enumerate(image_samples):
            row = i // cols
            col = i % cols

            frame_widget = self.create_frame_widget(frame_data, i)
            self.frames_layout.addWidget(frame_widget, row, col)

        # Select first frame by default
        if image_samples:
            self.select_frame(0)

    def create_frame_widget(self, frame_data, index):
        """Create a widget for a single frame."""
        widget = QWidget()
        widget.setFixedSize(400, 320)  # Increased size for better visibility
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(5, 5, 5, 5)

        # Convert image data to QPixmap for display
        # Initialize pixmap with placeholder in case of error
        pixmap = QPixmap(380, 240)  # Increased thumbnail size
        pixmap.fill(Qt.gray)

        try:
            raw_image_data = frame_data["data"]
            topic_type = frame_data.get("topic_type", "sensor_msgs/msg/Image")

            # Convert raw rosbag message to proper format first

            # Convert based on topic type
            image_msg = convert_to_mock(raw_image_data, topic_type)

            # Use the same image processing as calibration widget
            if hasattr(image_msg, "_type") and image_msg._type == "sensor_msgs/msg/CompressedImage":
                # Handle compressed image using cv2.imdecode
                np_arr = np.frombuffer(image_msg.data, np.uint8)
                img_array = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            else:
                # Handle regular image using ros_utils.image_to_numpy
                img_array = ros_utils.image_to_numpy(image_msg)
                # Convert from RGB to BGR if needed
                if "bgr" not in image_msg.encoding and len(img_array.shape) == 3:
                    img_array = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
                # Ensure it's 3-channel BGR
                if len(img_array.shape) == 2:  # Grayscale
                    img_array = cv2.cvtColor(img_array, cv2.COLOR_GRAY2BGR)

            # Resize for thumbnail while maintaining aspect ratio
            height, width = img_array.shape[:2]
            aspect_ratio = width / height

            # Increased thumbnail size
            max_thumb_width = 380
            max_thumb_height = 240

            if aspect_ratio > max_thumb_width / max_thumb_height:
                # Width-constrained
                thumb_width = max_thumb_width
                thumb_height = int(thumb_width / aspect_ratio)
            else:
                # Height-constrained
                thumb_height = max_thumb_height
                thumb_width = int(thumb_height * aspect_ratio)

            img_resized = cv2.resize(img_array, (thumb_width, thumb_height))

            # Convert to QPixmap
            height, width, channel = img_resized.shape
            bytes_per_line = 3 * width

            # Convert BGR to RGB for Qt
            img_rgb = cv2.cvtColor(img_resized, cv2.COLOR_BGR2RGB)

            q_image = QImage(img_rgb.data, width, height, bytes_per_line, QImage.Format_RGB888)
            pixmap = QPixmap.fromImage(q_image)

        except Exception as e:
            print(f"Error creating frame thumbnail: {e}")
            # pixmap is already set to placeholder above

        # Image label
        image_label = QLabel()
        image_label.setPixmap(pixmap)
        image_label.setAlignment(Qt.AlignCenter)
        image_label.setStyleSheet("border: 2px solid #666; background-color: #333;")
        layout.addWidget(image_label)

        # Frame info
        timestamp = frame_data.get("timestamp", "Unknown")
        info_label = QLabel(f"Frame {index + 1}")
        info_label.setAlignment(Qt.AlignCenter)
        info_label.setStyleSheet("color: white; font-weight: bold; padding: 5px;")
        layout.addWidget(info_label)

        # Selection button
        select_button = QPushButton("Select This Frame")
        select_button.clicked.connect(lambda: self.select_frame(index))
        layout.addWidget(select_button)

        # Store reference for selection highlighting
        widget.image_label = image_label
        widget.frame_index = index

        return widget

    def select_frame(self, index):
        """Select a frame and highlight it."""
        self.selected_frame_index = index

        # Update visual selection
        for i in range(self.frames_layout.count()):
            item = self.frames_layout.itemAt(i)
            if item and item.widget():
                widget = item.widget()
                if hasattr(widget, "image_label") and hasattr(widget, "frame_index"):
                    if widget.frame_index == index:
                        # Highlight selected frame
                        widget.image_label.setStyleSheet(
                            "border: 3px solid #4CAF50; background-color: #333;"
                        )
                    else:
                        # Normal border for non-selected frames
                        widget.image_label.setStyleSheet(
                            "border: 2px solid #666; background-color: #333;"
                        )

        print(f"Selected frame {index + 1}")

    def go_back(self):
        """Go back to transform selection."""
        self.parent().setCurrentIndex(1)  # Transform view

    def proceed_to_calibration(self):
        """Proceed to calibration with selected frame."""
        if self.frame_samples and self.image_topic:
            self.frame_selected.emit(self.selected_frame_index)
