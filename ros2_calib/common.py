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

"""
Common styling and configuration constants for ros2_calib application.
"""


# UI Styling Constants
class UIStyles:
    """Centralized UI styling configuration."""

    # Button highlight style - orange background with rounded corners
    HIGHLIGHT_BUTTON = """
        QPushButton {
            background-color: #fc9403;
            color: white;
        }
        QPushButton:hover {
            background-color: #ff9f14;
            border-color: #f5941a;
        }
        QPushButton:pressed {
            background-color: #e6860c;
            border-color: #d17a0a;
        }
    """

    # Default button style - system default appearance
    DEFAULT_BUTTON = ""

    # Colors
    HIGHLIGHT_COLOR = "#fc9403"
    HIGHLIGHT_BORDER_COLOR = "#e8a20b"
    HOVER_COLOR = "#ff9f14"
    PRESSED_COLOR = "#e6860c"


class Colors:
    """Color constants used throughout the application."""

    # UI Colors
    HIGHLIGHT_ORANGE = "#fc9403"
    WARNING_RED = "#dc3545"
    SUCCESS_GREEN = "#28a745"
    INFO_BLUE = "#17a2b8"

    # Graphics Colors
    CORRESPONDENCE_2D = "yellow"
    CORRESPONDENCE_3D = "cyan"
    SELECTION_HIGHLIGHT = "cyan"
    CROSS_MARKER = "yellow"


class AppConstants:
    """Application-wide constants."""

    APP_NAME = "ros2_calib"
    VERSION = "0.1.0"
    DESCRIPTION = "Manual LiDAR-Camera Calibration Tool"

    # Default values
    DEFAULT_POINT_SIZE = 4
    DEFAULT_COLORMAP = "inferno"
    DEFAULT_TRANSLATION_STEP = 1.0  # cm
    DEFAULT_ROTATION_STEP = 0.1  # degrees

    # Minimum requirements
    MIN_CORRESPONDENCES = 4
