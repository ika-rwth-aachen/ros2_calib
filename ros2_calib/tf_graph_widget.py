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

from typing import Dict, List, Optional

from NodeGraphQt import BaseNode, NodeGraph
from NodeGraphQt.constants import PipeLayoutEnum
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QHBoxLayout, QLabel, QVBoxLayout, QWidget


class TFFrameNode(BaseNode):
    """Custom node for representing a single TF frame in the graph."""

    __identifier__ = "tf"
    NODE_NAME = "TFFrameNode"

    def __init__(self):
        super(TFFrameNode, self).__init__()
        self.add_input("parent")
        self.add_output("child")
        self.set_color(80, 80, 80)  # Default color


class TFGraphWidget(QWidget):
    """A widget to display a TF (Transform) tree as a node graph."""

    def __init__(
        self,
        tf_tree: Dict,
        source_frame: str,
        target_frame: str,
        path_frames: Optional[List[str]] = None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.tf_tree = tf_tree
        self.source_frame = source_frame
        self.target_frame = target_frame
        self.path_frames = path_frames or []

        self.setWindowTitle("TF Tree Visualization")
        self.setGeometry(300, 300, 1000, 700)

        # Set window flags to make it a proper independent window
        self.setWindowFlags(Qt.Window | Qt.WindowCloseButtonHint | Qt.WindowMinMaxButtonsHint)
        self.setAttribute(Qt.WA_DeleteOnClose, True)

        self._setup_ui()

    def closeEvent(self, event):
        """Handle window close event properly."""
        event.accept()

    def _setup_ui(self):
        """Initializes the node graph and populates it with the TF tree."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Create the node graph controller
        self.graph = NodeGraph()
        self.graph.set_pipe_style(PipeLayoutEnum.STRAIGHT.value)
        self.graph.register_node(TFFrameNode)

        # Create nodes for each frame in the TF tree
        self._create_nodes()

        # Connect the nodes based on the parent-child relationships
        self._connect_nodes()

        # Arrange nodes automatically for a clean layout
        self.graph.auto_layout_nodes()
        self.graph.fit_to_selection()

        # Add the graph widget to the layout
        layout.addWidget(self.graph.widget)

        self.legend_widget = self._build_legend_widget(self.graph.widget)
        self.legend_widget.adjustSize()
        self.legend_widget.move(12, 12)
        self.legend_widget.raise_()

    def _build_legend_widget(self, parent: QWidget) -> QWidget:
        legend = QWidget(parent)
        legend_layout = QHBoxLayout(legend)
        legend_layout.setContentsMargins(12, 8, 12, 8)
        legend_layout.setSpacing(16)

        legend.setStyleSheet(
            "background-color: rgba(255, 255, 255, 235);border: 1px solid #bbb;border-radius: 6px;"
        )
        legend.setAttribute(Qt.WA_TransparentForMouseEvents, True)

        self._add_legend_item(legend_layout, "#329632", "Source frame")
        self._add_legend_item(legend_layout, "#969632", "Path frame")
        self._add_legend_item(legend_layout, "#323296", "Target frame")

        legend_layout.addStretch(1)
        return legend

    def _add_legend_item(self, layout: QHBoxLayout, color: str, label: str) -> None:
        swatch = QLabel()
        swatch.setFixedSize(14, 14)
        swatch.setStyleSheet(
            f"background-color: {color}; border: 1px solid #444; border-radius: 3px;"
        )

        text = QLabel(label)
        text.setStyleSheet("color: #333; font-size: 12px;")

        container = QWidget()
        container_layout = QHBoxLayout(container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(6)
        container_layout.addWidget(swatch)
        container_layout.addWidget(text)

        layout.addWidget(container)

    def _create_nodes(self):
        """Creates a graphical node for each unique frame in the TF tree."""
        self.frame_nodes: Dict[str, TFFrameNode] = {}
        path_set = set(self.path_frames)

        # Collect all unique frame names
        all_frames = set(self.tf_tree.keys())
        for parent, children in self.tf_tree.items():
            all_frames.update(children.keys())

        # Create a node for each frame
        for frame in all_frames:
            if frame not in self.frame_nodes:
                node = self.graph.create_node("tf.TFFrameNode", name=frame)

                # Apply special colors to highlight the path
                if frame == self.source_frame:
                    node.set_color(50, 150, 50)  # Green for source
                elif frame == self.target_frame:
                    node.set_color(50, 50, 150)  # Blue for target
                elif frame in path_set:
                    node.set_color(150, 150, 50)  # Yellow for path

                self.frame_nodes[frame] = node

    def _connect_nodes(self):
        """Connects the created nodes based on the TF tree structure."""
        for parent_frame, children in self.tf_tree.items():
            for child_frame in children.keys():
                if parent_frame in self.frame_nodes and child_frame in self.frame_nodes:
                    parent_node = self.frame_nodes[parent_frame]
                    child_node = self.frame_nodes[child_frame]
                    parent_node.output(0).connect_to(child_node.input(0))
