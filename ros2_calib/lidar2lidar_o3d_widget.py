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

import copy
import struct
from typing import Callable

import numpy as np
import open3d as o3d
import open3d.visualization.gui as gui
import open3d.visualization.rendering as rendering
from scipy.spatial.transform import Rotation


class LiDAR2LiDARCalibrationO3D:
    """Open3D GUI-based LiDAR-to-LiDAR calibration interface."""

    def __init__(self, pointcloud_msg, pointcloud2_msg, initial_transform=None, completion_callback: Callable = None):
        self.source_pointcloud_msg = pointcloud_msg
        self.target_pointcloud_msg = pointcloud2_msg
        self.initial_transform = initial_transform if initial_transform is not None else np.eye(4)
        self.current_transform = self.initial_transform.copy()
        self.completion_callback = completion_callback

        # Convert ROS messages to Open3D point clouds
        self.source_cloud = self.ros_to_open3d(pointcloud_msg)
        self.target_cloud = self.ros_to_open3d(pointcloud2_msg)

        # Apply initial transformation to target cloud
        self.target_cloud_transformed = copy.deepcopy(self.target_cloud)
        self.target_cloud_transformed.transform(self.current_transform)

        # Set initial colors before UI setup
        self.update_point_colors()

        # Set up UI
        self.setup_ui()

    def ros_to_open3d(self, pointcloud_msg):
        """Convert ROS PointCloud2 message to Open3D point cloud."""
        points = []

        # Extract point data from ROS message
        point_step = pointcloud_msg.point_step
        data = pointcloud_msg.data

        # Parse fields to find x, y, z offsets
        x_offset = y_offset = z_offset = None
        for field in pointcloud_msg.fields:
            if field.name == "x":
                x_offset = field.offset
            elif field.name == "y":
                y_offset = field.offset
            elif field.name == "z":
                z_offset = field.offset

        # Extract points
        for i in range(0, len(data), point_step):
            if i + point_step <= len(data):
                # Extract x, y, z
                x = struct.unpack_from("f", data, i + x_offset)[0]
                y = struct.unpack_from("f", data, i + y_offset)[0]
                z = struct.unpack_from("f", data, i + z_offset)[0]

                # Skip invalid points
                if not (np.isfinite(x) and np.isfinite(y) and np.isfinite(z)):
                    continue

                points.append([x, y, z])

        # Create Open3D point cloud
        cloud = o3d.geometry.PointCloud()
        cloud.points = o3d.utility.Vector3dVector(np.array(points))

        return cloud

    def setup_ui(self):
        """Setup the Open3D GUI interface."""
        gui.Application.instance.initialize()

        self.window = gui.Application.instance.create_window(
            "LiDAR-to-LiDAR Calibration", 1400, 900
        )

        # Theme and spacing
        em = self.window.theme.font_size
        margin = gui.Margins(0.5 * em, 0.5 * em, 0.5 * em, 0.5 * em)
        separation_height = int(round(0.5 * em))

        # Main scene widget
        self._scene = gui.SceneWidget()
        self._scene.scene = rendering.Open3DScene(self.window.renderer)
        self._scene.scene.set_background([0.1, 0.1, 0.1, 1.0])

        # Control panel
        self._settings_panel = gui.Vert(0, margin)

        # Title
        title_label = gui.Label("LiDAR-to-LiDAR Calibration")
        title_label.text_color = gui.Color(1, 1, 1)
        self._settings_panel.add_child(title_label)
        self._settings_panel.add_fixed(separation_height)

        # Point cloud info
        self.setup_info_section(em, separation_height)

        # Visualization controls
        self.setup_visualization_controls(em, separation_height)

        # ICP controls
        self.setup_icp_controls(em, separation_height)

        # Manual adjustment controls
        self.setup_manual_controls(em, separation_height)


        # Action buttons
        self.setup_action_buttons(em, separation_height)

        # Set up layout
        self.window.set_on_layout(self._on_layout)
        self.window.add_child(self._scene)
        self.window.add_child(self._settings_panel)

        # Initialize scene
        self.update_scene()

        # Force a second update to ensure colors are rendered properly
        gui.Application.instance.post_to_main_thread(self.window, lambda: self.update_scene())

        # Show window
        self.window.set_on_close(self._on_window_close)

    def setup_info_section(self, em, separation_height):
        """Setup point cloud information section."""
        info_section = gui.CollapsableVert("Point Cloud Information", 0.25 * em,
                                          gui.Margins(em, 0, 0, 0))

        source_info = gui.Label(f"Source: {len(self.source_cloud.points)} points")
        target_info = gui.Label(f"Target: {len(self.target_cloud.points)} points")

        info_section.add_child(source_info)
        info_section.add_child(target_info)

        self._settings_panel.add_child(info_section)
        self._settings_panel.add_fixed(separation_height)

    def setup_visualization_controls(self, em, separation_height):
        """Setup visualization control section."""
        vis_section = gui.CollapsableVert("Visualization", 0.25 * em,
                                         gui.Margins(em, 0, 0, 0))

        # Checkboxes for showing clouds
        self._show_source = gui.Checkbox("Show Source Cloud")
        self._show_source.checked = True
        self._show_source.set_on_checked(self._on_show_source_changed)
        vis_section.add_child(self._show_source)

        self._show_target = gui.Checkbox("Show Target Cloud")
        self._show_target.checked = True
        self._show_target.set_on_checked(self._on_show_target_changed)
        vis_section.add_child(self._show_target)

        self._settings_panel.add_child(vis_section)
        self._settings_panel.add_fixed(separation_height)

    def setup_icp_controls(self, em, separation_height):
        """Setup ICP control section."""
        icp_section = gui.CollapsableVert("Automatic Registration", 0.25 * em,
                                         gui.Margins(em, 0, 0, 0))

        # ICP parameters
        grid = gui.VGrid(2, 0.25 * em)

        grid.add_child(gui.Label("Max Distance:"))
        self._max_distance = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._max_distance.double_value = 0.5
        grid.add_child(self._max_distance)

        grid.add_child(gui.Label("Max Iterations:"))
        self._max_iterations = gui.NumberEdit(gui.NumberEdit.INT)
        self._max_iterations.int_value = 50
        grid.add_child(self._max_iterations)

        icp_section.add_child(grid)

        # ICP buttons
        button_layout = gui.Horiz(0.25 * em)

        self._icp_p2p_button = gui.Button("Point-to-Point ICP")
        self._icp_p2p_button.set_on_clicked(self._on_icp_p2p)
        button_layout.add_child(self._icp_p2p_button)

        self._icp_p2plane_button = gui.Button("Point-to-Plane ICP")
        self._icp_p2plane_button.set_on_clicked(self._on_icp_p2plane)
        button_layout.add_child(self._icp_p2plane_button)

        icp_section.add_child(button_layout)

        # ICP result display
        self._icp_result = gui.Label("")
        icp_section.add_child(self._icp_result)

        self._settings_panel.add_child(icp_section)
        self._settings_panel.add_fixed(separation_height)

    def setup_manual_controls(self, em, separation_height):
        """Setup manual adjustment controls."""
        manual_section = gui.CollapsableVert("Manual Adjustment", 0.25 * em,
                                            gui.Margins(em, 0, 0, 0))

        # Translation and rotation controls in table format
        # Translation section
        trans_grid = gui.VGrid(4, 0.25 * em)  # 4 columns: label, X, Y, Z

        trans_grid.add_child(gui.Label(""))  # Empty cell
        trans_grid.add_child(gui.Label("X (m)"))
        trans_grid.add_child(gui.Label("Y (m)"))
        trans_grid.add_child(gui.Label("Z (m)"))

        trans_grid.add_child(gui.Label("Value:"))
        self._tx = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._tx.double_value = 0.0
        self._tx.set_on_value_changed(self._on_manual_transform_changed)
        trans_grid.add_child(self._tx)

        self._ty = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._ty.double_value = 0.0
        self._ty.set_on_value_changed(self._on_manual_transform_changed)
        trans_grid.add_child(self._ty)

        self._tz = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._tz.double_value = 0.0
        self._tz.set_on_value_changed(self._on_manual_transform_changed)
        trans_grid.add_child(self._tz)

        manual_section.add_child(trans_grid)

        # Rotation section
        rot_grid = gui.VGrid(4, 0.25 * em)  # 4 columns: label, Roll, Pitch, Yaw

        rot_grid.add_child(gui.Label(""))  # Empty cell
        rot_grid.add_child(gui.Label("Roll (rad)"))
        rot_grid.add_child(gui.Label("Pitch (rad)"))
        rot_grid.add_child(gui.Label("Yaw (rad)"))

        rot_grid.add_child(gui.Label("Value:"))
        self._rx = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._rx.double_value = 0.0
        self._rx.set_on_value_changed(self._on_manual_transform_changed)
        rot_grid.add_child(self._rx)

        self._ry = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._ry.double_value = 0.0
        self._ry.set_on_value_changed(self._on_manual_transform_changed)
        rot_grid.add_child(self._ry)

        self._rz = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._rz.double_value = 0.0
        self._rz.set_on_value_changed(self._on_manual_transform_changed)
        rot_grid.add_child(self._rz)

        manual_section.add_child(rot_grid)

        # Step size controls
        manual_section.add_child(gui.Label("Step Sizes:"))
        step_grid = gui.VGrid(2, 0.25 * em)

        step_grid.add_child(gui.Label("Translation (m):"))
        self._trans_step = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._trans_step.double_value = 0.01
        step_grid.add_child(self._trans_step)

        step_grid.add_child(gui.Label("Rotation (rad):"))
        self._rot_step = gui.NumberEdit(gui.NumberEdit.DOUBLE)
        self._rot_step.double_value = 0.001
        step_grid.add_child(self._rot_step)

        manual_section.add_child(step_grid)

        # Quick adjustment buttons in two columns
        manual_section.add_child(gui.Label("Quick Adjustments:"))

        # Create two-column layout for adjustment buttons
        button_columns = gui.Horiz(0.5 * em)

        # Left column: Translation (XYZ)
        trans_column = gui.Vert(0.1 * em)
        trans_column.add_child(gui.Label("Translation:"))

        # X buttons
        x_buttons = gui.Horiz(0.1 * em)
        x_buttons.add_child(gui.Label("X:"))
        btn_x_minus = gui.Button("-")
        btn_x_minus.set_on_clicked(lambda: self._adjust_translation('x', -self._trans_step.double_value))
        x_buttons.add_child(btn_x_minus)
        btn_x_plus = gui.Button("+")
        btn_x_plus.set_on_clicked(lambda: self._adjust_translation('x', self._trans_step.double_value))
        x_buttons.add_child(btn_x_plus)
        trans_column.add_child(x_buttons)

        # Y buttons
        y_buttons = gui.Horiz(0.1 * em)
        y_buttons.add_child(gui.Label("Y:"))
        btn_y_minus = gui.Button("-")
        btn_y_minus.set_on_clicked(lambda: self._adjust_translation('y', -self._trans_step.double_value))
        y_buttons.add_child(btn_y_minus)
        btn_y_plus = gui.Button("+")
        btn_y_plus.set_on_clicked(lambda: self._adjust_translation('y', self._trans_step.double_value))
        y_buttons.add_child(btn_y_plus)
        trans_column.add_child(y_buttons)

        # Z buttons
        z_buttons = gui.Horiz(0.1 * em)
        z_buttons.add_child(gui.Label("Z:"))
        btn_z_minus = gui.Button("-")
        btn_z_minus.set_on_clicked(lambda: self._adjust_translation('z', -self._trans_step.double_value))
        z_buttons.add_child(btn_z_minus)
        btn_z_plus = gui.Button("+")
        btn_z_plus.set_on_clicked(lambda: self._adjust_translation('z', self._trans_step.double_value))
        z_buttons.add_child(btn_z_plus)
        trans_column.add_child(z_buttons)

        button_columns.add_child(trans_column)

        # Right column: Rotation (RPY)
        rot_column = gui.Vert(0.1 * em)
        rot_column.add_child(gui.Label("Rotation:"))

        # Roll buttons
        roll_buttons = gui.Horiz(0.1 * em)
        roll_buttons.add_child(gui.Label("Roll:"))
        btn_roll_minus = gui.Button("-")
        btn_roll_minus.set_on_clicked(lambda: self._adjust_rotation('roll', -self._rot_step.double_value))
        roll_buttons.add_child(btn_roll_minus)
        btn_roll_plus = gui.Button("+")
        btn_roll_plus.set_on_clicked(lambda: self._adjust_rotation('roll', self._rot_step.double_value))
        roll_buttons.add_child(btn_roll_plus)
        rot_column.add_child(roll_buttons)

        # Pitch buttons
        pitch_buttons = gui.Horiz(0.1 * em)
        pitch_buttons.add_child(gui.Label("Pitch:"))
        btn_pitch_minus = gui.Button("-")
        btn_pitch_minus.set_on_clicked(lambda: self._adjust_rotation('pitch', -self._rot_step.double_value))
        pitch_buttons.add_child(btn_pitch_minus)
        btn_pitch_plus = gui.Button("+")
        btn_pitch_plus.set_on_clicked(lambda: self._adjust_rotation('pitch', self._rot_step.double_value))
        pitch_buttons.add_child(btn_pitch_plus)
        rot_column.add_child(pitch_buttons)

        # Yaw buttons
        yaw_buttons = gui.Horiz(0.1 * em)
        yaw_buttons.add_child(gui.Label("Yaw:"))
        btn_yaw_minus = gui.Button("-")
        btn_yaw_minus.set_on_clicked(lambda: self._adjust_rotation('yaw', -self._rot_step.double_value))
        yaw_buttons.add_child(btn_yaw_minus)
        btn_yaw_plus = gui.Button("+")
        btn_yaw_plus.set_on_clicked(lambda: self._adjust_rotation('yaw', self._rot_step.double_value))
        yaw_buttons.add_child(btn_yaw_plus)
        rot_column.add_child(yaw_buttons)

        button_columns.add_child(rot_column)

        manual_section.add_child(button_columns)

        self._settings_panel.add_child(manual_section)
        self._settings_panel.add_fixed(separation_height)


    def setup_action_buttons(self, em, separation_height):
        """Setup action buttons."""
        # Create a vertical layout for both action buttons
        action_section = gui.CollapsableVert("Actions", 0.25 * em,
                                            gui.Margins(em, 0, 0, 0))

        # Add the reset button first (full width)
        reset_button = gui.Button("Reset to Initial Transform")
        reset_button.set_on_clicked(self._on_reset_transform)
        action_section.add_child(reset_button)

        # Add some spacing between buttons
        action_section.add_fixed(int(0.25 * em))

        # Add the export button below (full width)
        self._finish_button = gui.Button("Export Calibration")
        self._finish_button.set_on_clicked(self._on_finish)
        action_section.add_child(self._finish_button)

        self._settings_panel.add_child(action_section)

        # Update initial displays
        self.update_manual_controls_from_transform()

    def _on_layout(self, layout_context):
        """Handle window layout."""
        r = self.window.content_rect
        self._scene.frame = r

        # Settings panel on the right
        width = 22 * layout_context.theme.font_size
        height = min(
            r.height,
            self._settings_panel.calc_preferred_size(
                layout_context, gui.Widget.Constraints()
            ).height
        )
        self._settings_panel.frame = gui.Rect(
            r.get_right() - width, r.y, width, height
        )

    def _on_show_source_changed(self, checked):
        """Handle source cloud visibility change."""
        self.update_scene()

    def _on_show_target_changed(self, checked):
        """Handle target cloud visibility change."""
        self.update_scene()

    def _on_icp_p2p(self):
        """Run point-to-point ICP."""
        threshold = self._max_distance.double_value
        max_iteration = self._max_iterations.int_value

        reg_p2p = o3d.pipelines.registration.registration_icp(
            self.target_cloud_transformed,
            self.source_cloud,
            threshold,
            np.eye(4),
            o3d.pipelines.registration.TransformationEstimationPointToPoint(),
            o3d.pipelines.registration.ICPConvergenceCriteria(max_iteration=max_iteration),
        )

        # Update transformation
        icp_transform = reg_p2p.transformation
        self.current_transform = np.dot(self.current_transform, icp_transform)

        # Update UI
        self.update_manual_controls_from_transform()
        self.apply_current_transform()

        # Display results
        result_text = f"P2P ICP: Fitness={reg_p2p.fitness:.4f}, RMSE={reg_p2p.inlier_rmse:.4f}"
        self._icp_result.text = result_text

    def _on_icp_p2plane(self):
        """Run point-to-plane ICP."""
        threshold = self._max_distance.double_value
        max_iteration = self._max_iterations.int_value

        # Estimate normals for the source cloud
        source_with_normals = copy.deepcopy(self.source_cloud)
        source_with_normals.estimate_normals()

        reg_p2plane = o3d.pipelines.registration.registration_icp(
            self.target_cloud_transformed,
            source_with_normals,
            threshold,
            np.eye(4),
            o3d.pipelines.registration.TransformationEstimationPointToPlane(),
            o3d.pipelines.registration.ICPConvergenceCriteria(max_iteration=max_iteration),
        )

        # Update transformation
        icp_transform = reg_p2plane.transformation
        self.current_transform = np.dot(self.current_transform, icp_transform)

        # Update UI
        self.update_manual_controls_from_transform()
        self.apply_current_transform()

        # Display results
        result_text = f"P2Plane ICP: Fitness={reg_p2plane.fitness:.4f}, RMSE={reg_p2plane.inlier_rmse:.4f}"
        self._icp_result.text = result_text

    def _on_manual_transform_changed(self, value):
        """Handle manual transform control changes."""
        # Create transformation matrix from manual controls
        tx, ty, tz = self._tx.double_value, self._ty.double_value, self._tz.double_value
        rx, ry, rz = self._rx.double_value, self._ry.double_value, self._rz.double_value

        # Create transformation matrix
        rotation_matrix = Rotation.from_euler("xyz", [rx, ry, rz]).as_matrix()
        self.current_transform = np.eye(4)
        self.current_transform[:3, :3] = rotation_matrix
        self.current_transform[:3, 3] = [tx, ty, tz]

        # Update visualization and display
        self.apply_current_transform()

    def _adjust_translation(self, axis, delta):
        """Adjust translation by delta amount."""
        if axis == 'x':
            self._tx.double_value += delta
        elif axis == 'y':
            self._ty.double_value += delta
        elif axis == 'z':
            self._tz.double_value += delta
        self._on_manual_transform_changed(0)

    def _adjust_rotation(self, axis, delta):
        """Adjust rotation by delta amount."""
        if axis == 'roll':
            new_value = self._rx.double_value + delta
            self._rx.double_value = ((new_value + np.pi) % (2 * np.pi)) - np.pi
        elif axis == 'pitch':
            new_value = self._ry.double_value + delta
            self._ry.double_value = ((new_value + np.pi) % (2 * np.pi)) - np.pi
        elif axis == 'yaw':
            new_value = self._rz.double_value + delta
            self._rz.double_value = ((new_value + np.pi) % (2 * np.pi)) - np.pi
        self._on_manual_transform_changed(0)

    def _on_reset_transform(self):
        """Reset to initial transformation."""
        self.current_transform = self.initial_transform.copy()
        self.update_manual_controls_from_transform()
        self.apply_current_transform()

    def _on_finish(self):
        """Finish calibration and return to main window with results."""
        if self.completion_callback:
            self.completion_callback(self.current_transform)
        # Close the window to exit the Open3D application
        self.window.close()
        # Also terminate the application to prevent threading issues
        try:
            gui.Application.instance.quit()
        except:
            pass  # Ignore errors if already closing

    def _on_window_close(self):
        """Handle window close."""
        # Properly terminate the Open3D application
        try:
            gui.Application.instance.quit()
        except:
            pass  # Ignore errors if already closing
        return True

    def apply_current_transform(self):
        """Apply current transformation to target cloud."""
        # Reset target cloud to original position
        self.target_cloud_transformed = copy.deepcopy(self.target_cloud)
        # Apply transformation
        self.target_cloud_transformed.transform(self.current_transform)
        # Intensity data is stored separately, no need to preserve it on the cloud object
        # Update colors
        self.update_point_colors()
        # Refresh visualization
        self.update_scene()

    def update_manual_controls_from_transform(self):
        """Update manual controls from current transformation matrix."""
        # Extract translation
        translation = self.current_transform[:3, 3]
        self._tx.double_value = translation[0]
        self._ty.double_value = translation[1]
        self._tz.double_value = translation[2]

        # Extract rotation
        rotation_matrix = self.current_transform[:3, :3]
        euler_angles = Rotation.from_matrix(rotation_matrix).as_euler("xyz")
        self._rx.double_value = euler_angles[0]
        self._ry.double_value = euler_angles[1]
        self._rz.double_value = euler_angles[2]


    def update_point_colors(self):
        """Update point cloud colors with uniform colors."""
        # Use uniform colors - blue for source, orange for target
        self.source_cloud.paint_uniform_color([0, 0.651, 0.929])  # Blue
        self.target_cloud_transformed.paint_uniform_color([1, 0.706, 0])  # Orange

    def update_scene(self):
        """Update the 3D scene without resetting camera."""
        # Remove existing geometries by name instead of clearing all
        try:
            self._scene.scene.remove_geometry("source_cloud")
        except:
            pass
        try:
            self._scene.scene.remove_geometry("target_cloud")
        except:
            pass

        # Add geometries based on checkboxes
        if self._show_source.checked:
            mat = rendering.MaterialRecord()
            mat.shader = "defaultUnlit"
            mat.point_size = 2.0
            self._scene.scene.add_geometry("source_cloud", self.source_cloud, mat)

        if self._show_target.checked:
            mat = rendering.MaterialRecord()
            mat.shader = "defaultUnlit"
            mat.point_size = 2.0
            self._scene.scene.add_geometry("target_cloud", self.target_cloud_transformed, mat)

        # Set up camera only on first update
        if not hasattr(self, '_camera_initialized'):
            self._camera_initialized = True
            bounds = self._scene.scene.bounding_box
            if not bounds.is_empty():
                self._scene.setup_camera(60, bounds, bounds.get_center())


    def run(self):
        """Run the calibration interface."""
        gui.Application.instance.run()


def launch_lidar2lidar_calibration(pointcloud_msg, pointcloud2_msg, initial_transform=None, completion_callback=None):
    """Launch the LiDAR-to-LiDAR calibration interface."""
    app = LiDAR2LiDARCalibrationO3D(pointcloud_msg, pointcloud2_msg, initial_transform, completion_callback)
    app.run()
    return app.current_transform
