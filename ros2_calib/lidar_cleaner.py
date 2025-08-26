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
Inspired by the RePLAy paper:
@inproceedings{zhu2024replay,
  title={RePLAy: Remove Projective LiDAR Depthmap Artifacts via Exploiting Epipolar Geometry},
  author={Zhu, Shengjie and Ganesan, Girish Chandar and Kumar, Abhinav and Liu, Xiaoming},
  booktitle={ECCV},
  year={2024},
}
"""

import numpy as np
from scipy.interpolate import NearestNDInterpolator
from scipy.ndimage import map_coordinates


class LiDARCleaner:
    """
    A NumPy/SciPy-based class for cleaning LiDAR point cloud data by detecting and removing
    occluded points when projected onto a camera image.
    """

    def __init__(
        self, intrinsic_cam, extrinsic_LiDAR2Cam, LiDARPoints3D, height, width, rszh=0.5, rszw=1.0
    ):
        self.intrinsic_cam = np.asarray(intrinsic_cam, dtype=np.float32)
        self.extrinsic_LiDAR2Cam = np.asarray(extrinsic_LiDAR2Cam, dtype=np.float32)

        npts = LiDARPoints3D.shape[1]
        self.LiDARPoints3D = np.vstack(
            [np.asarray(LiDARPoints3D, dtype=np.float32), np.ones((1, npts), dtype=np.float32)]
        )

        self.height_rz, self.width_rz = int(height * rszh), int(width * rszw)
        self.height, self.width = int(height), int(width)

        self.resizeM = np.eye(3, dtype=np.float32)
        self.resizeM[0, 0] = self.width_rz / self.width
        self.resizeM[1, 1] = self.height_rz / self.height

    def prj(self, intrinsic, extrinsic, pc3D, height, width, min_dist=0.1):
        prjpc = intrinsic @ extrinsic @ pc3D
        depth = prjpc[2, :]

        # Avoid division by zero
        depth[depth == 0] = 1e-8

        prjpc[0, :] = prjpc[0, :] / depth
        prjpc[1, :] = prjpc[1, :] / depth

        visible_sel = (
            (depth > min_dist)
            & (prjpc[0, :] > 0.5)
            & (prjpc[0, :] < width - 0.5)
            & (prjpc[1, :] > 0.5)
            & (prjpc[1, :] < height - 0.5)
        )

        return prjpc[0:2, :], depth, visible_sel

    def inpainting_depth(self, visible_cam):
        pure_rotation = self.extrinsic_LiDAR2Cam.copy()
        pure_rotation[0:3, 3] = 0.0

        grid_x, grid_y = np.meshgrid(np.arange(self.width_rz), np.arange(self.height_rz))

        prjpc, depths, visible_points = self.prj(
            self.resizeM @ self.intrinsic_cam,
            pure_rotation,
            self.LiDARPoints3D,
            height=self.height_rz,
            width=self.width_rz,
        )
        visible_points = visible_points & visible_cam

        prjpc_val = prjpc[:, visible_points].T
        depths_val = depths[visible_points]

        if prjpc_val.shape[0] <= 100:
            print("Warning: Insufficient valid points for depth interpolation.")
            return None, None, None, None

        nearest_func = NearestNDInterpolator(prjpc_val, depths_val)
        inpainted_depth = nearest_func(grid_x, grid_y)

        return inpainted_depth, prjpc, depths, visible_points

    def pad_pose44(self, pose34):
        pose = np.eye(4, dtype=np.float32)
        pose[0:3, :] = pose34
        return pose

    def pad_intrinsic44(self, intrinsic33):
        intrinsic = np.eye(4, dtype=np.float32)
        intrinsic[0:3, 0:3] = intrinsic33
        return intrinsic

    def epplinedir(self, prjpc_vlidar):
        intrinsic_cam_scaled = self.resizeM @ self.intrinsic_cam

        pure_rotation_44 = self.pad_pose44(self.extrinsic_LiDAR2Cam)
        pure_rotation_44[0:3, 3] = 0

        pure_translation_44 = self.pad_pose44(self.extrinsic_LiDAR2Cam) @ np.linalg.inv(
            pure_rotation_44
        )

        epipole = -intrinsic_cam_scaled @ pure_translation_44[0:3, 3:4]
        epipole /= epipole[2, 0] + 1e-8
        eppx, eppy = epipole[0, 0], epipole[1, 0]

        eppdir = np.array([eppx - prjpc_vlidar[0, :], eppy - prjpc_vlidar[1, :]])
        norm = np.linalg.norm(eppdir, axis=0)
        norm[norm == 0] = 1e-8
        eppdir /= norm
        eppdir = eppdir.T

        return eppdir, pure_translation_44[0:3, :]

    def interpolated_depth_np(self, depth, querylocation):
        h, w = depth.shape
        qx, qy = querylocation[..., 0], querylocation[..., 1]
        coords = np.stack([qy.ravel(), qx.ravel()])
        querydepth = map_coordinates(depth, coords, order=0, mode="constant", cval=1e5)
        return querydepth.reshape(qx.shape)

    def backprj_prj_np(self, intrinsic, pure_translation, enumlocation, depthinterp):
        intrinsic44 = self.pad_intrinsic44(intrinsic)  # CORRECTED
        pure_translation44 = self.pad_pose44(pure_translation)

        prjM = intrinsic44 @ pure_translation44 @ np.linalg.inv(intrinsic44)

        nquery, nsample, _ = enumlocation.shape
        qx, qy = enumlocation[..., 0], enumlocation[..., 1]

        pts3D_h = np.stack(
            [qx * depthinterp, qy * depthinterp, depthinterp, np.ones_like(depthinterp)], axis=-1
        )

        pts3D_h = pts3D_h.reshape(-1, 4).T
        pts3Dprj_h = prjM @ pts3D_h

        pts3Dprj_h[2, pts3Dprj_h[2, :] == 0] = 1e-8

        pts3Dprjx = (pts3Dprj_h[0, :] / pts3Dprj_h[2, :]).reshape(nquery, nsample)
        pts3Dprjy = (pts3Dprj_h[1, :] / pts3Dprj_h[2, :]).reshape(nquery, nsample)

        return np.stack([pts3Dprjx, pts3Dprjy], axis=-1)

    def clean(
        self,
        intrinsic,
        pure_translation,
        depthmap,
        prjpc_lidar,
        prjpc_cam,
        eppdir,
        selector,
        srch_resolution=0.5,
    ):
        prjpc_lidar_, prjpc_cam_ = prjpc_lidar[selector, :], prjpc_cam[selector, :]
        eppdir_ = eppdir[selector, :]

        mindist, maxdist = 1, 100
        samplenum = int(np.ceil((maxdist - mindist) / srch_resolution).item() + 1)
        sampled_range = np.linspace(mindist, maxdist, samplenum, dtype=np.float32)

        nanchor = len(eppdir_)
        enumlocation = (
            prjpc_lidar_[:, np.newaxis, :]
            + sampled_range[np.newaxis, :, np.newaxis] * eppdir_[:, np.newaxis, :]
        )

        depthinterp = self.interpolated_depth_np(depthmap, enumlocation)

        pts3Dprj = self.backprj_prj_np(intrinsic, pure_translation, enumlocation, depthinterp)

        prj_dir = pts3Dprj - prjpc_cam_[:, np.newaxis, :]
        norm = np.linalg.norm(prj_dir, axis=2)
        norm[norm == 0] = 1e-8
        prj_dir /= norm[..., np.newaxis]

        cosdiff = np.sum(prj_dir * eppdir_[:, np.newaxis, :], axis=2)
        cosdiffmax = np.min(cosdiff, axis=1)
        occluded = cosdiffmax < 0

        return occluded

    def run(self):
        _, _, visible_points = self.prj(
            self.intrinsic_cam,
            self.extrinsic_LiDAR2Cam,
            self.LiDARPoints3D,
            height=self.height,
            width=self.width,
        )

        inpainted_depth, prjpc_vlidar, _, visible_sel_vlidar = self.inpainting_depth(
            visible_cam=visible_points
        )

        if inpainted_depth is None:
            return visible_points

        eppdir, pure_translation = self.epplinedir(prjpc_vlidar)

        prjpc_cam, _, _ = self.prj(
            self.resizeM @ self.intrinsic_cam,
            self.extrinsic_LiDAR2Cam,
            self.LiDARPoints3D,
            height=self.height_rz,
            width=self.width_rz,
        )

        occluded = self.clean(
            self.resizeM @ self.intrinsic_cam,
            pure_translation,
            inpainted_depth,
            prjpc_vlidar.T,
            prjpc_cam.T,
            eppdir,
            visible_sel_vlidar,
            srch_resolution=1.0,
        )

        tomask = np.zeros_like(occluded, dtype=bool)
        tomask[occluded] = True

        tomask_all = np.zeros_like(visible_sel_vlidar, dtype=bool)
        tomask_all[visible_sel_vlidar] = tomask

        visible_points_filtered = visible_points.copy()
        visible_points_filtered[tomask_all] = False

        return visible_points_filtered
