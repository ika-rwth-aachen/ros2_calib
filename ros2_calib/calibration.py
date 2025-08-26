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
from scipy.optimize import least_squares
from scipy.spatial.transform import Rotation


def objective_function(params, points_3d, points_2d, K):
    rvec = params[:3]
    tvec = params[3:]

    points_proj, _ = cv2.projectPoints(points_3d, rvec, tvec, K, None)
    error = (points_proj.reshape(-1, 2) - points_2d).ravel()
    return error


def calibrate(
    correspondences, K, pnp_method=cv2.SOLVEPNP_ITERATIVE, lsq_method="lm", lsq_verbose=2
):
    print("---" + "-" * 10 + " Starting Calibration " + "-" * 10 + "---")
    if len(correspondences) < 4:
        print("Error: Need at least 4 correspondences for calibration.")
        return np.identity(4)

    points_3d = np.array([c[1] for c in correspondences], dtype=np.float32)
    points_2d = np.array([c[0] for c in correspondences], dtype=np.float32)

    initial_params = np.zeros(6)

    if pnp_method is not None:
        print(f"Running RANSAC on {len(points_2d)} correspondences...")
        try:
            # Use solvePnPRansac to get a robust initial estimate and identify inliers
            success, rvec_ransac, tvec_ransac, inliers = cv2.solvePnPRansac(
                points_3d,
                points_2d,
                K,
                None,
                iterationsCount=100,
                reprojectionError=8.0,
                flags=pnp_method,
            )
            if not success:
                print("RANSAC failed to find a solution.")
                return np.identity(4)

            inlier_count = len(inliers) if inliers is not None else 0
            print(f"RANSAC found {inlier_count} inliers out of {len(points_2d)} points.")

            if inlier_count < 4:
                print("Not enough inliers found by RANSAC.")
                return np.identity(4)

            # Refine the pose using only the inliers
            inlier_points_3d = points_3d[inliers.ravel()]
            inlier_points_2d = points_2d[inliers.ravel()]

            # Initial parameters for least_squares from RANSAC result
            initial_params = np.concatenate((rvec_ransac.ravel(), tvec_ransac.ravel()))
            points_3d = inlier_points_3d
            points_2d = inlier_points_2d

        except cv2.error as e:
            print(f"An OpenCV error occurred during RANSAC: {e}")
            print("Falling back to simple least squares with all points.")

    print("\nRefining pose with inliers using least squares optimization...")
    res = least_squares(
        objective_function,
        initial_params,
        args=(points_3d, points_2d, K),
        method=lsq_method,
        verbose=lsq_verbose,
    )

    # --- Convert result to 4x4 matrix ---
    rvec_opt = res.x[:3]
    tvec_opt = res.x[3:]
    R_opt, _ = cv2.Rodrigues(rvec_opt)

    extrinsics = np.identity(4)
    extrinsics[:3, :3] = R_opt
    extrinsics[:3, 3] = tvec_opt.ravel()

    print("\n---" + "-" * 10 + " Calibration Finished " + "-" * 10 + "---")
    rpy = Rotation.from_matrix(R_opt).as_euler("xyz", degrees=True)
    print(f"Translation (x, y, z): {tvec_opt[0]:.4f}, {tvec_opt[1]:.4f}, {tvec_opt[2]:.4f}")
    print(f"Rotation (roll, pitch, yaw): {rpy[0]:.4f}, {rpy[1]:.4f}, {rpy[2]:.4f}")
    print("Final Extrinsic Matrix:")
    print(extrinsics)

    return extrinsics
