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
Custom tf_transformations implementation using transforms3d.

This replaces the tf_transformations dependency with a minimal implementation
using transforms3d as the backend.
"""

import numpy as np
import transforms3d

# Constants
TRANSLATION_IDENTITY = [0.0, 0.0, 0.0]
ROTATION_IDENTITY = np.identity(3, dtype=np.float64)
ZOOM_IDENTITY = [1.0, 1.0, 1.0]


def identity_matrix():
    """Return 4x4 identity/unit matrix."""
    return transforms3d.affines.compose(TRANSLATION_IDENTITY, ROTATION_IDENTITY, ZOOM_IDENTITY)


def translation_matrix(direction):
    """Return matrix to translate by direction vector."""
    return transforms3d.affines.compose(direction, ROTATION_IDENTITY, ZOOM_IDENTITY)


def translation_from_matrix(matrix):
    """Return translation vector from translation matrix."""
    return transforms3d.affines.decompose(matrix)[0]


def quaternion_matrix(quaternion):
    """Return 4x4 homogeneous rotation matrix from quaternion.

    Quaternion format: [x, y, z, w]
    """
    # transforms3d expects [w, x, y, z] format
    w, x, y, z = quaternion[3], quaternion[0], quaternion[1], quaternion[2]
    rotation_matrix = transforms3d.quaternions.quat2mat([w, x, y, z])
    return transforms3d.affines.compose(TRANSLATION_IDENTITY, rotation_matrix, ZOOM_IDENTITY)


def quaternion_from_matrix(matrix):
    """Return quaternion from rotation matrix.

    Returns quaternion in [x, y, z, w] format.
    """
    rotation_matrix = transforms3d.affines.decompose(matrix)[1]
    w, x, y, z = transforms3d.quaternions.mat2quat(rotation_matrix)
    return np.array([x, y, z, w])


def euler_matrix(ai, aj, ak, axes="sxyz"):
    """Return homogeneous rotation matrix from Euler angles and axis sequence."""
    rotation_matrix = transforms3d.euler.euler2mat(ai, aj, ak, axes=axes)
    return transforms3d.affines.compose(TRANSLATION_IDENTITY, rotation_matrix, ZOOM_IDENTITY)


def euler_from_matrix(matrix, axes="sxyz"):
    """Return Euler angles from rotation matrix for specified axis sequence."""
    rotation_matrix = transforms3d.affines.decompose(matrix)[1]
    return transforms3d.euler.mat2euler(rotation_matrix, axes=axes)


def compose_matrix(scale=None, shear=None, angles=None, translate=None, perspective=None):
    """Return transformation matrix from sequence of transformations."""
    T = translate if translate is not None else TRANSLATION_IDENTITY
    if angles is not None:
        R = transforms3d.euler.euler2mat(*angles)
    else:
        R = ROTATION_IDENTITY
    Z = scale if scale is not None else ZOOM_IDENTITY
    S = shear if shear is not None else [0.0, 0.0, 0.0]
    M = transforms3d.affines.compose(T, R, Z, S)

    # Note: perspective is not implemented in this simplified version
    return M


def decompose_matrix(matrix):
    """Return sequence of transformations from transformation matrix."""
    T, R, Z, S = transforms3d.affines.decompose(matrix)
    angles = transforms3d.euler.mat2euler(R)
    # Return format: scale, shear, angles, translate, perspective
    return Z, S, angles, T, None
