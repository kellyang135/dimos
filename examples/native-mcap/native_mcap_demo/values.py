# Copyright 2026 Dimensional Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import hashlib
from typing import Any

import numpy as np

from dimos.msgs.geometry_msgs.PoseStamped import PoseStamped
from dimos.msgs.geometry_msgs.Vector3 import Vector3
from dimos.msgs.sensor_msgs.Image import Image, ImageFormat
from dimos.msgs.sensor_msgs.Imu import Imu


def values(index: int) -> dict[str, Any]:
    ts = 1_700_000_000.0 + index * 0.2
    return {
        "imu": Imu(ts=ts, frame_id="imu_link", angular_velocity=Vector3(index, 2, 3)),
        "pose": PoseStamped(ts=ts, frame_id="world", position=Vector3(index, 2 * index, 0)),
        "color_image": Image(
            data=np.full((16, 16, 3), [20, 80, 140], dtype=np.uint8),
            format=ImageFormat.RGB,
            ts=ts,
            frame_id="camera",
        ),
    }


def digest(message: Any) -> str:
    return hashlib.sha256(message.lcm_encode()).hexdigest()
