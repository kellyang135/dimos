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

"""Validate an actual CLI Rust recording and optionally its received replay."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np

from dimos.memory.cli.dataset import open_dataset
from dimos.memory.type.observation import Observation
from dimos.msgs.geometry_msgs.PoseStamped import PoseStamped
from dimos.msgs.sensor_msgs.Image import Image
from dimos.msgs.sensor_msgs.Imu import Imu
from native_mcap_demo.values import digest, values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording", type=Path)
    parser.add_argument("--replay-result", type=Path)
    args = parser.parse_args()
    expected: dict[str, list[str]] = {}
    with open_dataset(args.recording) as store:
        for name in store.list_streams():
            observations: list[Observation[Any]] = store.stream(name).to_list()
            assert observations, f"empty stream: {name}"
            expected[name] = []
            for observation in observations:
                message = observation.data
                index = round((message.ts - 1_700_000_000.0) / 0.2)
                source = values(index)
                assert abs(observation.ts - message.ts) < 1e-6
                if name.endswith("color_image"):
                    assert isinstance(message, Image)
                    assert message.frame_id == "camera"
                    assert message.data.shape == (16, 16, 3)
                    assert (
                        np.abs(message.data.astype(float) - source["color_image"].data).mean() < 5
                    )
                elif name.endswith("imu"):
                    assert isinstance(message, Imu)
                    assert message.lcm_encode() == source["imu"].lcm_encode()
                elif name.endswith("pose"):
                    assert isinstance(message, PoseStamped)
                    assert message.lcm_encode() == source["pose"].lcm_encode()
                else:
                    raise AssertionError(f"unexpected stream: {name}")
                expected[name].append(digest(message))
        assert len(expected) == 3
    if args.replay_result:
        assert json.loads(args.replay_result.read_text()) == expected, "replay differs from MCAP"
    print(
        "PASS: typed data/source timestamps", {name: len(rows) for name, rows in expected.items()}
    )
    if args.replay_result:
        print("PASS: every replayed payload matches the recording in order")


if __name__ == "__main__":
    main()
