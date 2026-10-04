# Copyright 2025-2026 Dimensional Inc.
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

"""Tests for ``ConnectionSource``: readings in, complete commands out."""

from __future__ import annotations

import time

import pytest

from dimos.control.connection_source import ConnectionSource
from dimos.control.contract.description import ControlDescription, Limits
from dimos.control.contract.keys import POSITION, VELOCITY, VX, VY, WZ, Key
from dimos.control.contract.presets import (
    imu_resource,
    manipulator_description,
    twist_base_description,
)
from dimos.hardware.manipulators.spec import ControlMode
from dimos.msgs.control_msgs.ControlValues import ControlValues

J1 = "arm/joint1"
J2 = "arm/joint2"


def arm(**kwargs: object) -> ControlDescription:
    """A two-joint arm that reports position and velocity, limited to +-1 rad."""
    joints = ["joint1", "joint2"]
    settings: dict[str, object] = {
        "limits": {Key.of("arm", j, POSITION): Limits(-1.0, 1.0) for j in joints},
        "state": (POSITION, VELOCITY),
        "command": (POSITION, VELOCITY),
    }
    settings.update(kwargs)
    return manipulator_description("arm", joints, **settings)  # type: ignore[arg-type]


def reading(
    description: ControlDescription, values: dict[str, float] | None = None, sequence: int = 0
) -> ControlValues:
    """A full reading from the robot: every key it reports, 0.0 unless given."""
    given = values or {}
    keys = description.state_keys()
    return ControlValues(
        source=description.source,
        sequence=sequence,
        interface_names=list(keys),
        values=[given.get(key, 0.0) for key in keys],
    )


def source_at(
    description: ControlDescription, values: dict[str, float] | None = None
) -> ConnectionSource:
    """A source that has had one reading."""
    source = ConnectionSource(description)
    source.on_state(reading(description, values))
    return source


def test_joints_and_sensors_come_from_a_reading() -> None:
    description = manipulator_description(
        "arm", ["joint1"], state=(POSITION,), command=(POSITION,), sensors=[imu_resource()]
    )
    source = source_at(description, {"arm/joint1/position": 0.3, "arm/imu/qw": 1.0})

    joints = source.read_joints()
    assert joints.joint_positions == {J1: 0.3}
    # Not reported, so left out rather than read as 0.0.
    assert joints.get_velocity(J1) is None
    assert joints.joint_efforts == {}
    sensors = source.read_sensors()
    assert list(sensors) == ["arm/imu"]
    assert sensors["arm/imu"]["qw"] == 1.0
    assert len(sensors["arm/imu"]) == 10


def test_not_ready_until_the_first_reading() -> None:
    source = ConnectionSource(arm())
    assert not source.ready_for_control()
    assert source.read_joints().joint_positions == {}

    source.on_state(reading(arm()))
    assert source.ready_for_control()


def test_a_reading_that_does_not_match_is_dropped() -> None:
    description = arm()
    source = source_at(description, {"arm/joint1/position": 0.3})

    missing = ControlValues(source="arm", interface_names=[f"{J1}/position"], values=[0.9])
    source.on_state(missing)
    other_robot = reading(description, {"arm/joint1/position": 0.9})
    other_robot.source = "other"
    source.on_state(other_robot)

    assert source.read_joints().joint_positions[J1] == 0.3


def test_with_no_task_every_joint_is_held_where_it_is() -> None:
    source = source_at(arm(), {"arm/joint1/position": 0.3, "arm/joint2/position": -0.2})

    assert source.command({}, None) == {f"{J1}/position": 0.3, f"{J2}/position": -0.2}


def test_task_values_are_sent_and_the_other_joints_held() -> None:
    description = arm()
    source = source_at(description, {"arm/joint1/position": 0.3, "arm/joint2/position": -0.2})

    assert source.command({J1: 0.5}, ControlMode.POSITION) == {
        f"{J1}/position": 0.5,
        f"{J2}/position": -0.2,
    }

    # Once the task stops, the joint stays at what it was last told, not
    # wherever it has got to.
    source.on_state(reading(description, {"arm/joint1/position": 0.45}))
    assert source.command({}, None)[f"{J1}/position"] == 0.5


def test_a_velocity_tick_zeroes_the_other_joints_then_holds_where_they_stopped() -> None:
    description = arm()
    source = source_at(description, {"arm/joint1/position": 0.3, "arm/joint2/position": -0.2})
    source.command({J1: 0.1}, ControlMode.POSITION)

    assert source.command({J1: 0.2}, ControlMode.VELOCITY) == {
        f"{J1}/velocity": 0.2,
        f"{J2}/velocity": 0.0,
    }

    source.on_state(reading(description, {"arm/joint1/position": 0.6, "arm/joint2/position": -0.1}))
    assert source.command({}, None) == {f"{J1}/position": 0.6, f"{J2}/position": -0.1}


def test_a_held_position_stays_inside_the_limit_but_task_values_are_sent_as_is() -> None:
    source = source_at(arm(), {"arm/joint1/position": 1.0001})

    assert source.command({}, None)[f"{J1}/position"] == 1.0
    assert source.command({J1: 1.5}, ControlMode.POSITION)[f"{J1}/position"] == 1.5
    assert source.command({}, None)[f"{J1}/position"] == 1.0


def test_a_joint_that_cannot_take_a_position_gets_zero_velocity() -> None:
    source = source_at(arm(command=(VELOCITY,), limits={}), {"arm/joint1/position": 0.3})

    assert source.command({}, None) == {f"{J1}/velocity": 0.0, f"{J2}/velocity": 0.0}


def test_a_base_is_told_zero_on_every_axis() -> None:
    limits: dict[str, Limits] = {
        Key.of("go2", "base", axis): Limits(-1.0, 1.0) for axis in (VX, VY, WZ)
    }
    description = twist_base_description("go2", limits=limits)
    source = source_at(description, {"go2/base/vx": 0.4, "go2/base/x": 2.0})

    assert source.joint_names == []
    assert source.read_joints().joint_positions == {}
    assert source.read_sensors() == {}
    assert source.command({}, None) == {"go2/base/vx": 0.0, "go2/base/vy": 0.0, "go2/base/wz": 0.0}


def test_described_again_it_keeps_its_holds_and_waits_for_the_next_reading() -> None:
    description = arm()
    before = source_at(description, {"arm/joint1/position": 0.3})
    before.command({J1: 0.5}, ControlMode.POSITION)

    after = ConnectionSource(description)
    after.adopt(before)

    # Its joints are still known, but nothing is sent until it reports again.
    assert after.read_joints().joint_positions[J1] == 0.3
    assert not after.ready_for_control()

    # A held arm sags a little below its target; the hold stays on the target.
    after.on_state(reading(description, {"arm/joint1/position": 0.49}, sequence=1))
    assert after.ready_for_control()
    assert after.command({}, None)[f"{J1}/position"] == 0.5


def test_a_restarted_connection_is_held_where_it_now_is() -> None:
    description = arm()
    source = ConnectionSource(description)
    source.on_state(reading(description, {"arm/joint1/position": 0.3}, sequence=7))
    source.command({J1: 0.5}, ControlMode.POSITION)
    assert source.command({}, None)[f"{J1}/position"] == 0.5

    # It restarted and counts from 0 again; it was moved by hand meanwhile.
    source.on_state(reading(description, {"arm/joint1/position": 0.9}, sequence=0))

    assert source.command({}, None)[f"{J1}/position"] == 0.9


@pytest.mark.parametrize(
    ("sequence", "held"),
    [(2, 0.5), (0, 0.48)],
    ids=["same-connection-keeps-holds", "restarted-takes-them-again"],
)
def test_a_robot_that_goes_quiet_is_commanded_again_once_it_reports(
    sequence: int, held: float
) -> None:
    description = arm()
    source = ConnectionSource(description)
    source.on_state(reading(description, {"arm/joint1/position": 0.3}, sequence=1))
    source.command({J1: 0.5}, ControlMode.POSITION)

    quiet = time.monotonic() + description.deadman_timeout_s + 0.01
    assert not source.ready_for_control(quiet)
    # Its joints stay in what tasks see.
    assert source.read_joints().joint_positions[J1] == 0.3

    source.on_state(reading(description, {"arm/joint1/position": 0.48}, sequence=sequence))
    assert source.ready_for_control()
    assert source.command({}, None)[f"{J1}/position"] == held
