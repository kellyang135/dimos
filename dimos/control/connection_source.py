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

"""The coordinator's record of one robot it drives through a connection module.

A connection module publishes a robot's readings on ``control_state`` and
obeys the ``ControlValues`` frames it receives on ``control_command``. The
coordinator keeps one ``ConnectionSource`` per robot: its description, its
latest reading, and the positions its joints are being held at. The tick
loop reads joint and sensor state from it, and asks it for the complete
command to send each tick.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import threading
import time

from dimos.control.contract.description import ControlDescription, Limits, ResourceKind
from dimos.control.contract.keys import EFFORT, POSITION, SEPARATOR, VELOCITY
from dimos.control.contract.sequence import Freshness
from dimos.control.contract.validate import FrameRejectedError, validate_state
from dimos.control.task import JointStateSnapshot
from dimos.hardware.manipulators.spec import ControlMode
from dimos.msgs.control_msgs.ControlValues import ControlValues
from dimos.utils.logging_config import setup_logger

logger = setup_logger()

# Which part of a joint each task control mode drives.
_INTERFACE_BY_MODE: dict[ControlMode, str] = {
    ControlMode.POSITION: POSITION,
    ControlMode.SERVO_POSITION: POSITION,
    ControlMode.VELOCITY: VELOCITY,
    ControlMode.TORQUE: EFFORT,
}


@dataclass(frozen=True, slots=True)
class _Joint:
    """One joint's name and the keys it reports and accepts."""

    name: str
    state_keys: dict[str, str]
    command_keys: dict[str, str]
    position_limit: Limits | None


class ConnectionSource:
    """One robot behind a connection module, as the coordinator's tick loop sees it.

    Joints are named by robot and part, e.g. "arm/joint1", the same names
    tasks claim. Each tick the coordinator sends the robot a complete
    command: what tasks asked for, and for every other joint:

    - in a tick where tasks drive this robot by velocity: velocity 0;
    - otherwise: hold its position. That is the last position a task sent,
      or, if none did, where the joint was when the hold began. A held
      position is kept inside the joint's position limit. A joint that
      cannot be told a position gets velocity 0 instead.

    When the robot's connection module restarts, its readings count up from
    0 again. A reading numbered lower than the last one therefore means the
    robot may have been moved while its driver was down, so every hold is
    taken again from that reading.

    A base is told 0 on every axis. Sensors are never told anything.

    A robot whose readings stop for longer than its deadman timeout is no
    longer commanded; its connection's deadman halts it.

    What tasks ask for is sent as is, even past a limit or to an interface
    the robot does not accept. The connection module then refuses the whole
    frame, and if that keeps happening its deadman halts the robot.

    Does no I/O: the coordinator feeds it readings and publishes what it
    builds.

    Args:
        description: What the robot reports and accepts, from its connection
            module's ``describe_control``.
    """

    def __init__(self, description: ControlDescription) -> None:
        self._description = description
        self._joints: list[_Joint] = []
        self._base_command_keys: list[str] = []
        self._sensor_keys: list[tuple[str, str, str]] = []
        for resource in description.resources:
            prefix = f"{description.source}{SEPARATOR}{resource.name}"
            if resource.kind is ResourceKind.JOINT:
                self._joints.append(
                    _Joint(
                        name=prefix,
                        state_keys={
                            i: f"{prefix}{SEPARATOR}{i}" for i in resource.state_interfaces
                        },
                        command_keys={
                            i: f"{prefix}{SEPARATOR}{i}" for i in resource.command_interfaces
                        },
                        position_limit=description.limits.get(f"{prefix}{SEPARATOR}{POSITION}"),
                    )
                )
            elif resource.kind is ResourceKind.BASE:
                self._base_command_keys += [
                    f"{prefix}{SEPARATOR}{i}" for i in resource.command_interfaces
                ]
            else:
                self._sensor_keys += [
                    (prefix, i, f"{prefix}{SEPARATOR}{i}") for i in resource.state_interfaces
                ]
        # Guards the reading and the flags below, which readings change and
        # the tick loop reads.
        self._lock = threading.Lock()
        self._latest: dict[str, float] | None = None
        self._received_at = 0.0
        # When the last reading was accepted, by time.monotonic().
        self._freshness = Freshness()
        self._last_sequence: int | None = None
        # A reading numbered lower than the last arrived: the connection
        # restarted. The tick loop drops the holds when it sees this.
        self._restarted = False
        # Described again: send nothing until the robot reports again.
        self._awaiting_reading = False
        # Joint name -> the position it is held at. Only the tick loop touches it.
        self._holds: dict[str, float] = {}
        self._last_warned = float("-inf")

    @property
    def hardware_id(self) -> str:
        """The robot's name, e.g. "arm"."""
        return self._description.source

    @property
    def description(self) -> ControlDescription:
        """What the robot reports and accepts."""
        return self._description

    @property
    def joint_names(self) -> list[str]:
        """Every joint, e.g. ["arm/joint1", "arm/joint2"]."""
        return [joint.name for joint in self._joints]

    def on_state(self, frame: ControlValues) -> None:
        """Keep a reading from the robot, or drop it if it does not match the
        description (logged at most once a second)."""
        try:
            values = validate_state(self._description, frame)
        except FrameRejectedError as error:
            now = time.monotonic()
            if now - self._last_warned >= 1.0:
                self._last_warned = now
                logger.warning(f"Dropped a reading from {self.hardware_id!r}: {error}")
            return
        with self._lock:
            if self._last_sequence is not None and frame.sequence < self._last_sequence:
                self._restarted = True
            self._last_sequence = frame.sequence
            self._latest = values
            self._received_at = time.time()
            self._freshness.mark(time.monotonic())
            self._awaiting_reading = False

    def adopt(self, other: ConnectionSource) -> None:
        """Carry on from ``other``, an earlier record of the same robot with the
        same description: its reading and its holds.

        Commands wait for the robot's next reading, so that a robot whose
        connection module restarted is never sent where it was before.
        """
        with other._lock:
            latest, received_at = other._latest, other._received_at
            last_receipt = other._freshness.last_receipt
            last_sequence, restarted = other._last_sequence, other._restarted
        with self._lock:
            self._latest, self._received_at = latest, received_at
            self._freshness.last_receipt = last_receipt
            self._last_sequence, self._restarted = last_sequence, restarted
            self._awaiting_reading = True
        self._holds = dict(other._holds)

    def ready_for_control(self, now: float | None = None) -> bool:
        """Whether a command can be sent: the robot has reported again since it
        was last described, and its last reading is no older than its deadman
        timeout.

        Args:
            now: The current ``time.monotonic()``, in seconds. Defaults to now.
        """
        if now is None:
            now = time.monotonic()
        with self._lock:
            return not self._awaiting_reading and not self._freshness.is_stale(
                now, self._description.deadman_timeout_s
            )

    def read_joints(self) -> JointStateSnapshot:
        """The joints' latest positions, velocities and efforts.

        A joint that does not report something is left out of that lookup,
        rather than given a zero that looks like a real reading.
        """
        latest = self._latest or {}
        snapshot = JointStateSnapshot(timestamp=self._received_at)
        for joint in self._joints:
            for interface, into in (
                (POSITION, snapshot.joint_positions),
                (VELOCITY, snapshot.joint_velocities),
                (EFFORT, snapshot.joint_efforts),
            ):
                key = joint.state_keys.get(interface)
                if key is not None and key in latest:
                    into[joint.name] = latest[key]
        return snapshot

    def read_sensors(self) -> dict[str, dict[str, float]]:
        """Each sensor's latest readings, e.g. ``{"g1/imu": {"qw": 1.0, ...}}``."""
        latest = self._latest or {}
        readings: dict[str, dict[str, float]] = {}
        for sensor, reading, key in self._sensor_keys:
            if key in latest:
                readings.setdefault(sensor, {})[reading] = latest[key]
        return readings

    def command(self, winners: Mapping[str, float], mode: ControlMode | None) -> dict[str, float]:
        """The complete command to send the robot this tick, by key.

        Args:
            winners: What tasks asked for, by joint name, e.g.
                ``{"arm/joint1": 0.3}``. Only this robot's joints.
            mode: How the winners drive their joints. ``None`` when no task
                drives this robot this tick.
        """
        with self._lock:
            latest = self._latest
            if self._restarted:
                self._restarted = False
                self._holds.clear()
        interface = _INTERFACE_BY_MODE.get(mode) if mode is not None else None
        values: dict[str, float] = {}
        for joint in self._joints:
            if interface is not None and joint.name in winners:
                value = winners[joint.name]
                values[f"{joint.name}{SEPARATOR}{interface}"] = value
                if interface == POSITION:
                    self._holds[joint.name] = value
                else:
                    self._holds.pop(joint.name, None)
                continue
            velocity_key = joint.command_keys.get(VELOCITY)
            if mode is ControlMode.VELOCITY and velocity_key is not None:
                values[velocity_key] = 0.0
                self._holds.pop(joint.name, None)
                continue
            hold = self._hold(joint, latest)
            if hold is not None:
                values[joint.command_keys[POSITION]] = hold
            elif velocity_key is not None:
                values[velocity_key] = 0.0
        for key in self._base_command_keys:
            values[key] = 0.0
        return values

    def _hold(self, joint: _Joint, latest: dict[str, float] | None) -> float | None:
        """Where to hold ``joint``, inside its position limit, or ``None`` if
        it cannot be told a position or its position is unknown.

        Args:
            joint: The joint to hold.
            latest: The robot's latest reading, to start a new hold from.
        """
        if POSITION not in joint.command_keys:
            return None
        held = self._holds.get(joint.name)
        if held is None:
            measured_key = joint.state_keys.get(POSITION)
            if measured_key is None or latest is None or measured_key not in latest:
                return None
            held = latest[measured_key]
            self._holds[joint.name] = held
        limit = joint.position_limit
        if limit is not None:
            if limit.lo is not None:
                held = max(held, limit.lo)
            if limit.hi is not None:
                held = min(held, limit.hi)
        return held
