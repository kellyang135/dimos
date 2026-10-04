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

"""Tests for the coordinator finding connection modules, driving their robots,
and stopping them.

Most run in-process: connection modules are stand-ins that answer
``describe_control``, ``halt_robot`` and ``status``, readings are fed straight
into ``control_state``, and the tick loop is stepped by hand, one tick at a time.

The last four run end to end, with ``MockConnectionModule`` in real worker
processes. Two send the coordinator a trajectory and wait until the mock
arm's readings, as the coordinator sees them, arrive at its end. That only
happens if commands reach the mock on ``control_command`` and its readings
come back on ``control_state``. The other two stop the mock: once with an
emergency stop, once by killing the coordinator's process.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
import os
import signal
import time
from typing import Any

import pytest

from dimos.control.components import HardwareComponent, HardwareType, make_joints
from dimos.control.connection.connection_module import ConnectionStatus
from dimos.control.connection.mock_connection import MockConnectionModule
from dimos.control.contract.description import ControlDescription, Limits
from dimos.control.contract.keys import EFFORT, POSITION, VX, VY, WZ, Key
from dimos.control.contract.presets import (
    imu_resource,
    manipulator_description,
    twist_base_description,
)
import dimos.control.coordinator as coord_mod
from dimos.control.coordinator import ControlCoordinator
from dimos.control.task import (
    BaseControlTask,
    ControlMode,
    CoordinatorState,
    JointCommandOutput,
    ResourceClaim,
)
from dimos.control.tasks.trajectory_task.trajectory_task import (
    TrajectoryExecutionStatus,
    joint_trajectory_task,
)
import dimos.control.tick_loop as tick_mod
from dimos.control.tick_loop import TickLoop
from dimos.core.coordination.blueprint_config.parser import BlueprintConfigParser
from dimos.core.coordination.blueprints import Blueprint
from dimos.core.coordination.module_coordinator import ModuleCoordinator
from dimos.core.stream import Out
from dimos.msgs.control_msgs.ControlValues import ControlValues
from dimos.msgs.trajectory_msgs.JointTrajectory import JointTrajectory
from dimos.msgs.trajectory_msgs.TrajectoryPoint import TrajectoryPoint
from dimos.robot.manipulators.common.mock import coordinator_mock_connection


def arm(joints: int = 2, name: str = "arm", **kwargs: Any) -> ControlDescription:
    """An arm, called "arm" unless named, that reports and takes joint positions."""
    return manipulator_description(
        name,
        [f"joint{i}" for i in range(1, joints + 1)],
        state=(POSITION,),
        command=(POSITION,),
        **kwargs,
    )


def base() -> ControlDescription:
    limits: dict[str, Limits] = {
        Key.of("go2", "base", axis): Limits(-1.0, 1.0) for axis in (VX, VY, WZ)
    }
    return twist_base_description("go2", limits=limits)


def reading(
    description: ControlDescription, values: dict[str, float] | None = None
) -> ControlValues:
    """A full reading from the robot: every key it reports, 0.0 unless given."""
    given = values or {}
    keys = description.state_keys()
    return ControlValues(
        source=description.source,
        interface_names=list(keys),
        values=[given.get(key, 0.0) for key in keys],
    )


STATUS = ConnectionStatus(
    connected=True,
    last_state_time=1.0,
    last_command_time=2.0,
    deadman_fired=False,
    last_rejection=None,
    last_error=None,
)


class StandIn:
    """Plays a module proxy: ``rpcs`` names what it serves.

    A connection stand-in also answers ``halt_robot`` and ``status`` when the
    coordinator calls them by name, unless ``answers`` is False.
    """

    def __init__(
        self,
        name: str,
        descriptions: list[ControlDescription] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.remote_name = name
        self.descriptions = descriptions or []
        self.error = error
        connection = descriptions is not None or error is not None
        self.rpcs = {"start", "stop", *(["describe_control"] if connection else [])}
        self.asked = 0
        self.closed = False
        self.answers = True
        self.halts = 0

    def describe_control(self) -> list[ControlDescription]:
        self.asked += 1
        if self.error is not None:
            raise self.error
        return list(self.descriptions)

    def halt_robot(self) -> None:
        self.halts += 1
        if self.error is not None:
            raise self.error

    def status(self) -> ConnectionStatus:
        return STATUS

    def stop_rpc_client(self) -> None:
        self.closed = True


class LocalTransport:
    """An in-process transport: publishing calls every subscriber at once."""

    def __init__(self) -> None:
        self.subscribers: list[Callable[[Any], Any]] = []

    def publish(self, value: Any) -> None:
        for cb in list(self.subscribers):
            cb(value)

    def subscribe(self, cb: Callable[[Any], Any], selfstream: Any = None) -> Callable[[], None]:
        self.subscribers.append(cb)
        return lambda: self.subscribers.remove(cb)

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class SendsPositions(BaseControlTask):
    """Always active; drives fixed positions, and keeps the last state it saw."""

    def __init__(self, positions: dict[str, float]) -> None:
        self._name = "sends_positions"
        self._positions = positions
        self.last_state: CoordinatorState | None = None

    def claim(self) -> ResourceClaim:
        return ResourceClaim(joints=frozenset(self._positions), priority=10)

    def is_active(self) -> bool:
        return True

    def compute(self, state: CoordinatorState) -> JointCommandOutput | None:
        self.last_state = state
        return JointCommandOutput(
            joint_names=list(self._positions),
            positions=list(self._positions.values()),
            mode=ControlMode.POSITION,
        )

    def on_preempted(self, by_task: str, joints: frozenset[str]) -> None:
        pass


class ArmPortCoordinator(ControlCoordinator):
    arm_control_command: Out[ControlValues]


class Rig:
    """A started coordinator whose tick loop only moves when ``tick`` is called."""

    def __init__(self, coordinator: ControlCoordinator) -> None:
        self.coordinator = coordinator
        self.state = LocalTransport()
        coordinator.control_state.transport = self.state  # type: ignore[assignment]
        self.sent: list[ControlValues] = []
        coordinator.control_command.subscribe(self.sent.append)
        coordinator.start()
        assert coordinator._tick_loop is not None
        self.tick = coordinator._tick_loop._tick
        # Calls the coordinator makes by module name go to the stand-ins.
        self.modules: dict[str, StandIn] = {}
        coordinator.rpc.call = self._call  # type: ignore[method-assign, assignment]

    def find(self, *modules: StandIn) -> None:
        self.modules = {module.remote_name: module for module in modules}
        self.coordinator.on_system_modules(list(modules))  # type: ignore[arg-type]

    def _call(self, name: str, arguments: Any, answer: Callable[[Any], None]) -> Callable[[], None]:
        module_name, method = name.split("/")
        module = self.modules[module_name]
        if module.answers:
            try:
                answer(getattr(module, method)(*arguments[0], **arguments[1]))
            except Exception as error:
                answer(error)
        return lambda: None

    def report(
        self, description: ControlDescription, values: dict[str, float] | None = None
    ) -> None:
        self.state.publish(reading(description, values))


@pytest.fixture
def rig(mocker: Any) -> Iterator[Callable[..., Rig]]:
    mocker.patch.object(TickLoop, "start")
    made: list[Rig] = []

    def build(cls: type[ControlCoordinator] = ControlCoordinator, **config: Any) -> Rig:
        made.append(Rig(cls(**config)))
        return made[-1]

    yield build
    for each in made:
        each.coordinator.stop()


def test_only_modules_serving_describe_control_are_asked(rig: Callable[..., Rig]) -> None:
    r = rig()
    other = StandIn("Other")
    connection = StandIn("Mock", [arm()])

    r.find(other, connection)

    assert other.asked == 0
    assert connection.asked == 1
    assert r.coordinator.list_hardware() == ["arm"]
    assert r.coordinator.list_joints() == ["arm/joint1", "arm/joint2"]


def test_every_call_asks_again_and_replaces_what_was_found(rig: Callable[..., Rig]) -> None:
    r = rig()
    connection = StandIn("Mock", [arm(joints=2)])
    r.find(connection)

    connection.descriptions = [arm(joints=3)]
    r.find(connection)
    assert connection.asked == 2
    assert r.coordinator.list_joints() == ["arm/joint1", "arm/joint2", "arm/joint3"]

    r.find()
    assert r.coordinator.list_hardware() == []
    assert r.coordinator.list_joints() == []


def test_one_failing_module_does_not_stop_the_rest(rig: Callable[..., Rig]) -> None:
    r = rig()
    broken = StandIn("Broken", error=RuntimeError("no"))
    first = StandIn("First", [arm(joints=2)])
    duplicate = StandIn("Duplicate", [arm(joints=3)])

    r.find(broken, first, duplicate)

    assert r.coordinator.list_joints() == ["arm/joint1", "arm/joint2"]


def test_a_robot_with_an_effort_only_joint_is_not_driven(
    rig: Callable[..., Rig], mocker: Any
) -> None:
    error = mocker.patch.object(coord_mod.logger, "error")
    r = rig()
    limp = manipulator_description("limp", ["joint1"], state=(POSITION,), command=(EFFORT,))

    r.find(StandIn("Limp", [limp]), StandIn("Mock", [arm()]))

    assert r.coordinator.list_hardware() == ["arm"]
    logged = str(error.call_args)
    assert "'limp'" in logged
    assert "joint1" in logged


def test_every_proxy_it_was_given_is_closed(rig: Callable[..., Rig]) -> None:
    r = rig()
    modules = [StandIn("Other"), StandIn("Mock", [arm()]), StandIn("Broken", error=ValueError())]

    r.find(*modules)

    assert all(module.closed for module in modules)


def test_a_coordinator_with_adapters_ignores_connections(
    rig: Callable[..., Rig], mocker: Any
) -> None:
    error = mocker.patch.object(coord_mod.logger, "error")
    adapter_arm = HardwareComponent(
        hardware_id="left_arm",
        hardware_type=HardwareType.MANIPULATOR,
        joints=make_joints("left_arm", 2),
        adapter_type="mock",
    )
    r = rig(hardware=[adapter_arm])

    r.find(StandIn("Mock", [arm()]))

    assert r.coordinator.list_hardware() == ["left_arm"]
    assert "adapters" in str(error.call_args)


def test_every_robot_that_has_reported_gets_a_complete_command_each_tick(
    rig: Callable[..., Rig],
) -> None:
    r = rig()
    r.find(StandIn("Mock", [arm()]), StandIn("Go2", [base()]))

    r.tick()
    assert r.sent == []

    r.report(arm(), {"arm/joint1/position": 0.3, "arm/joint2/position": -0.2})
    r.tick()
    r.report(base())
    r.tick()

    assert [frame.sequence for frame in r.sent] == [0, 1, 2]
    held = {"arm/joint1/position": 0.3, "arm/joint2/position": -0.2}
    assert r.sent[0].as_dict() == held
    by_robot = {frame.interface_names[0].split("/")[0]: frame.as_dict() for frame in r.sent[1:]}
    assert by_robot == {
        "arm": held,
        "go2": {"go2/base/vx": 0.0, "go2/base/vy": 0.0, "go2/base/wz": 0.0},
    }
    assert r.coordinator.get_joint_positions() == {"arm/joint1": 0.3, "arm/joint2": -0.2}


def test_tasks_drive_connection_joints_and_see_their_sensors(rig: Callable[..., Rig]) -> None:
    r = rig()
    description = arm(sensors=[imu_resource()])
    r.find(StandIn("Mock", [description]))
    task = SendsPositions({"arm/joint1": 0.5})
    r.coordinator.add_task(task)

    r.report(description, {"arm/joint2/position": -0.2, "arm/imu/qw": 1.0})
    r.tick()

    assert r.sent[-1].as_dict() == {"arm/joint1/position": 0.5, "arm/joint2/position": -0.2}
    assert task.last_state is not None
    assert task.last_state.sensors.readings["arm/imu"]["qw"] == 1.0
    # The arm reports no velocity, so none is made up.
    assert task.last_state.joints.get_velocity("arm/joint1") is None


def test_found_again_a_robot_keeps_its_holds_and_waits_to_report(
    rig: Callable[..., Rig],
) -> None:
    r = rig()
    connection = StandIn("Mock", [arm()])
    r.find(connection)
    r.report(arm(), {"arm/joint1/position": 0.3})
    r.tick()
    # A held arm sags a little below where it is held.
    r.report(arm(), {"arm/joint1/position": 0.29})
    r.tick()
    assert [frame.as_dict()["arm/joint1/position"] for frame in r.sent] == [0.3, 0.3]

    # Something unrelated was loaded, so every connection module is found again.
    r.find(StandIn("Camera"), connection)
    r.tick()
    assert len(r.sent) == 2
    assert r.coordinator.get_joint_positions()["arm/joint1"] == 0.29

    # Still held where it was, not where it sagged to.
    r.report(arm(), {"arm/joint1/position": 0.28})
    r.tick()
    assert r.sent[-1].as_dict()["arm/joint1/position"] == 0.3


def test_a_robot_described_differently_waits_for_a_reading_in_its_new_shape(
    rig: Callable[..., Rig],
) -> None:
    r = rig()
    connection = StandIn("Mock", [arm(joints=2)])
    r.find(connection)
    r.report(arm(joints=2))
    r.tick()

    connection.descriptions = [arm(joints=3)]
    r.find(connection)
    r.tick()
    assert len(r.sent) == 1
    assert r.coordinator.get_joint_positions() == {}

    r.report(arm(joints=3))
    r.tick()
    assert len(r.sent[-1].interface_names) == 3


def test_a_robot_whose_readings_stop_is_not_commanded(rig: Callable[..., Rig]) -> None:
    r = rig()
    description = arm(deadman_timeout_s=0.2)
    r.find(StandIn("Mock", [description]))
    r.report(description, {"arm/joint1/position": 0.3})
    r.tick()
    assert len(r.sent) == 1

    time.sleep(0.3)
    r.tick()
    assert len(r.sent) == 1
    assert r.coordinator.get_joint_positions()["arm/joint1"] == 0.3

    r.report(description, {"arm/joint1/position": 0.3})
    r.tick()
    assert len(r.sent) == 2


def test_with_the_option_on_a_robot_also_gets_its_own_topic(rig: Callable[..., Rig]) -> None:
    r = rig(ArmPortCoordinator, publish_robot_commands=True)
    own: list[ControlValues] = []
    r.coordinator.arm_control_command.subscribe(own.append)  # type: ignore[attr-defined]
    r.find(StandIn("Mock", [arm()]), StandIn("Go2", [base()]))

    r.report(arm())
    r.report(base())
    r.tick()

    assert len(r.sent) == 2
    assert own == [frame for frame in r.sent if frame.interface_names[0].startswith("arm/")]


def test_with_the_option_off_the_robot_topic_stays_quiet(rig: Callable[..., Rig]) -> None:
    r = rig(ArmPortCoordinator)
    own: list[ControlValues] = []
    r.coordinator.arm_control_command.subscribe(own.append)  # type: ignore[attr-defined]
    r.find(StandIn("Mock", [arm()]))

    r.report(arm())
    r.tick()

    assert len(r.sent) == 1
    assert own == []


def test_a_robot_without_its_own_joints_port_is_still_driven(
    rig: Callable[..., Rig], mocker: Any
) -> None:
    error = mocker.patch.object(tick_mod.logger, "error")
    r = rig(publish_robot_joint_states=True)
    r.find(StandIn("Mock", [arm()]))

    r.report(arm())
    r.tick()

    assert len(r.sent) == 1
    error.assert_not_called()


def test_estop_sends_nothing_more_and_halts_each_connection_module_once(
    rig: Callable[..., Rig],
) -> None:
    r = rig()
    # One module running two robots, and a module that is not a connection.
    both = StandIn("Both", [arm(), base()])
    r.find(both, StandIn("Camera"))
    r.report(arm())
    r.report(base())
    r.tick()
    assert len(r.sent) == 2

    assert r.coordinator.set_estop(True)

    assert both.halts == 1
    r.report(arm())
    r.report(base())
    r.tick()
    assert len(r.sent) == 2


def test_a_module_that_fails_to_halt_is_named_and_does_not_hold_up_the_rest(
    rig: Callable[..., Rig], mocker: Any
) -> None:
    mocker.patch.object(coord_mod, "_CONNECTION_RPC_TIMEOUT_S", 0.05)
    error = mocker.patch.object(coord_mod.logger, "error")
    r = rig()
    silent = StandIn("Silent", [arm()])
    silent.answers = False
    broken = StandIn("Broken", [arm(name="left")])
    working = StandIn("Working", [base()])
    r.find(silent, broken, working)
    broken.error = RuntimeError("no")

    assert r.coordinator.set_estop(True) is False

    assert working.halts == 1
    assert broken.halts == 1
    logged = str(error.call_args_list)
    assert "'Silent' did not answer halt_robot" in logged
    assert "'Broken' failed halt_robot" in logged


def test_clearing_the_estop_holds_each_joint_where_the_robot_now_is(
    rig: Callable[..., Rig],
) -> None:
    r = rig()
    r.find(StandIn("Mock", [arm()]))
    task = SendsPositions({"arm/joint1": 0.5})
    r.coordinator.add_task(task)
    r.report(arm(), {"arm/joint1/position": 0.3})
    r.tick()
    assert r.sent[-1].as_dict()["arm/joint1/position"] == 0.5

    r.coordinator.set_estop(True)
    # SendsPositions has no set_estop; take it away so nothing drives the arm.
    r.coordinator.remove_task(task.name)
    # Halted short of where it was told to go.
    r.report(arm(), {"arm/joint1/position": 0.4})
    r.tick()
    assert len(r.sent) == 1

    assert r.coordinator.set_estop(False)
    r.tick()

    assert len(r.sent) == 2
    assert r.sent[-1].as_dict() == {"arm/joint1/position": 0.4, "arm/joint2/position": 0.0}


def test_connection_status_is_by_robot_and_none_for_a_silent_module(
    rig: Callable[..., Rig], mocker: Any
) -> None:
    mocker.patch.object(coord_mod, "_CONNECTION_RPC_TIMEOUT_S", 0.05)
    r = rig()
    silent = StandIn("Silent", [arm(name="left")])
    silent.answers = False
    r.find(StandIn("Both", [arm(), base()]), silent, StandIn("Camera"))

    assert r.coordinator.get_connection_status() == {
        "arm": STATUS,
        "go2": STATUS,
        "left": None,
    }


# End to end, in real worker processes.

JOINTS = make_joints("mock", 7)
TARGET = [0.2, -0.1, 0.3, 0.0, 0.1, -0.2, 0.25]
# Long enough that a busy CI machine never trips the mock's deadman.
DEADMAN_S = 0.5


def wait_until(condition: Callable[[], bool], timeout_s: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.02)
    return True


Build = Callable[..., ModuleCoordinator]


@pytest.fixture
def build() -> Iterator[Build]:
    """Build blueprints with no viewer, and stop every one at the end of the test."""
    built: list[ModuleCoordinator] = []

    def _build(blueprint: Blueprint, **modules: dict[str, Any]) -> ModuleCoordinator:
        parsed = BlueprintConfigParser(blueprint).parse(
            environ={},
            overrides={"g": {"viewer": "none"}, **modules},
        )
        built.append(ModuleCoordinator.build(blueprint, parsed))
        return built[-1]

    yield _build
    for system in built:
        system.stop()


def drive_to_target(system: ModuleCoordinator) -> None:
    coordinator = system.get_instance(ControlCoordinator)
    mock = system.get_instance(MockConnectionModule)

    assert wait_until(lambda: set(JOINTS) <= set(coordinator.get_joint_positions()))
    start = coordinator.get_joint_positions()
    trajectory = JointTrajectory(
        joint_names=JOINTS,
        points=[
            TrajectoryPoint(positions=[start[joint] for joint in JOINTS]),
            TrajectoryPoint(positions=TARGET, time_from_start=0.3),
        ],
    )
    result = coordinator.execute_trajectory(trajectory)
    assert result.status is TrajectoryExecutionStatus.ACCEPTED, result

    def at_target() -> bool:
        positions = coordinator.get_joint_positions()
        return all(abs(positions[j] - t) < 1e-6 for j, t in zip(JOINTS, TARGET, strict=True))

    assert wait_until(at_target)
    status = mock.status()
    assert status.last_rejection is None
    assert status.last_error is None
    assert status.last_command_time is not None


def test_coordinator_and_mock_in_one_blueprint(build: Build) -> None:
    system = build(
        coordinator_mock_connection,
        **{MockConnectionModule.name: {"deadman_timeout_s": DEADMAN_S}},
    )

    drive_to_target(system)


def test_mock_loaded_after_the_coordinator_is_still_driven(build: Build) -> None:
    system = build(ControlCoordinator.blueprint(tasks=[joint_trajectory_task(JOINTS)]))

    system.load_blueprint(MockConnectionModule.blueprint(deadman_timeout_s=DEADMAN_S))

    drive_to_target(system)


def test_estop_halts_the_mock_and_clearing_drives_it_again(build: Build) -> None:
    system = build(
        coordinator_mock_connection,
        **{MockConnectionModule.name: {"deadman_timeout_s": DEADMAN_S}},
    )
    coordinator = system.get_instance(ControlCoordinator)
    mock = system.get_instance(MockConnectionModule)
    assert wait_until(lambda: mock.status().last_command_time is not None)

    assert coordinator.set_estop(True)
    stopped_at = time.time()
    status = coordinator.get_connection_status()["mock"]
    assert status is not None and status.connected
    # Sent nothing more, so its deadman fires too.
    assert wait_until(lambda: mock.status().deadman_fired, timeout_s=DEADMAN_S + 2.0)

    assert coordinator.set_estop(False)
    assert wait_until(lambda: (mock.status().last_command_time or 0.0) > stopped_at + DEADMAN_S)
    assert not mock.status().deadman_fired


def test_killing_the_coordinator_lets_the_mock_deadman_halt_it(build: Build) -> None:
    system = build(
        coordinator_mock_connection,
        **{MockConnectionModule.name: {"deadman_timeout_s": DEADMAN_S}},
    )
    mock = system.get_instance(MockConnectionModule)
    assert wait_until(lambda: mock.status().last_command_time is not None)
    assert not mock.status().deadman_fired

    workers = system._managers["python"].workers  # type: ignore[attr-defined]
    [worker] = [w for w in workers if "ControlCoordinator" in w.module_names]
    assert "MockConnectionModule" not in worker.module_names
    assert worker.pid is not None
    os.kill(worker.pid, signal.SIGKILL)

    # The deadman fires DEADMAN_S after the last command; the rest is slack
    # for a busy CI machine.
    assert wait_until(lambda: mock.status().deadman_fired, timeout_s=DEADMAN_S + 2.0)
