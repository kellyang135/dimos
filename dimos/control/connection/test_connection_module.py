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

"""Tests for ``ConnectionModule``, driven through ``MockConnectionModule``.

Real modules with real threads, no coordinator. Their two ports are given an
in-process transport, so nothing touches the network.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import replace
import pickle
import threading
import time
from typing import Any

import pytest

from dimos.control.connection.connection_module import ConnectionStatus
from dimos.control.connection.mock_connection import MockConnectionModule
from dimos.control.contract.description import ControlDescription, Limits
from dimos.control.contract.keys import VX, VY, WZ, Key
from dimos.control.contract.presets import twist_base_description
from dimos.control.contract.validate import DescriptionError
from dimos.msgs.control_msgs.ControlValues import ControlValues

JOINT1 = "mock/joint1/position"
DEADMAN_S = 0.05


class LocalTransport:
    """An in-process transport: publishing calls every subscriber at once."""

    def __init__(self) -> None:
        self.sent: list[Any] = []
        self.subscribers: list[Callable[[Any], Any]] = []

    def broadcast(self, selfstream: Any, value: Any) -> None:
        self.sent.append(value)
        for cb in list(self.subscribers):
            cb(value)

    def publish(self, value: Any) -> None:
        self.broadcast(None, value)

    def subscribe(self, cb: Callable[[Any], Any], selfstream: Any = None) -> Callable[[], None]:
        self.subscribers.append(cb)
        return lambda: self.subscribers.remove(cb)

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class Rig:
    """One started module and the two ends of its ports."""

    def __init__(self, module: MockConnectionModule) -> None:
        self.module = module
        self.state = LocalTransport()
        self.bus = LocalTransport()
        module.set_transport("control_state", self.state)  # type: ignore[arg-type]
        module.set_transport("control_command", self.bus)  # type: ignore[arg-type]

    def send(self, sequence: int, values: dict[str, float]) -> None:
        self.bus.publish(
            ControlValues(
                source="test",
                sequence=sequence,
                interface_names=list(values),
                values=list(values.values()),
            )
        )

    def frames(self, source: str = "mock") -> list[ControlValues]:
        return [frame for frame in list(self.state.sent) if frame.source == source]


@pytest.fixture
def rig() -> Iterator[Callable[..., Rig]]:
    """Build modules, start them, and stop every one at the end of the test."""
    made: list[Rig] = []

    def build(cls: type[MockConnectionModule] = MockConnectionModule, **config: Any) -> Rig:
        config.setdefault("deadman_timeout_s", DEADMAN_S)
        made.append(Rig(cls(**config)))
        made[-1].module.start()
        return made[-1]

    yield build
    for each in made:
        each.module.stop()


def wait_until(condition: Callable[[], bool], timeout_s: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.005)
    return True


class ArmAndBase(MockConnectionModule):
    """One module running two robots: the mock arm and a base called "base"."""

    def describe(self) -> list[ControlDescription]:
        arm = super().describe()
        assert isinstance(arm, ControlDescription)
        limits = {Key.of("base", "base", axis): Limits(-1.0, 1.0) for axis in (VX, VY, WZ)}
        return [arm, twist_base_description("base", limits=limits)]


class SlowWrite(MockConnectionModule):
    """Its first write waits until the test lets it go."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.inside = threading.Event()
        self.release = threading.Event()

    def write(self, values: dict[str, float]) -> None:
        self.inside.set()
        self.release.wait(5.0)
        super().write(values)


class FailingWrite(MockConnectionModule):
    def write(self, values: dict[str, float]) -> None:
        raise RuntimeError("motor said no")


class FailingHalt(MockConnectionModule):
    def halt(self) -> None:
        super().halt()
        raise RuntimeError("brake said no")


class PushedState(MockConnectionModule):
    """Never polled: readings only go out when the test publishes them."""

    def read_state(self) -> dict[str, float] | None:
        return None


class BadDescription(MockConnectionModule):
    connected = False

    def describe(self) -> ControlDescription:
        arm = super().describe()
        assert isinstance(arm, ControlDescription)
        return replace(arm, state_rate_hz=0.0)

    def connect(self) -> None:
        self.connected = True


def test_a_bad_description_fails_start_before_connecting() -> None:
    module = BadDescription()
    try:
        with pytest.raises(DescriptionError, match="state_rate_hz"):
            module.start()
        assert not module.connected
    finally:
        module.stop()


def test_readings_arrive_at_about_the_state_rate_numbered_in_order(
    rig: Callable[..., Rig],
) -> None:
    started = time.monotonic()
    r = rig(state_rate_hz=100.0)
    assert wait_until(lambda: len(r.frames()) >= 20)
    # 20 readings at 100 Hz take at least 19 periods.
    assert time.monotonic() - started >= 0.18
    frames = r.frames()
    assert [f.sequence for f in frames] == list(range(len(frames)))
    assert all(f.epoch == 0 for f in frames)
    assert set(frames[0].interface_names) == {f"mock/joint{i}/position" for i in range(1, 8)}


def test_a_command_is_written_and_the_readings_follow(rig: Callable[..., Rig]) -> None:
    r = rig()
    r.send(1, {JOINT1: 0.5})
    assert wait_until(lambda: list(r.module.writes) == [{JOINT1: 0.5}])
    assert wait_until(lambda: any(f.as_dict()[JOINT1] == 0.5 for f in r.frames()))
    assert r.module.status().last_command_time is not None


def test_keys_of_other_robots_are_ignored(rig: Callable[..., Rig]) -> None:
    r = rig()
    r.send(1, {"other/joint1/position": 9.0})
    r.send(2, {JOINT1: 0.5, "other/joint1/position": 9.0})
    assert wait_until(lambda: len(r.module.writes) == 1)
    assert list(r.module.writes) == [{JOINT1: 0.5}]
    assert r.module.status().last_rejection is None


def test_two_robots_in_one_module_each_get_only_their_own_keys(
    rig: Callable[..., Rig],
) -> None:
    r = rig(ArmAndBase)
    assert [d.source for d in r.module.describe_control()] == ["mock", "base"]
    r.send(1, {JOINT1: 0.5, "base/base/vx": 0.2, "base/base/vy": 0.0, "base/base/wz": 0.1})
    assert wait_until(lambda: len(r.module.writes) == 2)
    assert sorted(r.module.writes, key=lambda w: sorted(w)) == [
        {"base/base/vx": 0.2, "base/base/vy": 0.0, "base/base/wz": 0.1},
        {JOINT1: 0.5},
    ]
    # Readings go out as one frame per robot.
    assert wait_until(lambda: bool(r.frames("mock")) and bool(r.frames("base")))
    assert all(k.startswith("base/") for k in r.frames("base")[-1].interface_names)


def test_a_base_command_past_its_limit_is_pulled_back_to_it(rig: Callable[..., Rig]) -> None:
    r = rig(ArmAndBase)
    r.send(1, {"base/base/vx": 5.0})
    assert wait_until(lambda: list(r.module.writes) == [{"base/base/vx": 1.0}])


def test_an_arm_command_past_its_limit_is_refused_and_shown(rig: Callable[..., Rig]) -> None:
    r = rig(position_limit=1.0)
    r.send(1, {JOINT1: 1.5})
    assert wait_until(lambda: r.module.status().last_rejection is not None)
    assert "limit" in str(r.module.status().last_rejection)
    assert list(r.module.writes) == []


def test_an_old_or_repeated_command_is_refused(rig: Callable[..., Rig]) -> None:
    r = rig(deadman_timeout_s=10.0)
    r.send(5, {JOINT1: 0.1})
    assert wait_until(lambda: len(r.module.writes) == 1)
    r.send(3, {JOINT1: 0.2})
    assert wait_until(lambda: "sequence" in str(r.module.status().last_rejection))
    r.send(6, {JOINT1: 0.3})
    assert wait_until(lambda: len(r.module.writes) == 2)
    assert r.module.writes[-1] == {JOINT1: 0.3}


def test_only_the_newest_command_is_written_when_write_is_slow(
    rig: Callable[..., Rig],
) -> None:
    r = rig(SlowWrite, deadman_timeout_s=10.0)
    module = r.module
    assert isinstance(module, SlowWrite)
    r.send(1, {JOINT1: 0.1})
    assert module.inside.wait(2.0)
    # Sending returns at once even though write is stuck.
    for sequence in (2, 3, 4, 5):
        r.send(sequence, {JOINT1: sequence / 10})
    module.release.set()
    assert wait_until(lambda: len(module.writes) == 2)
    time.sleep(0.05)
    assert list(module.writes) == [{JOINT1: 0.1}, {JOINT1: 0.5}]


def test_the_deadman_waits_for_the_first_command(rig: Callable[..., Rig]) -> None:
    r = rig()
    time.sleep(4 * DEADMAN_S)
    assert r.module.halts == 0
    assert not r.module.status().deadman_fired


def test_the_deadman_halts_once_then_starts_again_on_the_next_command(
    rig: Callable[..., Rig],
) -> None:
    r = rig()
    r.send(1, {JOINT1: 0.1})
    assert wait_until(lambda: r.module.halts == 1)
    assert r.module.status().deadman_fired
    time.sleep(4 * DEADMAN_S)
    assert r.module.halts == 1

    r.send(2, {JOINT1: 0.2})
    assert wait_until(lambda: len(r.module.writes) == 2)
    assert wait_until(lambda: r.module.halts == 2)


def test_frames_for_other_robots_do_not_feed_the_deadman(rig: Callable[..., Rig]) -> None:
    r = rig()
    r.send(1, {JOINT1: 0.1})
    deadline = time.monotonic() + 1.0
    while r.module.halts == 0 and time.monotonic() < deadline:
        r.send(2, {"other/joint1/position": 0.0})
        r.send(3, {})
        time.sleep(DEADMAN_S / 5)
    assert r.module.halts == 1


def test_after_the_deadman_a_sender_counting_from_zero_is_accepted(
    rig: Callable[..., Rig],
) -> None:
    r = rig()
    r.send(100, {JOINT1: 0.1})
    assert wait_until(lambda: r.module.halts == 1)
    r.send(0, {JOINT1: 0.2})
    assert wait_until(lambda: len(r.module.writes) == 2)
    assert r.module.status().last_rejection is None


def test_a_failing_write_halts_shows_the_error_and_carries_on(
    rig: Callable[..., Rig],
) -> None:
    r = rig(FailingWrite)
    r.send(1, {JOINT1: 0.1})
    assert wait_until(lambda: r.module.halts == 1)
    status = r.module.status()
    assert "motor said no" in str(status.last_error)
    assert status.connected
    # The failed write ended the deadman, so it does not halt a second time.
    time.sleep(4 * DEADMAN_S)
    assert r.module.halts == 1
    r.send(2, {JOINT1: 0.2})
    assert wait_until(lambda: r.module.halts == 2)


def test_published_readings_are_checked_against_the_description(
    rig: Callable[..., Rig],
) -> None:
    r = rig(PushedState)
    readings = {f"mock/joint{i}/position": 0.0 for i in range(1, 8)}
    r.module.publish_state(readings)
    assert len(r.frames()) == 1

    r.module.publish_state({JOINT1: 0.0})
    assert len(r.frames()) == 1
    assert "missing_key" in str(r.module.status().last_error)

    r.module.publish_state({"other/joint1/position": 0.0})
    assert "other" in str(r.module.status().last_error)


def test_stopping_halts_once_and_a_second_stop_is_harmless(rig: Callable[..., Rig]) -> None:
    r = rig()
    r.module.stop()
    r.module.stop()
    assert r.module.halts == 1
    assert not r.module.status().connected
    r.send(1, {JOINT1: 0.1})
    time.sleep(0.05)
    assert list(r.module.writes) == []


def test_halt_robot_halts_at_once_every_time_it_is_called(rig: Callable[..., Rig]) -> None:
    # A driver overrides halt, so halting on request needs an RPC of its own.
    assert "halt_robot" in MockConnectionModule.rpcs
    r = rig()
    assert r.module.halt_robot()
    assert r.module.halt_robot()
    assert r.module.halts == 2


def test_halt_robot_says_so_when_halt_fails(rig: Callable[..., Rig]) -> None:
    r = rig(FailingHalt)
    assert r.module.halt_robot() is False
    assert "brake said no" in (r.module.status().last_error or "")


def test_halt_robot_waits_for_a_write_in_progress(rig: Callable[..., Rig]) -> None:
    r = rig(SlowWrite, deadman_timeout_s=10.0)
    module = r.module
    assert isinstance(module, SlowWrite)
    r.send(1, {JOINT1: 0.1})
    assert module.inside.wait(2.0)

    halting = threading.Thread(target=module.halt_robot)
    halting.start()
    time.sleep(0.05)
    assert module.halts == 0
    module.release.set()
    halting.join(2.0)
    assert module.halts == 1
    assert list(module.writes) == [{JOINT1: 0.1}]


def test_halt_robot_after_stop_does_nothing(rig: Callable[..., Rig]) -> None:
    r = rig()
    r.module.stop()
    assert r.module.halt_robot() is False
    assert r.module.halts == 1


def test_describe_control_and_status_are_rpcs_that_cross_processes(
    rig: Callable[..., Rig],
) -> None:
    assert {"describe_control", "status"} <= set(MockConnectionModule.rpcs)
    r = rig()
    [description] = r.module.describe_control()
    assert description == r.module.describe()
    assert pickle.loads(pickle.dumps(description)) == description

    assert wait_until(lambda: r.module.status().last_state_time is not None)
    status = r.module.status()
    assert isinstance(status, ConnectionStatus)
    assert pickle.loads(pickle.dumps(status)) == status
    assert status.connected
    assert status.last_command_time is None
    assert not status.deadman_fired
    assert status.last_rejection is None
    assert status.last_error is None
