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

"""The base every robot driver is built on: one module per robot.

A driver subclasses ``ConnectionModule`` and fills in five methods for its
hardware. The base publishes the robot's readings on ``control_state``, checks
the commands that arrive on ``control_command`` and hands the good ones to the
driver, and halts the robot when commands stop coming. Anything that publishes
``ControlValues`` can drive it; it does not need a coordinator.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
import threading
import time
from typing import Any, ClassVar

from reactivex import operators as ops
from reactivex.abc import DisposableBase

from dimos.constants import DEFAULT_THREAD_JOIN_TIMEOUT
from dimos.control.contract.description import ControlDescription
from dimos.control.contract.keys import SEPARATOR
from dimos.control.contract.validate import (
    FrameRejectedError,
    Rejected,
    validate_command,
    validate_description,
    validate_state,
)
from dimos.core.core import rpc
from dimos.core.module import Module
from dimos.core.stream import In, Out
from dimos.msgs.control_msgs.ControlValues import ControlValues
from dimos.utils.logging_config import setup_logger

logger = setup_logger()


@dataclass(frozen=True, slots=True)
class ConnectionStatus:
    """A snapshot of one connection, returned by ``ConnectionModule.status``.

    Attributes:
        connected: ``connect`` succeeded and the module has not stopped.
        last_state_time: When readings were last published, in Unix seconds,
            or ``None`` if never.
        last_command_time: When a command was last accepted, in Unix seconds,
            or ``None`` if never.
        deadman_fired: Commands stopped arriving, the robot was halted, and no
            command has been accepted since.
        last_rejection: Why the last refused command was refused, or ``None``.
        last_error: The last failure in the driver (``write``, ``read_state``,
            ``halt``, or a reading that does not match the description), or
            ``None``.
    """

    connected: bool
    last_state_time: float | None
    last_command_time: float | None
    deadman_fired: bool
    last_rejection: str | None
    last_error: str | None


class ConnectionModule(Module, ABC):
    """A robot driver. Subclass it and fill in the five abstract methods.

    Starting it calls ``describe``, checks each description (raising on the
    first bad one, before touching the hardware), then calls ``connect``.

    Readings: a thread calls ``read_state`` at the fastest ``state_rate_hz``
    among the descriptions and publishes the result, one frame per robot.

    Commands: only frames carrying keys of a robot this module runs are
    looked at, and only that robot's keys are passed on. Each robot keeps just
    its newest frame, so if ``write`` is slower than commands arrive, the ones
    in between are skipped. A frame is checked with ``validate_command``
    (order, unknown keys, limits) before ``write`` sees it.

    Deadman: once a command has been accepted, if no other is accepted within
    the shortest ``deadman_timeout_s`` among the descriptions, ``halt`` is
    called once and each robot's last sequence number is forgotten, so a
    sender that restarts counting from 0 is accepted. The next accepted
    command starts the deadman again.

    ``write`` and ``halt`` never run at the same time. Commands and the
    deadman share the module's event loop, so a ``write`` that hangs stops
    both: the deadman cannot fire and no later command is handled until it
    returns.

    To close the hardware connection when the module stops, override ``stop``
    and close it after calling ``super().stop()``, which halts the robot first.
    """

    dedicated_worker: ClassVar[bool] = True

    control_state: Out[ControlValues]
    control_command: In[ControlValues]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._descriptions: dict[str, ControlDescription] = {}
        self._deadman_s = 0.0
        # Guards the status fields and the stop flag.
        self._lock = threading.Lock()
        # Held around every write() and halt(), so they never overlap, and
        # around every change to _connected.
        self._hardware_lock = threading.Lock()
        # Serialises state publishing, so sequence numbers go out in order.
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._stopping = False
        self._connected = False
        self._subscriptions: list[DisposableBase] = []
        self._state_thread: threading.Thread | None = None
        self._state_sequence: dict[str, int] = {}
        # Only touched on the module's event loop, where commands are handled.
        self._last_sequence: dict[str, int] = {}
        self._deadman_timer: asyncio.TimerHandle | None = None
        self._last_state_time: float | None = None
        self._last_command_time: float | None = None
        self._deadman_fired = False
        self._last_rejection: str | None = None
        self._last_error: str | None = None
        # When a rejection (True) or an error (False) was last logged.
        self._last_logged: dict[bool, float] = {}

    @abstractmethod
    def describe(self) -> ControlDescription | list[ControlDescription]:
        """Say what the robot is, usually with one preset from
        ``dimos.control.contract.presets`` built from ``self.config``.

        Runs before ``connect``, so it must not touch the hardware. Return a
        list when this module runs several robots, such as an arm and the base
        under it, each with its own ``source`` name.
        """

    @abstractmethod
    def connect(self) -> None:
        """Open the connection to the hardware. Must not make anything move."""

    @abstractmethod
    def read_state(self) -> dict[str, float] | None:
        """The robot's latest readings, or ``None`` if nothing new has arrived.

        Keys are full keys such as ``"arm/joint1/position"``, each value in
        its key's unit, and every key a description lists must be present. A
        driver whose SDK delivers readings in a callback returns ``None`` here
        and calls ``publish_state`` from the callback instead.
        """

    @abstractmethod
    def write(self, values: dict[str, float]) -> None:
        """Send one checked command to the hardware.

        Send and return, servo-style: never wait for a motion to finish; its
        progress shows up in the readings. Switching the hardware between ways
        of being driven (position, velocity) happens here, and any waiting a
        mode switch needs is the driver's own business. Raise if the hardware
        refuses: the module then logs it, calls ``halt`` and carries on.

        Args:
            values: The keys of one robot only, e.g.
                ``{"arm/joint1/position": 0.3}``, already within its limits.
        """

    @abstractmethod
    def halt(self) -> None:
        """Stop the robot moving now, e.g. hold its position or zero its speeds.

        Must be safe to call any number of times, including before any
        command. Called when commands stop arriving, after a failed ``write``,
        and when the module stops.
        """

    @rpc
    def start(self) -> None:
        super().start()
        described = self.describe()
        descriptions = described if isinstance(described, list) else [described]
        for description in descriptions:
            validate_description(description)
        sources = [description.source for description in descriptions]
        if not sources or len(set(sources)) != len(sources):
            raise ValueError(f"{self._label}: robot names must be present and unique: {sources}")
        self._descriptions = {description.source: description for description in descriptions}
        self._deadman_s = min(description.deadman_timeout_s for description in descriptions)
        self._state_sequence = dict.fromkeys(sources, 0)

        self.connect()
        self._connected = True
        self._subscriptions = [self._listen(source) for source in sources]
        self._state_thread = threading.Thread(
            target=self._state_loop, daemon=True, name=f"{self._label}-state"
        )
        self._state_thread.start()

    @rpc
    def stop(self) -> None:
        with self._lock:
            if self._stopping:
                return
            self._stopping = True
        # Commands are cut off before the final halt, so none can follow it.
        for subscription in self._subscriptions:
            subscription.dispose()
        self._stop_event.set()
        if self._state_thread is not None:
            self._state_thread.join(timeout=DEFAULT_THREAD_JOIN_TIMEOUT)
        with self._hardware_lock:
            if self._connected:
                self._call_halt()
            self._connected = False
        super().stop()

    @rpc
    def describe_control(self) -> list[ControlDescription]:
        """One description per robot this module runs. Empty before ``start``."""
        return list(self._descriptions.values())

    @rpc
    def status(self) -> ConnectionStatus:
        """A snapshot of the connection. See ``ConnectionStatus``."""
        with self._lock:
            return ConnectionStatus(
                connected=self._connected,
                last_state_time=self._last_state_time,
                last_command_time=self._last_command_time,
                deadman_fired=self._deadman_fired,
                last_rejection=self._last_rejection,
                last_error=self._last_error,
            )

    def publish_state(self, values: Mapping[str, float]) -> None:
        """Publish one set of readings on ``control_state``.

        Split into one frame per robot, each with its own sequence number
        counting up from 0. A robot's frame must carry exactly the keys its
        description lists; one that does not is dropped and shows up in
        ``status().last_error``. Does nothing once the module is stopping.

        Args:
            values: Readings by full key, e.g. ``{"arm/joint1/position": 0.3}``,
                each in its key's unit. Keys of several robots may be mixed.
        """
        if self._stop_event.is_set():
            return
        now = time.time()
        by_source: dict[str, dict[str, float]] = {}
        for key, value in values.items():
            by_source.setdefault(key.split(SEPARATOR, 1)[0], {})[key] = value
        for source, readings in by_source.items():
            description = self._descriptions.get(source)
            if description is None:
                self._report(f"readings for {source!r}, which this module does not run")
                continue
            try:
                frame = ControlValues(
                    source=source,
                    source_ts=now,
                    interface_names=list(readings),
                    values=list(readings.values()),
                )
                validate_state(description, frame)
            except (ValueError, FrameRejectedError) as error:
                self._report(f"bad readings for {source!r}: {error}")
                continue
            with self._state_lock:
                frame.sequence = self._state_sequence[source]
                self._state_sequence[source] += 1
                self.control_state.publish(frame)
            with self._lock:
                self._last_state_time = now

    def _listen(self, source: str) -> DisposableBase:
        """Pass frames carrying ``source``'s keys to ``_apply``, newest only.

        Filtering comes first, so another robot's frame never displaces this
        one's. Handling runs on the module's event loop.
        """
        prefix = f"{source}{SEPARATOR}"

        def carries_source(frame: ControlValues) -> bool:
            return any(name.startswith(prefix) for name in frame.interface_names)

        async def handle(frame: ControlValues) -> None:
            self._apply(source, frame)

        frames = self.control_command.pure_observable().pipe(ops.filter(carries_source))
        return self.process_observable(frames, handle)

    def _apply(self, source: str, frame: ControlValues) -> None:
        result = validate_command(
            self._descriptions[source], frame, last_sequence=self._last_sequence.get(source)
        )
        if isinstance(result, Rejected):
            self._report(f"{source}: {result.reason}: {result.detail}", rejection=True)
            return
        self._last_sequence[source] = frame.sequence
        with self._lock:
            self._last_command_time = time.time()
            self._deadman_fired = False
        self._start_deadman()
        try:
            with self._hardware_lock:
                if not self._connected:
                    return
                self.write(result.values)
        except Exception as error:
            self._report(f"write for {source!r} failed: {type(error).__name__}: {error}")
            self._cancel_deadman()
            self._halt()

    def _start_deadman(self) -> None:
        self._cancel_deadman()
        loop = asyncio.get_running_loop()
        self._deadman_timer = loop.call_later(self._deadman_s, self._deadman_expired)

    def _cancel_deadman(self) -> None:
        if self._deadman_timer is not None:
            self._deadman_timer.cancel()
            self._deadman_timer = None

    def _deadman_expired(self) -> None:
        self._deadman_timer = None
        # A halt ends the command session: a restarted sender may count from 0.
        self._last_sequence.clear()
        with self._lock:
            self._deadman_fired = True
        logger.warning(f"{self._label}: no command for {self._deadman_s} s, halting")
        self._halt()

    def _halt(self) -> None:
        with self._hardware_lock:
            if self._connected:
                self._call_halt()

    def _call_halt(self) -> None:
        try:
            self.halt()
        except Exception as error:
            self._report(f"halt failed: {type(error).__name__}: {error}")

    def _state_loop(self) -> None:
        period = 1.0 / max(d.state_rate_hz for d in self._descriptions.values())
        next_at = time.monotonic()
        while not self._stop_event.is_set():
            try:
                values = self.read_state()
                if values is not None:
                    self.publish_state(values)
            except Exception as error:
                self._report(f"read_state failed: {type(error).__name__}: {error}")
            # After falling behind, carry on from now rather than catching up.
            next_at = max(next_at + period, time.monotonic())
            self._stop_event.wait(next_at - time.monotonic())

    def _report(self, message: str, *, rejection: bool = False) -> None:
        """Record a problem for ``status``, and log it unless it repeats the
        last one or one of its kind was logged in the past second."""
        now = time.monotonic()
        with self._lock:
            previous = self._last_rejection if rejection else self._last_error
            if rejection:
                self._last_rejection = message
            else:
                self._last_error = message
            quiet = (
                message == previous or now - self._last_logged.get(rejection, float("-inf")) < 1.0
            )
            if not quiet:
                self._last_logged[rejection] = now
        if not quiet:
            logger.warning(f"{self._label}: {message}")

    @property
    def _label(self) -> str:
        return type(self).__name__
