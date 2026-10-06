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

"""Synthetic sensor blueprint for the standard CLI Rust recording workflow."""

from __future__ import annotations

from functools import partial
import hashlib
import json
import os
from pathlib import Path
import threading
from typing import Any

from reactivex.disposable import Disposable

from dimos.core.coordination.blueprints import autoconnect
from dimos.core.core import rpc
from dimos.core.global_config import global_config
from dimos.core.module import Module
from dimos.core.stream import In, Out
from dimos.memory.cli.dataset import open_dataset
from dimos.msgs.geometry_msgs.PoseStamped import PoseStamped
from dimos.msgs.sensor_msgs.Image import Image
from dimos.msgs.sensor_msgs.Imu import Imu
from dimos.protocol.service.zenohservice import LOOPBACK_INTERFACE
from dimos.utils.logging_config import setup_logger
from native_mcap_demo.values import digest, values

logger = setup_logger()


class SyntheticSensors(Module):
    imu: Out[Imu]
    pose: Out[PoseStamped]
    color_image: Out[Image]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._cancel = threading.Event()
        self._producer: threading.Thread | None = None

    @rpc
    def start(self) -> None:
        super().start()
        self._producer = threading.Thread(target=self._produce, daemon=True)
        self._producer.start()

    def _produce(self) -> None:
        index = 0
        while not self._cancel.is_set():
            for name, message in values(index).items():
                getattr(self, name).publish(message)
            index += 1
            self._cancel.wait(0.2)

    @rpc
    def stop(self) -> None:
        self._cancel.set()
        if self._producer is not None:
            self._producer.join(timeout=5)
        super().stop()


class Observer(Module):
    imu: In[Imu]
    pose: In[PoseStamped]
    color_image: In[Image]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._received: dict[str, list[str]] = {name: [] for name in ("imu", "pose", "color_image")}
        self._names = dict.fromkeys(self._received, "")
        self._expected = dict.fromkeys(self._received, 0)
        if global_config.replay_db != "go2_short":
            with open_dataset(global_config.replay_db) as store:
                for name in self._received:
                    self._names[name] = next(n for n in store.list_streams() if n.endswith(name))
                    self._expected[name] = store.stream(self._names[name]).count()

    def _observe(self, name: str, message: Any) -> None:
        self._received[name].append(digest(message))
        if all(len(self._received[key]) == count > 0 for key, count in self._expected.items()):
            logger.info("Replay received every recorded row; press Ctrl+C to finish")

    @rpc
    def start(self) -> None:
        super().start()
        for name in self._received:
            unsubscribe = getattr(self, name).subscribe(partial(self._observe, name))
            self.register_disposable(Disposable(unsubscribe))

    @rpc
    def stop(self) -> None:
        super().stop()
        result = {self._names[name] or name: hashes for name, hashes in self._received.items()}
        Path(os.environ.get("DIMOS_MCAP_DEMO_RESULT", "replay-result.json")).write_text(
            json.dumps(result)
        )


def _isolated(blueprint: Any) -> Any:
    session = os.environ["DIMOS_MCAP_DEMO_SESSION"]
    port = 17700 + int(hashlib.sha256(session.encode()).hexdigest()[:8], 16) % 5000
    return blueprint.global_config(
        transport="zenoh",
        viewer="none",
        robot_ip=None,
        robot_ips=None,
        zenoh_connect="",
        zenoh_scouting=False,
        zenoh_interface=LOOPBACK_INTERFACE,
        zenoh_scout_addr=f"224.0.0.224:{port}",
        zenoh_gossip=False,
    )


sensors = _isolated(autoconnect(SyntheticSensors.blueprint()))
watch = _isolated(autoconnect(Observer.blueprint()))
if global_config.replay_db != "go2_short":
    with open_dataset(global_config.replay_db) as recording:
        watch = watch.remappings(
            [
                (Observer, name, next(n for n in recording.list_streams() if n.endswith(name)))
                for name in ("imu", "pose", "color_image")
            ]
        )
