# Recording

`--record` writes the selected streams from a blueprint to one recording artifact.
The stable default remains the Python SQLite recorder. The Rust engine is an
explicit experimental option while it is evaluated.

```bash
dimos --record --simulation run unitree-go2
dimos --record --robot-ip 192.168.123.161 run unitree-go2
```

Bare `--record` means `--record sqlite`. SQLite lands at
`recordings/<run-id>/memory.db`; MCAP lands at
`recordings/<run-id>/memory.mcap`. The root is under the checkout, or
`~/.local/state/dimos/recordings/` for an installed package. `<run-id>` is the
same `YYYYMMDD-HHMMSS-<blueprint>` used by the run's `logs/` directory.

## Experimental Rust engine

Select the native engine explicitly:

```bash
dimos --record sqlite --record-engine rust run unitree-go2
dimos --record mcap --record-engine rust run unitree-go2
dimos --record mcap --record-engine rust --record-encoding-threads 8 run unitree-go2
```

`--record-encoding-threads` defaults to `4` and is valid only for the Rust
engine. Python remains the default because the native recorder is experimental;
MCAP recording currently requires the Rust engine.

The Rust engine records exact `LCMTransport` and `ZenohTransport` streams. It
rejects SHM, DDS, ROS, WebRTC, pickled, JPEG-transport, mixed LCM/Zenoh, and
other specialized transports before creating an artifact. Narrow
`--record-topics` or use the Python engine when a selection contains one of
those transports. Payloads must also be dimOS LCM message types.

The native process must report ready within 10 seconds, so build, configuration,
and subscription failures stop startup. If it exits unexpectedly after startup,
the error is logged and the rest of `dimos run` continues. Normal shutdown sends
SIGTERM and lets the existing native module runtime flush the artifact. There is
no automatic fallback to Python.

## Choosing streams

`--record-topics` takes comma-separated globs on the stream name (the blueprint name, e.g. `lidar`, not `/lidar`). Default `*`.

```bash
dimos --record --record-topics color_image run unitree-go2
dimos --record --record-topics lidar,odom,tf run unitree-go2
dimos --record --record-topics 'global_*' run unitree-go2
```

A pattern that matches no stream throws an error at startup, listing the valid stream names of the given blueprint.

Streams whose type is not a dimOS message (`Any`, `dict`) are not recorded. If
none of the selected streams is recordable, startup fails.

## Inspecting and replaying

View contents of the memory by stream:
```console
$ dimos mem summary recordings/<run-id>/memory.db

┏━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Stream         ┃  Items ┃    Hz ┃ Start (UTC)         ┃ Duration ┃       Size ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━┩
│ lidar          │     43 │   2.0 │ 2026-08-27 03:39:06 │    21.2s │   5.14 MiB │
│ color_image    │    376 │  17.3 │ 2026-08-27 03:39:06 │    21.6s │   4.26 MiB │
│ global_map     │      9 │   0.4 │ 2026-08-27 03:39:07 │    20.2s │   3.08 MiB │
│ global_costmap │      9 │   0.4 │ 2026-08-27 03:39:07 │    20.2s │ 421.03 KiB │
│ tf             │    981 │  45.2 │ 2026-08-27 03:39:06 │    21.7s │ 292.19 KiB │
│ cmd_vel        │  1,855 │  99.7 │ 2026-08-27 03:39:07 │    18.6s │ 101.45 KiB │
│ tele_cmd_vel   │  1,855 │  98.6 │ 2026-08-27 03:39:07 │    18.8s │ 101.45 KiB │
│ goal           │  1,855 │  98.9 │ 2026-08-27 03:39:07 │    18.8s │  94.20 KiB │
│ way_point      │  1,855 │  98.9 │ 2026-08-27 03:39:07 │    18.8s │  94.20 KiB │
│ odom           │    981 │  45.2 │ 2026-08-27 03:39:06 │    21.7s │  82.39 KiB │
│ stop_movement  │  1,855 │  98.9 │ 2026-08-27 03:39:07 │    18.7s │  16.30 KiB │
│ camera_info    │     24 │     - │ 2026-08-27 03:38:56 │     0.0s │   8.67 KiB │
│ nav_cmd_vel    │      3 │  32.8 │ 2026-08-27 03:39:28 │     0.1s │   168.00 B │
│ goal_request   │      1 │     - │ 2026-08-27 03:39:28 │     0.0s │    86.00 B │
│ path           │      2 │ 289.1 │ 2026-08-27 03:39:28 │     0.0s │    68.00 B │
│ goal_reached   │      1 │     - │ 2026-08-27 03:39:28 │     0.0s │     9.00 B │
├────────────────┼────────┼───────┼─────────────────────┼──────────┼────────────┤
│ total          │ 11,705 │       │                     │          │  13.66 MiB │
└────────────────┴────────┴───────┴─────────────────────┴──────────┴────────────┘
```
Replay memory from DB:
```bash
dimos --replay --replay-db recordings/<run-id>/memory.db run unitree-go2
```

`--replay` swaps the robot connection for the recording; it needs `lidar`, `odom`, and `color_image`, so record all streams (the default) if you intend to replay. Poses are not stored per frame; `tf` is recorded like any other stream and `dimos map global` uses it to register clouds. `dimos map pose-fill` instead derives poses from `odom` by default.

## Behavior

- Off unless `--record`; never active under `--replay`.
- The Python recorder uses one writer thread. Its queue holds 1000 messages, then
  drops and warns. The Rust recorder uses its existing native encoding pool and
  ordered writer pipeline.
- We also still have explicit recorder modules (`unitree-go2-memory`, `unitree-go2-mid360-record`, `unitree-g1-record`) that are unaffected and still record their own streams. These will be deprecated shortly.


## Rust MCAP inspection and generic replay

Native MCAP recordings are opened by the generic memory reader. Built-in LCM,
LZ4+LCM, JPEG and JSON String channels select their decoder from the channel
encoding/type metadata; unrecognized types remain raw bytes. Legacy Go2 DDS
recordings retain their aliases and compatibility decoders. Prefer
`dimos.memory.cli.dataset.open_dataset()` over direct `Go2McapStore` construction,
which now emits a deprecation warning. No file conversion or legacy codec removal
is part of this change.

For a reproducible synthetic record → Ctrl+C/flush → summary → Rerun export →
typed replay check, see [the native MCAP example](/examples/native-mcap/README.md).
It uses the standard CLI and an isolated loopback session, without a robot driver.

```sh skip
dimos mem summary recording.mcap
dimos mem rerun recording.mcap --no-gui --out recording.rrd
dimos --replay-db recording.mcap run replay
```

Generic replay republishes recorded streams. Use an isolated environment when a
file may contain command topics. `--replay` is a Boolean connection selector,
not a filename; Go2's `--replay --replay-db recording.db run unitree-go2` path
remains SQLite-only.
