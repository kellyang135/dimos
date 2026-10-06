# Record and inspect synthetic sensors with the Rust engine

Use a development checkout with dimOS installed and the native Rust recorder
prepared by the normal native build workflow. This example uses current main's
LCM message encoding, not the CDR proposal. The Python example package adds only
blueprint entry points; it does not build or install dimOS.

Install the example from the repository root:

```sh skip
pip install --no-deps -e examples/native-mcap
export DIMOS_MCAP_DEMO_SESSION="$(python -c 'import uuid; print(uuid.uuid4().hex)')"
export DIMOS_MCAP_DEMO_RESULT="$PWD/replay-result.json"
```

The session selects a private loopback Zenoh discovery group. The blueprints clear
robot IP/connect settings, disable LAN scouting/gossip and contain only synthetic
sensor ports. Keep the session variable in the same terminal for replay. They
never load a robot driver or publish actuator commands. The ordinary CLI may
report host prerequisites; this example does not authorize changing host settings.
The local acceptance run used a check-only host configurator and normal SHM fallback.

## Record, stop and inspect

```sh skip
dimos --transport zenoh --viewer none --no-build-native \
  --record mcap --record-engine rust run native-mcap-demo.sensors
```

After `Experimental Rust recorder ready`, record for roughly ten seconds, then
press **Ctrl+C**. Wait for `memory recorder flushed` before inspecting the file.
The log prints its exact `artifact_path`. Set that path below:

```sh skip
export RECORDING="$PWD/recordings/<run-id>/memory.mcap"
dimos mem summary "$RECORDING"
dimos mem rerun "$RECORDING" --no-gui --out "$PWD/recording.rrd"
python -m native_mcap_demo "$RECORDING"
```

Summary shows typed `imu`, `pose` and `color_image` streams. Rerun exports the image,
pose and the IMU type's existing axes representation.
The validator checks message types, deterministic sensor values and source
stamps. Image storage is JPEG and is checked with a pixel error bound. Source
publication begins before recorder readiness, so the opening source frames can
be absent; this is not a complete finite capture guarantee.

## Replay into another worker and compare

```sh skip
dimos --transport zenoh --viewer none --no-build-native \
  --replay-db "$RECORDING" run native-mcap-demo.watch replay --replay.speed=0.5
```

Wait for `Replay received every recorded row; press Ctrl+C to finish`, then press
**Ctrl+C**. The observer writes the result file during graceful shutdown:

```sh skip
python -m native_mcap_demo "$RECORDING" --replay-result "$DIMOS_MCAP_DEMO_RESULT"
```

The comparison requires every decoded payload, in order, to match the recording.
The explicit 0.5x speed leaves room for existing replay startup behavior; this
example does not fix or promise default-speed lossless playback. Generic replay
also includes the existing websocket visualization module even with `--viewer
none`; it does not launch a Rerun GUI.

`--replay` is not needed for generic `run replay`. It is a Boolean robot-connection
switch, used separately by `run unitree-go2`, whose replay input remains SQLite.
Do not replace this synthetic blueprint with a robot blueprint for this check.

## Legacy recordings

Native recordings use the generic typed `McapStore`. Legacy Go2 DDS channels keep
existing topic aliases and decoders through `open_dataset()`. Direct
`Go2McapStore` construction is deprecated; the codecs and class remain available.
This patch does not convert old files or expand Unitree schemas. An eventual
converter must report unsupported/lossy mappings and preserve original files.
