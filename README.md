> [!IMPORTANT]
> **Archived — this library is no longer maintained.**
>
> The Wyoming voice satellite now lives in
> [espOS](https://github.com/dirkwa/espOS) as two reusable ESP-IDF
> components: **`espos_audio`** (the `AudioDriver` contract a board
> implements) and **`espos_voice`** (protocol, TCP server, esp-sr
> WakeNet engine). See
> [docs/voice.md](https://github.com/dirkwa/espOS/blob/main/docs/voice.md).
>
> This library targets SensESP on PlatformIO. Everything using it moved
> to ESP-IDF 6 on espOS, and the code was folded into
> [espos-p4-cockpit](https://github.com/dirkwa/espos-p4-cockpit) during
> that port. The two copies then diverged — the cockpit gained fixes this
> one never received, including one for the wake fetch loop starving the
> idle task on CPU 0. Rather than keep a third copy in sync, the code was
> extracted into espOS so any espOS board can use it.
>
> Kept for history. Please do not build against it.

# sensesp-wyoming-satellite

Wyoming-protocol voice satellite for [SensESP](https://github.com/SignalK/SensESP):
a TCP server that lets a signalk-wyoming / Home Assistant orchestrator play TTS
to, and stream microphone audio from, an ESP32 board's audio codec (ES8311 on
the ESP32-P4 boards used by
[sensesp-p4-cockpit](https://github.com/dirkwa/sensesp-p4-cockpit)).

## Layout

- `src/sensesp_wyoming_satellite/wyoming_satellite.{h,cpp}` — the satellite:
  Wyoming TCP server, audio playback and capture
- `src/sensesp_wyoming_satellite/wake_engine.{h,cpp}` — wake-word engine
- `src/sensesp_wyoming_satellite/protocol/` — Wyoming protocol framing

## Usage

This library has no build target of its own — it is compiled by the PlatformIO
project that pulls it in (`lib_deps = symlink://../sensesp-wyoming-satellite`).
The reference downstream is
[sensesp-p4-cockpit](https://github.com/dirkwa/sensesp-p4-cockpit).

## License

sensesp-wyoming-satellite 1.0.0 and later is **source available, not open
source**. See [LICENSE.md](LICENSE.md).

**You may**, free of charge: run it on your own boat or fleet, private or
commercial; use it for internal company operations; modify it for your own use;
use it in education and research; and provide professional services around it.

**You may not**: redistribute it, or publish a modified version of it to the
PlatformIO registry, the Arduino library index or anywhere else. Verbatim
copies of official releases may be mirrored and cached.

Before 1.0.0 this library declared MIT (never tagged or released); that state
remains available under MIT, see
[LICENSE-MIT-through-v0.x.txt](LICENSE-MIT-through-v0.x.txt).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
