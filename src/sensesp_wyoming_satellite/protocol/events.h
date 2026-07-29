#pragma once

// Wyoming protocol events the satellite cares about — builders for the
// events we SEND and light parsers for the fields we READ. Mirrors
// signalk-wyoming/src/protocol/events.ts, but only the subset a satellite
// needs (the orchestrator implements the rest).
//
// `width` is BYTES per sample (2 = 16-bit signed little-endian PCM).

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "sensesp_wyoming_satellite/protocol/framing.h"

namespace sensesp_wyoming {

struct AudioFormat {
  uint32_t rate = 16000;
  uint8_t width = 2;
  uint8_t channels = 1;
  bool valid() const { return rate > 0 && width > 0 && channels > 0; }
};

// What this satellite advertises in its `info` reply to `describe`.
struct SatelliteInfo {
  std::string name = "cockpit";
  AudioFormat mic_format;   // capture format we can stream (Phase 2)
  AudioFormat snd_format;   // playback format we expect in audio-start
  bool supports_trigger = true;  // orchestrator may trigger a pipeline
  // active_wake_words stays empty until Phase 3.
};

// ---- Builders: append the encoded event to `out` -------------------------

// `info` reply. Seven program lists are all present (asr/tts/handle/intent/
// wake empty; mic + snd advertise our formats); plus a satellite artifact.
void build_info(std::vector<uint8_t>& out, const SatelliteInfo& info);

// `pong` — echoes the ping's optional text (pass "" for none).
void build_pong(std::vector<uint8_t>& out, const std::string& text);

// `played` — playback of an audio-start..audio-stop span finished.
void build_played(std::vector<uint8_t>& out);

// ---- Voice-in (push-to-talk) builders ------------------------------------

// `run-pipeline` — ask the orchestrator to run asr..tts on the audio we're
// about to stream. `name` identifies this satellite.
void build_run_pipeline(std::vector<uint8_t>& out, const std::string& name);

// `audio-start` / `audio-chunk` / `audio-stop` for the OUTBOUND mic stream.
void build_audio_start(std::vector<uint8_t>& out, const AudioFormat& fmt);
void build_audio_chunk(std::vector<uint8_t>& out, const AudioFormat& fmt,
                       const int16_t* samples, size_t frames);
void build_audio_stop(std::vector<uint8_t>& out);

// ---- Parsers: pull the fields we need from a DecodedEvent ----------------

// Parse a `transcript` event's text. Returns false if not a transcript.
bool parse_transcript(const DecodedEvent& ev, std::string* text);

// Parse an audio-start's format. Returns false if the event isn't
// audio-start or the format is unusable.
bool parse_audio_start(const DecodedEvent& ev, AudioFormat* out);

// Extract the optional `text` field of a ping (empty if absent).
std::string parse_ping_text(const DecodedEvent& ev);

}  // namespace sensesp_wyoming
