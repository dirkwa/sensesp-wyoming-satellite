#include "sensesp_wyoming_satellite/protocol/events.h"

#include "ArduinoJson.h"

namespace sensesp_wyoming {

namespace {

void add_format(JsonObject obj, const char* key, const AudioFormat& fmt) {
  JsonObject f = obj[key].to<JsonObject>();
  f["rate"] = fmt.rate;
  f["width"] = fmt.width;
  f["channels"] = fmt.channels;
}

}  // namespace

void build_info(std::vector<uint8_t>& out, const SatelliteInfo& info) {
  JsonDocument doc;
  // Seven program lists are always present (parseInfo tolerates absence,
  // but the reference always serializes them).
  doc["asr"].to<JsonArray>();
  doc["tts"].to<JsonArray>();
  doc["handle"].to<JsonArray>();
  doc["intent"].to<JsonArray>();
  doc["wake"].to<JsonArray>();

  // We can capture: advertise one mic program with our capture format.
  JsonObject mic = doc["mic"].add<JsonObject>();
  mic["name"] = info.name;
  add_format(mic, "mic_format", info.mic_format);

  // We can play: advertise one snd program with our playback format.
  JsonObject snd = doc["snd"].add<JsonObject>();
  snd["name"] = info.name;
  add_format(snd, "snd_format", info.snd_format);

  JsonObject sat = doc["satellite"].to<JsonObject>();
  sat["name"] = info.name;
  sat["active_wake_words"].to<JsonArray>();  // empty until Phase 3
  sat["supports_trigger"] = info.supports_trigger;

  std::string data_json;
  serializeJson(doc, data_json);
  encode_event(out, "info", data_json, nullptr, 0);
}

void build_pong(std::vector<uint8_t>& out, const std::string& text) {
  // Reference serializes `text` as null when unset.
  JsonDocument doc;
  if (text.empty()) {
    doc["text"] = nullptr;
  } else {
    doc["text"] = text;
  }
  std::string data_json;
  serializeJson(doc, data_json);
  encode_event(out, "pong", data_json, nullptr, 0);
}

void build_played(std::vector<uint8_t>& out) { encode_event(out, "played"); }

void build_run_pipeline(std::vector<uint8_t>& out, const std::string& name) {
  // asr..tts: transcribe our mic audio and (if a reply is synthesised) play
  // it back. The orchestrator segments/endpoints the utterance itself.
  JsonDocument doc;
  doc["start_stage"] = "asr";
  doc["end_stage"] = "tts";
  doc["restart_on_end"] = false;
  if (!name.empty()) doc["name"] = name;
  std::string data_json;
  serializeJson(doc, data_json);
  encode_event(out, "run-pipeline", data_json, nullptr, 0);
}

namespace {
std::string audio_format_json(const AudioFormat& fmt, uint32_t timestamp_ms,
                              bool with_ts) {
  JsonDocument doc;
  doc["rate"] = fmt.rate;
  doc["width"] = fmt.width;
  doc["channels"] = fmt.channels;
  if (with_ts) doc["timestamp"] = timestamp_ms;
  std::string s;
  serializeJson(doc, s);
  return s;
}
}  // namespace

void build_audio_start(std::vector<uint8_t>& out, const AudioFormat& fmt) {
  encode_event(out, "audio-start", audio_format_json(fmt, 0, false), nullptr,
               0);
}

void build_audio_chunk(std::vector<uint8_t>& out, const AudioFormat& fmt,
                       const int16_t* samples, size_t frames) {
  const uint8_t* payload = reinterpret_cast<const uint8_t*>(samples);
  encode_event(out, "audio-chunk", audio_format_json(fmt, 0, false), payload,
               frames * sizeof(int16_t));
}

void build_audio_stop(std::vector<uint8_t>& out) {
  encode_event(out, "audio-stop");
}

bool parse_transcript(const DecodedEvent& ev, std::string* text) {
  if (ev.type != "transcript" || ev.data_json.empty()) return false;
  JsonDocument doc;
  if (deserializeJson(doc, ev.data_json)) return false;
  const char* t = doc["text"];
  if (!t) return false;
  *text = t;
  return true;
}

bool parse_audio_start(const DecodedEvent& ev, AudioFormat* out) {
  if (ev.type != "audio-start" || ev.data_json.empty()) return false;
  JsonDocument doc;
  if (deserializeJson(doc, ev.data_json)) return false;
  AudioFormat fmt;
  fmt.rate = doc["rate"] | 0u;
  fmt.width = doc["width"] | 0;
  fmt.channels = doc["channels"] | 0;
  if (!fmt.valid()) return false;
  *out = fmt;
  return true;
}

std::string parse_ping_text(const DecodedEvent& ev) {
  if (ev.data_json.empty()) return std::string();
  JsonDocument doc;
  if (deserializeJson(doc, ev.data_json)) return std::string();
  const char* t = doc["text"];
  return t ? std::string(t) : std::string();
}

}  // namespace sensesp_wyoming
