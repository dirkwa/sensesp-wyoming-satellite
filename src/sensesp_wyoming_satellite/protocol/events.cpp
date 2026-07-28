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
