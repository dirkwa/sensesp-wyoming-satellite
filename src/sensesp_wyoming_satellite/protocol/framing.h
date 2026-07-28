#pragma once

// Wyoming wire framing — encoder + incremental decoder.
//
// Wire format (reference rhasspy/wyoming 1.10.0, wyoming/event.py):
//
//     <header JSON, one line, UTF-8>\n
//     <data JSON, exactly data_length bytes>     (only when data_length > 0)
//     <payload, exactly payload_length bytes>    (only when payload_length > 0)
//
// - data_length / payload_length are BYTE counts (UTF-8), omitted entirely
//   when the data dict / payload is empty.
// - No newline after the data block or payload; the next header starts
//   immediately after the payload's last byte.
// - `version` in the header is informational only.
//
// This is a faithful C++ port of signalk-wyoming/src/protocol/framing.ts,
// tuned for an ESP32 satellite: it decodes the small header/data events the
// orchestrator sends and hands the (potentially large) PCM payload back by
// reference into the decode buffer, so audio-chunks aren't copied twice.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "ArduinoJson.h"

namespace sensesp_wyoming {

// Protocol version written into every header we send (never validated by
// readers, but wyoming logs a warning outside 1.x).
constexpr const char* kWyomingVersion = "1.10.0";

// A decoded event: type + the raw data-block bytes (empty when absent) +
// a view of the payload bytes. `payload`/`payload_len` point INTO the
// decoder's buffer and are only valid until the next feed() call — copy
// them out (or consume them synchronously) before feeding more.
struct DecodedEvent {
  std::string type;
  // Raw UTF-8 JSON of the data block (or inline header data), or empty.
  std::string data_json;
  const uint8_t* payload = nullptr;
  size_t payload_len = 0;
};

// Encode one event to wire bytes, appended to `out`.
// `data_json` is the exact UTF-8 JSON for the data block ("" or "{}" => no
// data block). `payload`/`payload_len` is the optional binary payload.
void encode_event(std::vector<uint8_t>& out, const char* type,
                  const std::string& data_json, const uint8_t* payload,
                  size_t payload_len);

// Convenience: encode a header-only event (no data, no payload).
void encode_event(std::vector<uint8_t>& out, const char* type);

// Incremental push-parser. Feed it socket chunks; it invokes `on_event`
// for every complete event. Returns false on a framing violation (caller
// must drop the connection); true otherwise (including "need more bytes").
//
// Kept as a class so partial reads buffer across feed() calls. The buffer
// holds at most one in-flight event's bytes plus whatever trailed it.
class EventDecoder {
 public:
  // Callback receives a DecodedEvent whose payload view is valid only for
  // the duration of the call. Return false to abort decoding (treated like
  // a framing error by feed()).
  using EventFn = bool (*)(void* ctx, const DecodedEvent& ev);

  // Guardrails (reference asyncio default header limit is 64 KiB; we cap
  // the header line smaller and the data block generously, and stream the
  // payload so it never has to fit a fixed cap).
  static constexpr size_t kMaxHeaderBytes = 8 * 1024;
  static constexpr size_t kMaxDataBytes = 64 * 1024;

  // feed() appends `chunk` and drains as many complete events as possible,
  // calling on_event(ctx, ev) for each. Returns false on framing error.
  bool feed(const uint8_t* chunk, size_t len, EventFn on_event, void* ctx);

  bool failed() const { return failed_; }

 private:
  bool read_header();  // returns true if a header was parsed
  void compact();

  std::vector<uint8_t> buf_;
  size_t pos_ = 0;
  bool failed_ = false;

  // Pending header state (valid once have_header_).
  bool have_header_ = false;
  std::string type_;
  std::string inline_data_json_;  // data carried inline in the header
  size_t data_len_ = 0;
  size_t payload_len_ = 0;
};

}  // namespace sensesp_wyoming
