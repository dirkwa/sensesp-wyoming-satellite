#include "sensesp_wyoming_satellite/protocol/framing.h"

#include <cstring>

#include "esp_log.h"

namespace sensesp_wyoming {

namespace {
constexpr const char* kTag = "wyoming_frame";
}  // namespace

void encode_event(std::vector<uint8_t>& out, const char* type,
                  const std::string& data_json, const uint8_t* payload,
                  size_t payload_len) {
  // The header is itself JSON. Build it with a small ArduinoJson doc so
  // escaping is correct, then splice the data block / payload after it.
  const bool has_data = !data_json.empty() && data_json != "{}";

  JsonDocument header;
  header["type"] = type;
  header["version"] = kWyomingVersion;
  if (has_data) header["data_length"] = (uint32_t)data_json.size();
  if (payload && payload_len > 0) {
    header["payload_length"] = (uint32_t)payload_len;
  }

  std::string header_line;
  serializeJson(header, header_line);
  header_line.push_back('\n');

  out.insert(out.end(), header_line.begin(), header_line.end());
  if (has_data) out.insert(out.end(), data_json.begin(), data_json.end());
  if (payload && payload_len > 0) {
    out.insert(out.end(), payload, payload + payload_len);
  }
}

void encode_event(std::vector<uint8_t>& out, const char* type) {
  encode_event(out, type, std::string(), nullptr, 0);
}

void EventDecoder::compact() {
  if (pos_ > 0) {
    buf_.erase(buf_.begin(), buf_.begin() + pos_);
    pos_ = 0;
  }
}

bool EventDecoder::read_header() {
  // Find the newline that terminates the header line.
  size_t nl = std::string::npos;
  for (size_t i = pos_; i < buf_.size(); ++i) {
    if (buf_[i] == '\n') {
      nl = i;
      break;
    }
  }
  if (nl == std::string::npos) {
    if (buf_.size() - pos_ > kMaxHeaderBytes) {
      ESP_LOGE(kTag, "header line exceeds %u bytes", (unsigned)kMaxHeaderBytes);
      failed_ = true;
    }
    return false;  // need more bytes
  }

  std::string line((const char*)&buf_[pos_], nl - pos_);
  pos_ = nl + 1;

  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, line);
  if (err || !doc.is<JsonObject>()) {
    ESP_LOGE(kTag, "malformed header JSON: %s", err.c_str());
    failed_ = true;
    return false;
  }

  const char* type = doc["type"];
  if (!type || type[0] == '\0') {
    ESP_LOGE(kTag, "header missing \"type\"");
    failed_ = true;
    return false;
  }
  type_ = type;

  // Lengths: absent/null => 0. Reject negatives / oversize.
  long data_len = doc["data_length"] | 0;
  long payload_len = doc["payload_length"] | 0;
  if (data_len < 0 || payload_len < 0 ||
      (size_t)data_len > kMaxDataBytes) {
    ESP_LOGE(kTag, "invalid length fields (data=%ld payload=%ld)", data_len,
             payload_len);
    failed_ = true;
    return false;
  }
  data_len_ = (size_t)data_len;
  payload_len_ = (size_t)payload_len;

  // Inline header `data` object is accepted on read (never written by the
  // reference); the out-of-line block, if present, wins over it.
  inline_data_json_.clear();
  JsonVariantConst inline_data = doc["data"];
  if (inline_data.is<JsonObjectConst>()) {
    serializeJson(inline_data, inline_data_json_);
  }

  have_header_ = true;
  return true;
}

bool EventDecoder::feed(const uint8_t* chunk, size_t len, EventFn on_event,
                        void* ctx) {
  if (failed_) return false;
  if (chunk && len) buf_.insert(buf_.end(), chunk, chunk + len);

  for (;;) {
    if (!have_header_) {
      if (!read_header()) break;  // need more bytes, or failed_
      if (failed_) return false;
    }

    // Need the whole data block + payload buffered before we emit.
    size_t need = data_len_ + payload_len_;
    if (buf_.size() - pos_ < need) break;  // need more bytes

    DecodedEvent ev;
    ev.type = type_;
    if (data_len_ > 0) {
      ev.data_json.assign((const char*)&buf_[pos_], data_len_);
    } else if (!inline_data_json_.empty()) {
      ev.data_json = inline_data_json_;
    }
    if (payload_len_ > 0) {
      ev.payload = &buf_[pos_ + data_len_];
      ev.payload_len = payload_len_;
    }

    bool ok = on_event(ctx, ev);

    // Consume this event's bytes.
    pos_ += need;
    have_header_ = false;
    type_.clear();
    inline_data_json_.clear();
    data_len_ = 0;
    payload_len_ = 0;

    if (!ok) return false;  // handler asked us to stop / drop the connection
  }

  compact();
  return !failed_;
}

}  // namespace sensesp_wyoming
