#include "sensesp_wyoming_satellite/wyoming_satellite.h"

#include <cerrno>
#include <cstring>

#include "esp_log.h"
#include "lwip/sockets.h"

namespace sensesp_wyoming {

namespace {
constexpr const char* kTag = "wyoming_sat";
}  // namespace

WyomingSatellite::WyomingSatellite(
    sensesp_cockpit_display::AudioDriver* audio,
    const WyomingSatelliteConfig& config)
    : audio_(audio), config_(config) {}

WyomingSatellite::~WyomingSatellite() { stop(); }

void WyomingSatellite::start() {
  if (running_.exchange(true)) return;
  xTaskCreate(&WyomingSatellite::server_task, "wyoming_sat", 6144, this, 3,
              &server_task_);
  ESP_LOGI(kTag, "Wyoming satellite starting on port %u", config_.port);
}

void WyomingSatellite::stop() {
  if (!running_.exchange(false)) return;
  // Nudge a blocked accept()/recv() to notice running_==false: closing the
  // client socket unblocks recv; the accept loop wakes on its select tick.
  if (client_sock_ >= 0) {
    shutdown(client_sock_, SHUT_RDWR);
  }
  if (server_task_) {
    // The task exits on its next select/recv tick (≤1s accept, ≤500ms recv).
    vTaskDelay(pdMS_TO_TICKS(1200));
    server_task_ = nullptr;
  }
}

void WyomingSatellite::server_task(void* arg) {
  static_cast<WyomingSatellite*>(arg)->serve();
  vTaskDelete(nullptr);
}

void WyomingSatellite::serve() {
  int listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listen_sock < 0) {
    ESP_LOGE(kTag, "socket() failed: %d", errno);
    return;
  }
  int opt = 1;
  setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(config_.port);
  addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(listen_sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
    ESP_LOGE(kTag, "bind() failed: %d", errno);
    close(listen_sock);
    return;
  }
  if (listen(listen_sock, 1) != 0) {
    ESP_LOGE(kTag, "listen() failed: %d", errno);
    close(listen_sock);
    return;
  }
  ESP_LOGI(kTag, "Listening on port %u", config_.port);

  while (running_.load()) {
    struct timeval tv = {.tv_sec = 1, .tv_usec = 0};
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(listen_sock, &fds);
    int sel = select(listen_sock + 1, &fds, nullptr, nullptr, &tv);
    if (sel <= 0) continue;

    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int sock = accept(listen_sock, (struct sockaddr*)&client_addr, &client_len);
    if (sock < 0) continue;

    // Single-client: if one is already connected, refuse the newcomer.
    if (client_connected_.load()) {
      ESP_LOGW(kTag, "second client refused (single-client satellite)");
      close(sock);
      continue;
    }

    char ip[16];
    inet_ntoa_r(client_addr.sin_addr, ip, sizeof(ip));
    ESP_LOGI(kTag, "orchestrator connected from %s", ip);
    handle_client(sock);
    ESP_LOGI(kTag, "orchestrator disconnected");
  }

  close(listen_sock);
}

void WyomingSatellite::handle_client(int sock) {
  client_sock_ = sock;
  client_connected_.store(true);
  streaming_ = false;

  // Short read timeout so an inbound ping is answered well within the 5s
  // deadline even when nothing else is arriving.
  struct timeval rcv_tv = {.tv_sec = 0, .tv_usec = 300000};  // 300 ms
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &rcv_tv, sizeof(rcv_tv));
  struct timeval snd_tv = {.tv_sec = 5, .tv_usec = 0};  // playback can pace
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &snd_tv, sizeof(snd_tv));

  EventDecoder decoder;
  uint8_t recv_buf[1600];

  while (running_.load()) {
    int n = recv(sock, recv_buf, sizeof(recv_buf), 0);
    if (n > 0) {
      if (!decoder.feed(recv_buf, (size_t)n, &WyomingSatellite::on_event_tramp,
                        this)) {
        ESP_LOGW(kTag, "framing error / handler abort — dropping connection");
        break;
      }
    } else if (n == 0) {
      break;  // clean close
    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
      break;  // real error (timeout is EAGAIN/EWOULDBLOCK — keep looping)
    }
  }

  // Guarantee the codec mutex is released if we die mid-utterance.
  if (streaming_) {
    audio_->end_stream();
    streaming_ = false;
  }
  close(sock);
  client_sock_ = -1;
  client_connected_.store(false);
}

bool WyomingSatellite::on_event_tramp(void* ctx, const DecodedEvent& ev) {
  return static_cast<WyomingSatellite*>(ctx)->on_event(ev);
}

bool WyomingSatellite::on_event(const DecodedEvent& ev) {
  const std::string& t = ev.type;

  if (t == "describe") {
    SatelliteInfo info;
    info.name = config_.name;
    info.mic_format = {16000, 2, 1};  // capture format (Phase 2)
    info.snd_format = {config_.snd_rate, 2, 1};
    std::vector<uint8_t> out;
    build_info(out, info);
    return send_all(out);
  }

  if (t == "ping") {
    std::vector<uint8_t> out;
    build_pong(out, parse_ping_text(ev));
    return send_all(out);
  }

  if (t == "run-satellite" || t == "pause-satellite") {
    // Output-only Phase 1: nothing to arm. We already play whatever is
    // streamed regardless of run/pause (upstream routes audio-* to snd
    // unconditionally). Just acknowledge by continuing.
    ESP_LOGI(kTag, "%s", t.c_str());
    return true;
  }

  if (t == "audio-start") {
    AudioFormat fmt;
    if (!parse_audio_start(ev, &fmt)) {
      ESP_LOGW(kTag, "audio-start with bad/absent format — ignoring");
      return true;
    }
    play_fmt_ = fmt;
    if (!audio_->begin_stream(fmt.rate, fmt.width * 8, fmt.channels)) {
      ESP_LOGW(kTag, "begin_stream refused %lu Hz/%uch — playback skipped",
               (unsigned long)fmt.rate, fmt.channels);
      streaming_ = false;
    } else {
      streaming_ = true;
      ESP_LOGI(kTag, "audio-start %lu Hz", (unsigned long)fmt.rate);
    }
    return true;
  }

  if (t == "audio-chunk") {
    if (streaming_ && ev.payload && ev.payload_len >= 2) {
      // Payload is signed-16 LE mono PCM (width=2, channels=1). Feed it to
      // the blocking streaming sink — the block is our backpressure.
      const int16_t* pcm = reinterpret_cast<const int16_t*>(ev.payload);
      size_t frames = ev.payload_len / 2;
      audio_->write_stream(pcm, frames);
    }
    return true;
  }

  if (t == "audio-stop") {
    if (streaming_) {
      audio_->end_stream();
      streaming_ = false;
    }
    std::vector<uint8_t> out;
    build_played(out);
    ESP_LOGI(kTag, "audio-stop -> played");
    return send_all(out);
  }

  // transcript / detection / everything else: ignored in Phase 1
  // (forward-compatible — unknown events are not errors).
  return true;
}

bool WyomingSatellite::send_all(const std::vector<uint8_t>& bytes) {
  size_t off = 0;
  while (off < bytes.size()) {
    int sent = send(client_sock_, bytes.data() + off, bytes.size() - off,
                    MSG_NOSIGNAL);
    if (sent <= 0) {
      if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        // Send buffer full — brief yield, then retry (don't drop protocol
        // frames the way a disposable candump line can be dropped).
        vTaskDelay(pdMS_TO_TICKS(5));
        continue;
      }
      ESP_LOGW(kTag, "send failed: %d", errno);
      return false;
    }
    off += (size_t)sent;
  }
  return true;
}

}  // namespace sensesp_wyoming
