#include "sensesp_wyoming_satellite/wyoming_satellite.h"

#include <cerrno>
#include <cmath>
#include <cstdlib>
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
    : audio_(audio), config_(config) {
  send_mutex_ = xSemaphoreCreateMutex();
}

WyomingSatellite::~WyomingSatellite() {
  stop();
  if (send_mutex_) vSemaphoreDelete(send_mutex_);
}

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
  armed_ = false;
  listening_.store(false);
  ptt_pending_.store(false);
  set_state(SatState::Idle);

  // Short read timeout so an inbound ping is answered well within the 5s
  // deadline even when nothing else is arriving.
  struct timeval rcv_tv = {.tv_sec = 0, .tv_usec = 300000};  // 300 ms
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &rcv_tv, sizeof(rcv_tv));
  struct timeval snd_tv = {.tv_sec = 5, .tv_usec = 0};  // playback can pace
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &snd_tv, sizeof(snd_tv));

  EventDecoder decoder;
  uint8_t recv_buf[1600];

  while (running_.load()) {
    // A push-to-talk request arrives asynchronously (UI task). Start the mic
    // stream here on the satellite task so all socket writes stay serialised.
    if (ptt_pending_.exchange(false)) {
      if (armed_ && !listening_.load() && !streaming_) {
        std::vector<uint8_t> rp;
        build_run_pipeline(rp, config_.name);
        if (send_all(rp)) {
          listening_.store(true);
          set_state(SatState::Listening);
          if (xTaskCreate(&WyomingSatellite::mic_task, "wyoming_mic", 4096,
                          this, 4, &mic_task_) != pdPASS) {
            ESP_LOGW(kTag, "mic task create failed");
            listening_.store(false);
            set_state(SatState::Idle);
          } else {
            ESP_LOGI(kTag, "PTT: listening");
          }
        }
      } else {
        ESP_LOGW(kTag, "PTT ignored (armed=%d listening=%d speaking=%d)",
                 armed_, (int)listening_.load(), (int)streaming_);
      }
    }

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

  // Stop the mic task and release the playback codec if we die mid-action.
  listening_.store(false);
  if (mic_task_) {
    // run_mic exits when listening_ is false; give it a tick to finish its
    // current record/stop, then reclaim the handle.
    for (int i = 0; i < 20 && mic_task_; ++i) vTaskDelay(pdMS_TO_TICKS(10));
  }
  if (streaming_) {
    audio_->end_stream();
    streaming_ = false;
  }
  close(sock);
  client_sock_ = -1;
  client_connected_.store(false);
  set_state(SatState::Disconnected);
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

  if (t == "run-satellite") {
    // Armed: push-to-talk voice-in is now allowed. Playback works either way.
    armed_ = true;
    ESP_LOGI(kTag, "run-satellite (armed for voice-in)");
    return true;
  }
  if (t == "pause-satellite") {
    // Output-only: keep mic off the wire. Playback still works.
    armed_ = false;
    listening_.store(false);
    ESP_LOGI(kTag, "pause-satellite (output only)");
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

  if (t == "transcript") {
    std::string text;
    if (parse_transcript(ev, &text)) {
      // End of the utterance: stop streaming mic audio, play the done sound,
      // surface the text to the UI. The orchestrator publishes it to
      // SignalK's voice.command — we don't.
      listening_.store(false);
      set_state(SatState::Idle);
      ESP_LOGI(kTag, "transcript: \"%s\"", text.c_str());
      play_done_tone();
      if (transcript_cb_) transcript_cb_(transcript_ctx_, text.c_str());
    }
    return true;
  }

  // detection / everything else: ignored (forward-compatible — unknown
  // events are not errors).
  return true;
}

void WyomingSatellite::trigger_ptt() {
  // Called from any task (e.g. the LVGL UI). The satellite task picks this
  // up between recvs and starts the stream, so socket writes stay on one
  // task. Cheap and lock-free.
  if (!client_connected_.load()) return;
  ptt_pending_.store(true);
}

void WyomingSatellite::mic_task(void* arg) {
  static_cast<WyomingSatellite*>(arg)->run_mic();
  vTaskDelete(nullptr);
}

void WyomingSatellite::run_mic() {
  // Stream mic audio as audio-start / audio-chunk* / audio-stop while
  // listening_. The orchestrator endpoints the utterance and replies with a
  // transcript, which clears listening_. A safety cap bounds a stuck stream.
  const AudioFormat mic_fmt = {audio_->capture_rate(), 2, 1};
  const size_t kChunkFrames = 512;  // 32 ms at 16 kHz
  int16_t* buf = (int16_t*)malloc(kChunkFrames * sizeof(int16_t));
  if (!buf) {
    listening_.store(false);
    mic_task_ = nullptr;
    return;
  }

  audio_->start_capture();
  {
    std::vector<uint8_t> start;
    build_audio_start(start, mic_fmt);
    send_all(start);
  }

  // The orchestrator's endpointer ends the utterance on ~800 ms of silence
  // (its maxUtteranceMs is 10 s). This cap is only a backstop for a stuck
  // stream, so keep it just above the orchestrator's own max.
  const TickType_t t0 = xTaskGetTickCount();
  const TickType_t kMaxTicks = pdMS_TO_TICKS(12000);
  while (listening_.load() && running_.load() && client_connected_.load()) {
    if (xTaskGetTickCount() - t0 > kMaxTicks) {
      ESP_LOGW(kTag, "mic stream hit safety cap — stopping");
      break;
    }
    size_t frames = audio_->record_pcm(buf, kChunkFrames);
    if (frames == 0) continue;
    std::vector<uint8_t> chunk;
    build_audio_chunk(chunk, mic_fmt, buf, frames);
    if (!send_all(chunk)) break;
  }

  {
    std::vector<uint8_t> stop;
    build_audio_stop(stop);
    send_all(stop);
  }
  audio_->stop_capture();
  free(buf);
  listening_.store(false);
  mic_task_ = nullptr;
}

void WyomingSatellite::play_done_tone() {
  // Short confirmation blip so the user knows the utterance was captured.
  const uint32_t rate = 16000;
  const size_t n = rate / 10;  // 100 ms
  int16_t* pcm = (int16_t*)malloc(n * sizeof(int16_t));
  if (!pcm) return;
  for (size_t i = 0; i < n; ++i) {
    float env = 1.0f;
    size_t fade = n / 10;
    if (i < fade) env = (float)i / fade;
    else if (i >= n - fade) env = (float)(n - i) / fade;
    pcm[i] = (int16_t)(0.4f * 32767.0f * env *
                       sinf(2.0f * 3.14159265f * 880.0f * i / rate));
  }
  audio_->play_pcm(pcm, n);
  free(pcm);
}

bool WyomingSatellite::send_all(const std::vector<uint8_t>& bytes) {
  // Serialise writes: the mic task, the recv loop (pong / played / info) and
  // PTT all send on one socket.
  if (send_mutex_) xSemaphoreTake(send_mutex_, portMAX_DELAY);
  bool ok = true;
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
      ok = false;
      break;
    }
    off += (size_t)sent;
  }
  if (send_mutex_) xSemaphoreGive(send_mutex_);
  return ok;
}

}  // namespace sensesp_wyoming
