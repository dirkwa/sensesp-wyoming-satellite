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
  mic_done_ = xSemaphoreCreateBinary();
  server_done_ = xSemaphoreCreateBinary();
}

WyomingSatellite::~WyomingSatellite() {
  stop();  // joins the server (and transitively the mic) task before freeing
  if (send_mutex_) vSemaphoreDelete(send_mutex_);
  if (mic_done_) vSemaphoreDelete(mic_done_);
  if (server_done_) vSemaphoreDelete(server_done_);
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
    // Real join: serve() gives server_done_ just before the task self-deletes
    // (having already joined the mic task). Wait for it so the caller /
    // destructor never frees a semaphore a running task still touches. The
    // task exits on its next select/recv tick (≤1s accept, ≤500ms recv) plus
    // mic-task drain, so a few seconds is ample.
    if (xSemaphoreTake(server_done_, pdMS_TO_TICKS(8000)) != pdTRUE) {
      ESP_LOGE(kTag, "server task did not exit in time — leaking task handle");
    }
    server_task_ = nullptr;
  }
}

void WyomingSatellite::server_task(void* arg) {
  auto* self = static_cast<WyomingSatellite*>(arg);
  self->serve();
  // Signal stop() that the task (and any mic task it joined) is fully done
  // before self-deleting, so teardown can safely free shared primitives.
  xSemaphoreGive(self->server_done_);
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
  ptt_held_.store(false);
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
    // Push-to-talk is level-triggered: start a stream when the button is held
    // and we aren't already streaming. Starting here on the satellite task
    // keeps all socket writes serialised. A brief hold (released before this
    // tick) simply never satisfies the condition — no spurious stream.
    if (ptt_held_.load() && !listening_.load()) {
      if (armed_ && !streaming_) {
        std::vector<uint8_t> rp;
        build_run_pipeline(rp, config_.name);
        if (send_all(rp)) {
          listening_.store(true);
          set_state(SatState::Listening);
          TaskHandle_t t = nullptr;
          if (xTaskCreate(&WyomingSatellite::mic_task, "wyoming_mic", 4096,
                          this, 4, &t) != pdPASS) {
            // The orchestrator was told a pipeline is running; close it out
            // with audio-stop so it isn't left waiting on its own timeout.
            ESP_LOGW(kTag, "mic task create failed — sending audio-stop");
            listening_.store(false);
            set_state(SatState::Idle);
            std::vector<uint8_t> stop;
            build_audio_stop(stop);
            send_all(stop);
          } else {
            ESP_LOGI(kTag, "PTT: listening");
          }
        } else {
          ESP_LOGW(kTag, "PTT: run-pipeline send failed");
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

  // Stop the mic task and JOIN it before closing the socket. Signalling
  // listening_=false makes run_mic() exit its loop; mic_done_ is given when it
  // has fully returned (past its final send_all + stop_capture). Closing the
  // socket while the mic task could still be mid-send would let its
  // audio-chunk writes race the close — or, worse, land on the next client's
  // socket. Waiting here (same task that would accept the next client)
  // guarantees the mic task is done first.
  listening_.store(false);
  if (mic_running_.load()) {
    if (xSemaphoreTake(mic_done_, pdMS_TO_TICKS(3000)) != pdTRUE) {
      ESP_LOGE(kTag, "mic task did not finish — closing socket anyway");
    }
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
    ptt_held_.store(false);  // ignore a held button while paused
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
      // Reflect TTS playback in the UI-facing state (unless a voice-in
      // pipeline is mid-flight — don't stomp Listening).
      if (state() != SatState::Listening) set_state(SatState::Speaking);
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
      // Leave Listening alone (a pipeline may be mid-flight); otherwise
      // playback is done, so go back to Idle.
      if (state() == SatState::Speaking) set_state(SatState::Idle);
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
      // SignalK's voice.command — we don't. Clear ptt_held_ too so a still-
      // held button doesn't immediately start a second utterance.
      ptt_held_.store(false);
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

void WyomingSatellite::set_ptt_held(bool held) {
  // Level-triggered from any task (e.g. the LVGL UI): just records whether the
  // button is held. The socket task starts a stream when it sees held &&
  // !listening_, and run_mic() stops streaming when held goes false. No edge
  // race between press and release — the held flag is the single source of
  // truth. Lock-free.
  if (held && !client_connected_.load()) return;
  ptt_held_.store(held);
}

void WyomingSatellite::mic_task(void* arg) {
  static_cast<WyomingSatellite*>(arg)->run_mic();
  vTaskDelete(nullptr);
}

void WyomingSatellite::run_mic() {
  // Stream mic audio as audio-start / audio-chunk* / audio-stop while
  // listening_. The orchestrator endpoints the utterance and replies with a
  // transcript, which clears listening_. A safety cap bounds a stuck stream.
  //
  // EVERY exit path must: clear listening_, return the state to Idle (only the
  // transcript handler otherwise resets it, so a cap/send-fail/disconnect exit
  // would leave the UI stuck on Listening), and give mic_done_ (the join that
  // handle_client() waits on before closing the socket). mic_running_ gates
  // that wait so a never-started task doesn't hang it.
  mic_running_.store(true);
  const AudioFormat mic_fmt = {audio_->capture_rate(), 2, 1};
  const size_t kChunkFrames = 512;  // 32 ms at 16 kHz
  int16_t* buf = (int16_t*)malloc(kChunkFrames * sizeof(int16_t));
  if (!buf) {
    // No audio-start was sent, so send a bare audio-stop to close the pipeline
    // the run-pipeline opened.
    std::vector<uint8_t> stop;
    build_audio_stop(stop);
    send_all(stop);
    listening_.store(false);
    set_state(SatState::Idle);
    mic_running_.store(false);
    xSemaphoreGive(mic_done_);
    return;
  }

  audio_->start_capture();
  {
    std::vector<uint8_t> start;
    build_audio_start(start, mic_fmt);
    send_all(start);
  }

  // Stream while the button is held, then for a short RELEASE TAIL after it
  // is let go. The orchestrator ends an utterance only when its own energy
  // gate sees ~800 ms of silence — it ignores a satellite audio-stop — so on
  // release we keep streaming the (now-quiet) mic for kReleaseTailMs. That
  // near-silent tail trips the gate and the transcript comes back in ~1 s,
  // instead of the pipeline timing out 30 s later. A safety cap bounds a
  // stuck-held button; a received transcript clears listening_ early.
  const TickType_t t0 = xTaskGetTickCount();
  const TickType_t kMaxTicks = pdMS_TO_TICKS(20000);
  const TickType_t kReleaseTailTicks = pdMS_TO_TICKS(1200);
  TickType_t release_at = 0;  // 0 = still held
  while (listening_.load() && running_.load() && client_connected_.load()) {
    if (!ptt_held_.load() && release_at == 0) {
      release_at = xTaskGetTickCount();  // start the tail on release
    }
    if (release_at != 0 &&
        xTaskGetTickCount() - release_at > kReleaseTailTicks) {
      break;  // tail done — let the orchestrator's gate have ended it
    }
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
  set_state(SatState::Idle);
  mic_running_.store(false);
  xSemaphoreGive(mic_done_);
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
