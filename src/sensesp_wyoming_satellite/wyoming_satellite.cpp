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
  wake_done_ = xSemaphoreCreateBinary();
  probe_mutex_ = xSemaphoreCreateMutex();
}

WyomingSatellite::~WyomingSatellite() {
  stop();  // joins the server (and transitively the mic) task before freeing
  if (send_mutex_) vSemaphoreDelete(send_mutex_);
  if (mic_done_) vSemaphoreDelete(mic_done_);
  if (server_done_) vSemaphoreDelete(server_done_);
  if (wake_done_) vSemaphoreDelete(wake_done_);
  if (probe_mutex_) vSemaphoreDelete(probe_mutex_);
  if (probe_buf_) free(probe_buf_);
}

void WyomingSatellite::start() {
  if (running_.exchange(true)) return;
  xTaskCreate(&WyomingSatellite::server_task, "wyoming_sat", 6144, this, 3,
              &server_task_);
  ESP_LOGI(kTag, "Wyoming satellite starting on port %u", config_.port);

  if (config_.on_device_wake) {
    // On-device wake: esp-sr AFE + WakeNet reads the mic locally. On a
    // detection it runs the same pipeline PTT does. Build + configure a local
    // first; only publish to wake_engine_ once it's live, so /hello never sees
    // a half-constructed engine. (new can yield nullptr with -fno-exceptions.)
    WakeEngine* e = new WakeEngine(audio_);
    if (!e) {
      ESP_LOGW(kTag, "on-device wake alloc failed — tap-to-talk only");
    } else {
      e->set_muted_fn([this] { return mic_muted(); });
      e->set_on_detect([this] { start_wake_pipeline(); });
      e->set_input_gain(config_.wake_input_gain);
      e->set_threshold(config_.wake_threshold);
      if (e->start()) {
        wake_engine_.store(e);
      } else {
        ESP_LOGW(kTag, "on-device wake unavailable — tap-to-talk only");
        delete e;
      }
    }
  } else if (!config_.wake_host.empty()) {
    xTaskCreate(&WyomingSatellite::wake_task, "wyoming_wake", 4608, this, 3,
                &wake_task_);
    ESP_LOGI(kTag, "Network wake: streaming to %s:%u", config_.wake_host.c_str(),
             config_.wake_port);
  }
}

void WyomingSatellite::stop() {
  if (!running_.exchange(false)) return;
  // Nudge a blocked accept()/recv() to notice running_==false: closing the
  // client socket unblocks recv; the accept loop wakes on its select tick.
  if (client_sock_ >= 0) {
    shutdown(client_sock_, SHUT_RDWR);
  }
  // On-device wake engine: DETACH the pointer before stopping/deleting so a
  // /hello diagnostics read in flight sees null rather than a freed engine.
  if (WakeEngine* e = wake_engine_.exchange(nullptr)) {
    e->stop();  // joins the feed/fetch tasks + releases the mic
    delete e;
  }
  // The network wake task blocks in connect()/recv() with short timeouts, so
  // it wakes on its next tick and exits on running_==false. Join it first.
  if (wake_task_) {
    if (xSemaphoreTake(wake_done_, pdMS_TO_TICKS(8000)) != pdTRUE) {
      ESP_LOGE(kTag, "wake task did not exit in time — leaking task handle");
    }
    wake_task_ = nullptr;
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

  if (t == "detection") {
    // From the wake service (wake task context, called from within
    // wake_session()'s decoder.feed while the wake loop still owns the ADC).
    // Only flag it here — wake_session() closes its capture and THEN runs the
    // pipeline, so run_mic() is the sole ADC reader (single mic consumer).
    std::string name;
    parse_detection(ev, &name);
    ESP_LOGI(kTag, "wake detection: \"%s\"", name.c_str());
    wake_detections_.fetch_add(1);
    wake_detected_.store(true);
    return true;
  }

  // not-detected / everything else: ignored (forward-compatible — unknown
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
    // Muting must stop the mic NOW — abort before the next record_pcm()/send,
    // and skip the release tail, so no samples reach the orchestrator after
    // the user hits mute (set_ptt_held(false) alone would still stream the
    // 1.2 s tail). The audio-stop below closes the pipeline cleanly.
    if (mic_muted()) break;
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
    if (frames == 0) {
      // Failing read: back off instead of spinning. Unlike the wake loop this
      // can't wedge forever (the safety cap above ends the stream), but a bare
      // continue still busy-loops a whole utterance's worth of CPU against a
      // dead handle. No reopen attempt here: the capture is refcounted and the
      // wake engine may hold a reference too, so a close/reopen from inside
      // this stream could disturb the other consumer. Ending the utterance and
      // letting the caller re-open is the safer recovery.
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }
    // Boost the quiet panel mic so the orchestrator's energy-gate endpointer
    // registers speech (RMS floor ~700) and ends the utterance ~1 s after you
    // stop talking, instead of running to the safety cap. Saturating clamp so
    // loud peaks don't wrap. gain <= 1 leaves the samples untouched.
    if (config_.mic_stream_gain > 1) {
      for (size_t i = 0; i < frames; i++) {
        int32_t v = (int32_t)buf[i] * config_.mic_stream_gain;
        if (v > 32767) v = 32767;
        else if (v < -32768) v = -32768;
        buf[i] = (int16_t)v;
      }
    }
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

// ---- Wake mode ------------------------------------------------------------

void WyomingSatellite::wake_task(void* arg) {
  auto* self = static_cast<WyomingSatellite*>(arg);
  self->run_wake();
  xSemaphoreGive(self->wake_done_);
  vTaskDelete(nullptr);
}

void WyomingSatellite::run_wake() {
  // Reconnect loop to the wake service. Each successful connection runs a
  // capture-and-detect session; on any drop we back off and retry until
  // stop() clears running_.
  TickType_t backoff = pdMS_TO_TICKS(1000);
  const TickType_t kMaxBackoff = pdMS_TO_TICKS(10000);

  while (running_.load()) {
    int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock < 0) {
      vTaskDelay(backoff);
      continue;
    }
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(config_.wake_port);
    if (inet_pton(AF_INET, config_.wake_host.c_str(), &addr.sin_addr) != 1) {
      // Only a dotted-quad is accepted here; the caller resolves a hostname
      // to an IP before constructing the satellite.
      ESP_LOGE(kTag, "wake host '%s' is not an IPv4 address — wake disabled",
               config_.wake_host.c_str());
      close(sock);
      return;
    }
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
      close(sock);
      if (running_.load()) vTaskDelay(backoff);
      backoff = backoff * 2 > kMaxBackoff ? kMaxBackoff : backoff * 2;
      continue;
    }
    ESP_LOGI(kTag, "wake service connected %s:%u", config_.wake_host.c_str(),
             config_.wake_port);
    backoff = pdMS_TO_TICKS(1000);  // reset after a good connection
    wake_connected_.store(true);
    wake_session(sock);
    wake_connected_.store(false);
    close(sock);
    ESP_LOGI(kTag, "wake service disconnected");
    if (running_.load()) vTaskDelay(pdMS_TO_TICKS(500));
  }
}

bool WyomingSatellite::wake_session(int sock) {
  // Short recv timeout so a `detection` is noticed promptly and running_ /
  // mute changes are polled between mic reads.
  struct timeval rcv_tv = {.tv_sec = 0, .tv_usec = 50000};  // 50 ms
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &rcv_tv, sizeof(rcv_tv));
  struct timeval snd_tv = {.tv_sec = 2, .tv_usec = 0};
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &snd_tv, sizeof(snd_tv));

  // Arm the service for our configured wake words (empty = any), then stream
  // the mic. detect + a leading audio-start open the stream.
  auto sock_send = [&](const std::vector<uint8_t>& b) -> bool {
    size_t off = 0;
    while (off < b.size()) {
      int s = send(sock, b.data() + off, b.size() - off, MSG_NOSIGNAL);
      if (s <= 0) {
        if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          vTaskDelay(pdMS_TO_TICKS(5));
          continue;
        }
        return false;
      }
      off += (size_t)s;
    }
    return true;
  };

  {
    std::vector<uint8_t> det;
    build_detect(det, config_.wake_words);
    if (!sock_send(det)) return false;
  }

  const AudioFormat mic_fmt = {audio_->capture_rate(), 2, 1};
  const size_t kChunkFrames = 512;  // 32 ms at 16 kHz
  int16_t* buf = (int16_t*)malloc(kChunkFrames * sizeof(int16_t));
  if (!buf) return false;

  bool capturing = false;   // ADC open + audio-start sent
  bool muted_last = false;  // last mute state, to open/close on transitions
  // Consecutive failed (zero-frame) mic reads. Drives the stall recovery
  // below: reopen the capture after a short run, drop the session if even that
  // doesn't restore it. At ~20 ms per failed read these are ~0.4 s and ~2 s —
  // long enough not to fire on an incidental hiccup (a single reopen during a
  // pipeline hand-off), short enough that a wedged stream self-heals quickly
  // instead of staying dead until something else reconnects it.
  int zero_reads = 0;
  const int kZeroReadsBeforeReopen = 20;
  const int kZeroReadsBeforeReconnect = 100;
  wake_detected_.store(false);
  EventDecoder decoder;
  uint8_t recv_buf[512];

  auto open_capture = [&]() -> bool {
    if (capturing) return true;
    audio_->start_capture();
    std::vector<uint8_t> start;
    build_audio_start(start, mic_fmt);
    if (!sock_send(start)) return false;
    capturing = true;
    wake_capturing_.store(true);
    return true;
  };
  auto close_capture = [&]() {
    if (!capturing) return;
    std::vector<uint8_t> stop;
    build_audio_stop(stop);
    sock_send(stop);
    audio_->stop_capture();
    capturing = false;
    wake_capturing_.store(false);
  };

  bool ok = true;
  while (running_.load()) {
    // Drain any inbound events first (detection / not-detected).
    int n = recv(sock, recv_buf, sizeof(recv_buf), 0);
    if (n > 0) {
      if (!decoder.feed(recv_buf, (size_t)n, &WyomingSatellite::on_event_tramp,
                        this)) {
        ok = false;
        break;
      }
    } else if (n == 0) {
      ok = false;
      break;  // service closed
    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
      ok = false;
      break;  // real error
    }

    // A wake word fired: hand the mic to the orchestrator pipeline. Close our
    // capture FIRST (release the ADC) so run_mic() is the sole reader, play
    // the awake cue, then run the pipeline. This blocks the wake loop for the
    // whole utterance — exactly right, the mic has one owner at a time.
    if (wake_detected_.exchange(false)) {
      close_capture();
      if (config_.awake_cue) play_done_tone();
      run_detection_pipeline();
      continue;
    }

    // A PTT press can also start a pipeline (the tap-to-talk widget). Same
    // rule: release the ADC and wait it out.
    if (pipeline_active_.load() || listening_.load()) {
      close_capture();
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }

    // Privacy gate: when muted, don't stream to the wake service at all.
    const bool muted = mic_muted();
    if (muted != muted_last) {
      if (muted) close_capture();
      muted_last = muted;
    }
    if (muted) {
      vTaskDelay(pdMS_TO_TICKS(50));
      continue;
    }

    if (!open_capture()) {
      ok = false;
      break;
    }
    size_t frames = audio_->record_pcm(buf, kChunkFrames);
    if (frames == 0) {
      // A zero read means the codec errored (or the handle was closed under
      // us). Two reasons this must not be a bare `continue`:
      //
      // 1. No delay — a persistently failing read spins this task flat out.
      // 2. No recovery — the driver's handle can be left open-but-dead (its
      //    capturing_ stays true, so start_capture() is a refcount no-op and
      //    nothing ever reopens it). Then EVERY later read fails too and the
      //    wake stream is wedged until the session reconnects. Observed live:
      //    chunks frozen at a fixed count for minutes with capturing=true and
      //    connected=true, while the mic hardware was perfectly healthy.
      //
      // So: back off briefly, and after a short run of consecutive failures
      // force a capture close+reopen to rebuild the handle. If reopening
      // doesn't help either, drop the session — the reconnect path is the
      // last resort that is known to recover it.
      if (++zero_reads == kZeroReadsBeforeReopen) {
        ESP_LOGW(kTag, "mic reads failing — reopening capture");
        close_capture();
        vTaskDelay(pdMS_TO_TICKS(50));
        if (!open_capture()) {
          ok = false;
          break;
        }
      } else if (zero_reads >= kZeroReadsBeforeReconnect) {
        ESP_LOGE(kTag, "mic still dead after reopen — dropping wake session");
        ok = false;
        break;
      }
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }
    zero_reads = 0;  // a good read clears the failure run
    // Wake-only digital boost: the analog PGA alone leaves the MEMS mics too
    // quiet for openWakeWord's model. Scale here (with saturation) so the
    // stream, the peak gauge and the probe tee all reflect what the detector
    // receives. The push-to-talk path is untouched (it reads record_pcm
    // directly and whisper already copes).
    if (config_.wake_gain > 1.0f) {
      for (size_t i = 0; i < frames; ++i) {
        int32_t v = (int32_t)(buf[i] * config_.wake_gain);
        if (v > 32767) v = 32767;
        else if (v < -32768) v = -32768;
        buf[i] = (int16_t)v;
      }
    }
    // Track peak amplitude so /hello can tell "mic silent" from "mic fine but
    // model not matching".
    uint16_t peak = wake_peak_.load();
    for (size_t i = 0; i < frames; ++i) {
      int v = buf[i] < 0 ? -buf[i] : buf[i];
      if (v > peak) peak = (uint16_t)v;
    }
    wake_peak_.store(peak);
    // Tee into the probe ring so /mic_probe can dump exactly this audio. We
    // only reach here when unmuted (the wake session doesn't stream muted mic
    // audio). Re-enabling the snapshot is done INSIDE the lock, and only after
    // discarding any pre-mute samples that wake_pcm_clear() may have failed to
    // zero — so a snapshot never sees pre-mute audio, and a delayed in-flight
    // chunk can't flip the kill switch back on before the ring is clean.
    if (probe_mutex_ &&
        xSemaphoreTake(probe_mutex_, 0) == pdTRUE) {
      if (!probe_buf_) {
        probe_buf_ = (int16_t*)malloc(kProbeSamples * sizeof(int16_t));
      }
      if (probe_buf_) {
        if (probe_disabled_.load()) {
          // First post-clear write while still disabled: drop whatever the
          // clear couldn't reach, then re-enable — all under the lock, so a
          // concurrent snapshot sees either "disabled" or a freshly-reset ring.
          probe_head_ = 0;
          probe_filled_ = 0;
          probe_disabled_.store(false);
        }
        for (size_t i = 0; i < frames; ++i) {
          probe_buf_[probe_head_] = buf[i];
          probe_head_ = (probe_head_ + 1) % kProbeSamples;
          if (probe_filled_ < kProbeSamples) probe_filled_++;
        }
      }
      xSemaphoreGive(probe_mutex_);
    }
    std::vector<uint8_t> chunk;
    build_audio_chunk(chunk, mic_fmt, buf, frames);
    if (!sock_send(chunk)) {
      ok = false;
      break;
    }
    wake_chunks_.fetch_add(1);
  }

  close_capture();
  free(buf);
  return ok;
}

void WyomingSatellite::start_wake_pipeline() {
  // Called from the WakeEngine fetch task on a detection. Pause the engine so
  // it releases the mic (single consumer), play the awake cue, run the same
  // pipeline PTT does, then resume the engine to listen for the next word.
  if (!client_connected_.load() || !armed_) {
    ESP_LOGW(kTag, "wake ignored (no orchestrator / not armed)");
    return;
  }
  if (mic_muted()) return;
  // Runs on the engine's own fetch task, so the engine outlives this call;
  // snapshot the atomic once for the type.
  WakeEngine* e = wake_engine_.load();
  if (e) e->pause();  // frees the mic for run_mic()
  if (config_.awake_cue) play_done_tone();
  run_detection_pipeline();
  if (e) e->resume();
}

void WyomingSatellite::run_detection_pipeline() {
  // A wake word fired: run the same pipeline PTT does. The socket task starts
  // run_mic() when it sees ptt_held_ && !listening_; we set that, then wait
  // for the stream to begin and end so the wake loop stays off the mic for
  // the whole pipeline (single consumer). No release tail is needed — the
  // orchestrator's silence gate ends the utterance and clears listening_.
  if (!client_connected_.load() || !armed_) {
    ESP_LOGW(kTag, "detection ignored (no orchestrator / not armed)");
    return;
  }
  if (mic_muted()) return;  // privacy gate
  pipeline_active_.store(true);
  set_ptt_held(true);

  // Wait for run_mic() to actually take over the mic (listening_ true), then
  // for it to finish. Bounded so a stream that never starts can't wedge us.
  const TickType_t kStartTimeout = pdMS_TO_TICKS(1500);
  const TickType_t kMaxPipeline = pdMS_TO_TICKS(25000);
  TickType_t t0 = xTaskGetTickCount();
  while (running_.load() && !listening_.load() &&
         xTaskGetTickCount() - t0 < kStartTimeout) {
    vTaskDelay(pdMS_TO_TICKS(20));
  }
  t0 = xTaskGetTickCount();
  while (running_.load() && listening_.load() &&
         xTaskGetTickCount() - t0 < kMaxPipeline) {
    vTaskDelay(pdMS_TO_TICKS(50));
  }
  set_ptt_held(false);
  pipeline_active_.store(false);
}

size_t WyomingSatellite::wake_pcm_snapshot(int16_t* out, size_t max_samples) {
  if (!out || max_samples == 0) return 0;
  // Fast lock-free reject: wake_pcm_clear() sets the flag first, so a muted mic
  // is refused immediately even if the ring couldn't be zeroed.
  if (probe_disabled_.load()) return 0;
  // On-device wake fills its own probe ring (the network ring below is only
  // fed by wake_session, which isn't running on the on-device path).
  if (config_.on_device_wake) {
    WakeEngine* e = wake_engine_.load();
    return e ? e->pcm_snapshot(out, max_samples) : 0;
  }
  if (!probe_mutex_) return 0;
  size_t n = 0;
  if (xSemaphoreTake(probe_mutex_, pdMS_TO_TICKS(200)) != pdTRUE) return 0;
  // Re-check UNDER the lock: the writer flips the flag off + resets the ring
  // atomically w.r.t. this mutex, so this guarantees we never read pre-mute
  // samples racing a just-cleared/just-re-enabled ring.
  if (!probe_disabled_.load() && probe_buf_ && probe_filled_ > 0) {
    n = probe_filled_ < max_samples ? probe_filled_ : max_samples;
    // Oldest sample first. head points past the newest; the ring holds
    // probe_filled_ samples ending at head-1.
    size_t start = (probe_head_ + kProbeSamples - probe_filled_) % kProbeSamples;
    // We want the newest n samples: shift start forward if truncating.
    if (probe_filled_ > n) {
      start = (probe_head_ + kProbeSamples - n) % kProbeSamples;
    }
    for (size_t i = 0; i < n; ++i) {
      out[i] = probe_buf_[(start + i) % kProbeSamples];
    }
  }
  xSemaphoreGive(probe_mutex_);
  return n;
}

void WyomingSatellite::wake_pcm_clear() {
  // Drop any retained mic PCM (privacy: called when the mic is muted so a
  // later /mic_probe can't surface audio captured before the mute).
  //
  // On-device: the WakeEngine owns its own probe ring — clear it. Its feed
  // loop won't refill while muted (it checks the mic-mute gate) and resumes
  // filling on unmute, so no separate disable latch is needed there.
  if (config_.on_device_wake) {
    WakeEngine* e = wake_engine_.load();
    if (e) e->clear_probe();
    return;
  }
  // Network: set the disable flag FIRST and unconditionally — it's lock-free,
  // so the privacy guarantee holds even if we can't grab probe_mutex_;
  // snapshot() returns nothing while it's set. Zeroing the ring under the lock
  // is best-effort belt-and-braces; the flag is the guarantee.
  probe_disabled_.store(true);
  if (!probe_mutex_) return;
  if (xSemaphoreTake(probe_mutex_, pdMS_TO_TICKS(200)) != pdTRUE) return;
  probe_head_ = 0;
  probe_filled_ = 0;
  xSemaphoreGive(probe_mutex_);
}

bool WyomingSatellite::probe_mic_levels(
    sensesp_cockpit_display::AudioDriver::MicLevels& out) {
  if (!audio_) return false;
  // The probe builds a SECOND codec device and opens it on the SHARED I2S RX,
  // reconfiguring and then closing that RX. It must therefore be the SOLE mic
  // reader while it runs: record_pcm() holds no lock across its blocking read
  // (it can't — that would deadlock teardown), so a concurrent probe pulls the
  // RX out from under it and leaves the capture handle open-but-dead. The
  // driver's capturing_ stays true, so start_capture() is a refcount no-op and
  // nothing reopens it — every later read fails and the stream never recovers.
  //
  // BOTH wake back-ends hold the mic continuously, so both must be released:
  //
  // - On-device: pause the engine's feed (it re-opens the ADC on resume).
  // - Network: the wake loop owns the capture in wake_session(). It already
  //   yields the mic for a pipeline, so reuse that gate — pipeline_active_
  //   makes the loop close_capture() and idle. Previously only the on-device
  //   branch was released, so on a network-wake panel the probe collided with
  //   a live capture and wedged the wake stream (chunks frozen at a fixed
  //   count with capturing=true, until the session reconnected).
  WakeEngine* e = config_.on_device_wake ? wake_engine_.load() : nullptr;
  if (e) e->pause();
  const bool net_wake = !config_.on_device_wake;
  if (net_wake) {
    pipeline_active_.store(true);
    // Wait for the loop to observe the gate and release the ADC. It polls at
    // 20 ms; bound the wait so a stopped/disconnected wake loop (which never
    // clears wake_capturing_) can't block the probe indefinitely.
    const TickType_t kYieldTimeout = pdMS_TO_TICKS(300);
    TickType_t t0 = xTaskGetTickCount();
    while (wake_capturing_.load() &&
           xTaskGetTickCount() - t0 < kYieldTimeout) {
      vTaskDelay(pdMS_TO_TICKS(10));
    }
  }
  bool ok = audio_->probe_mic_channels(out);
  if (e) e->resume();
  if (net_wake) pipeline_active_.store(false);
  return ok;
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
