#pragma once

// WyomingSatellite — a Wyoming-protocol voice satellite for an ESP32 board.
//
// It is a TCP SERVER on :10700. The orchestrator (signalk-wyoming, or Home
// Assistant) is the CLIENT and dials out to us, exactly as it does to a
// wyoming-satellite. On connect it sends `describe`; we answer `info`, then
// it sends `run-satellite` (active) or `pause-satellite` (output-only). It
// keeps the connection alive with `ping` (we `pong`).
//
// OUTPUT (the boat speaks): the orchestrator frames TTS as `audio-start` /
// `audio-chunk` / `audio-stop`; we play it through the AudioDriver and reply
// `played`.
//
// INPUT (the boat listens, push-to-talk): trigger_ptt() sends `detection` +
// `run-pipeline` and streams the mic as `audio-chunk`s; the orchestrator
// endpoints the utterance, runs ASR, and returns a `transcript`, which stops
// our streaming and plays a done sound. The orchestrator publishes the text
// to SignalK's voice.command — the panel itself is a dumb mic pump.
//
// Single-client by design (a satellite mic is an open channel — one owner
// at a time, the security model wyoming relies on). A second connection is
// closed immediately.

#include <atomic>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include "sensesp_cockpit_display/hal/audio_driver.h"
#include "sensesp_wyoming_satellite/protocol/events.h"

namespace sensesp_wyoming {

struct WyomingSatelliteConfig {
  uint16_t port = 10700;
  const char* name = "cockpit";
  // Playback format we advertise + expect in audio-start. Piper medium/high
  // voices are 22050 Hz; the driver reopens the codec to match. Set to
  // 16000 if you constrain the orchestrator to 16 kHz voices.
  uint32_t snd_rate = 22050;
};

// Coarse state for a UI indicator.
enum class SatState { Disconnected, Idle, Listening, Speaking };

class WyomingSatellite {
 public:
  WyomingSatellite(sensesp_cockpit_display::AudioDriver* audio,
                   const WyomingSatelliteConfig& config = {});
  ~WyomingSatellite();

  void start();
  void stop();

  // Push-to-talk: begin a voice-command utterance. Safe to call from any
  // task (e.g. the LVGL UI on a mic-button press). No-op if not connected,
  // if the orchestrator hasn't armed us (run-satellite), or if already
  // listening. Streaming stops when the orchestrator returns a transcript
  // (or on a short safety timeout).
  void trigger_ptt();

  // For a /hello-style status line + a UI indicator.
  bool running() const { return running_.load(); }
  bool client_connected() const { return client_connected_.load(); }
  SatState state() const { return state_.load(); }

  // Optional UI hook: called (from the satellite task) with the recognised
  // text when a transcript arrives, so a widget can toast it. The callback
  // must not block. Null by default.
  using TranscriptFn = void (*)(void* ctx, const char* text);
  void set_transcript_cb(TranscriptFn cb, void* ctx) {
    transcript_cb_ = cb;
    transcript_ctx_ = ctx;
  }

 private:
  static void server_task(void* arg);
  void serve();                  // accept loop
  void handle_client(int sock);  // one connection's lifetime

  static void mic_task(void* arg);
  void run_mic();  // streams audio-chunk while listening_

  static bool on_event_tramp(void* ctx, const DecodedEvent& ev);
  bool on_event(const DecodedEvent& ev);

  bool send_all(const std::vector<uint8_t>& bytes);  // thread-safe
  void play_done_tone();
  void set_state(SatState s) { state_.store(s); }

  sensesp_cockpit_display::AudioDriver* audio_;
  WyomingSatelliteConfig config_;

  TaskHandle_t server_task_ = nullptr;
  std::atomic<bool> running_{false};
  std::atomic<bool> client_connected_{false};
  std::atomic<SatState> state_{SatState::Disconnected};

  // Per-connection state (only one client at a time).
  int client_sock_ = -1;
  SemaphoreHandle_t send_mutex_ = nullptr;  // serialises socket writes
  bool armed_ = false;      // orchestrator sent run-satellite (mic allowed)
  bool streaming_ = false;  // playback: between audio-start and audio-stop
  AudioFormat play_fmt_;

  // Voice-in (push-to-talk).
  TaskHandle_t mic_task_ = nullptr;
  std::atomic<bool> listening_{false};  // mic task should stream
  std::atomic<bool> ptt_pending_{false};

  TranscriptFn transcript_cb_ = nullptr;
  void* transcript_ctx_ = nullptr;
};

}  // namespace sensesp_wyoming
