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
// INPUT (the boat listens, push-to-talk): trigger_ptt() sends `run-pipeline`
// and streams the mic as `audio-start` / `audio-chunk` / `audio-stop`; the
// orchestrator endpoints the utterance, runs ASR, and returns a `transcript`,
// which stops our streaming and plays a done sound. The orchestrator publishes
// the text to SignalK's voice.command — the panel itself is a dumb mic pump.
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

  // Push-to-talk, LEVEL-triggered (press-and-hold): set held=true on the mic
  // button press, held=false on release. Safe to call from any task (e.g. the
  // LVGL UI). The satellite streams the mic while held is true and the
  // orchestrator has armed us (run-satellite); on release it sends audio-stop
  // and the orchestrator transcribes what it captured — no wait for silence
  // detection, and no edge/ordering race between press and release.
  void set_ptt_held(bool held);

  // Momentary trigger (tap-to-talk fallback): behaves like a press+auto-hold
  // that the orchestrator's silence endpointer ends. Kept for callers that
  // don't do press/release. Equivalent to set_ptt_held(true) with no release.
  void trigger_ptt() { set_ptt_held(true); }

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
  SemaphoreHandle_t server_done_ = nullptr;  // given when serve() exits
  std::atomic<bool> running_{false};
  std::atomic<bool> client_connected_{false};
  std::atomic<SatState> state_{SatState::Disconnected};

  // Per-connection state (only one client at a time).
  int client_sock_ = -1;
  SemaphoreHandle_t send_mutex_ = nullptr;  // serialises socket writes
  bool armed_ = false;      // orchestrator sent run-satellite (mic allowed)
  bool streaming_ = false;  // playback: between audio-start and audio-stop
  AudioFormat play_fmt_;

  // Voice-in (push-to-talk). Level-triggered: ptt_held_ reflects the button
  // being physically held; the socket task starts a stream when held && !
  // listening_, and the mic task streams while held_ stays true.
  std::atomic<bool> mic_running_{false};  // mic task alive (set by the task)
  SemaphoreHandle_t mic_done_ = nullptr;  // given when run_mic() exits
  std::atomic<bool> listening_{false};    // a mic stream is currently active
  std::atomic<bool> ptt_held_{false};     // button held (UI sets true/false)

  TranscriptFn transcript_cb_ = nullptr;
  void* transcript_ctx_ = nullptr;
};

}  // namespace sensesp_wyoming
