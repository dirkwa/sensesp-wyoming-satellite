#pragma once

// WyomingSatellite — a Wyoming-protocol voice satellite for an ESP32 board.
//
// It is a TCP SERVER on :10700. The orchestrator (signalk-wyoming, or Home
// Assistant) is the CLIENT and dials out to us, exactly as it does to a
// wyoming-satellite. On connect it sends `describe`; we answer `info`, then
// it sends `run-satellite` (active) or `pause-satellite` (output-only). It
// keeps the connection alive with `ping` (we `pong`), and plays TTS by
// framing `audio-start` / `audio-chunk` / `audio-stop`; we play it through
// the board AudioDriver and reply `played`.
//
// Phase 1 scope: OUTPUT ONLY (the boat speaks). Mic capture / voice-in is a
// later phase; this class already advertises a mic_format in `info` so the
// wire contract doesn't change when capture lands.
//
// Single-client by design (a satellite mic is an open channel — one owner
// at a time, the security model wyoming relies on). A second connection is
// closed immediately.

#include <atomic>

#include "freertos/FreeRTOS.h"
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

class WyomingSatellite {
 public:
  WyomingSatellite(sensesp_cockpit_display::AudioDriver* audio,
                   const WyomingSatelliteConfig& config = {});
  ~WyomingSatellite();

  void start();
  void stop();

  // For a /hello-style status line.
  bool running() const { return running_.load(); }
  bool client_connected() const { return client_connected_.load(); }

 private:
  static void server_task(void* arg);
  void serve();                 // accept loop
  void handle_client(int sock); // one connection's lifetime

  // Framing callback trampoline (EventDecoder is C-style function ptr).
  static bool on_event_tramp(void* ctx, const DecodedEvent& ev);
  bool on_event(const DecodedEvent& ev);

  bool send_all(const std::vector<uint8_t>& bytes);

  sensesp_cockpit_display::AudioDriver* audio_;
  WyomingSatelliteConfig config_;

  TaskHandle_t server_task_ = nullptr;
  std::atomic<bool> running_{false};
  std::atomic<bool> client_connected_{false};

  // Per-connection state (only one client at a time, so plain members).
  int client_sock_ = -1;
  bool streaming_ = false;   // between audio-start and audio-stop
  AudioFormat play_fmt_;
};

}  // namespace sensesp_wyoming
