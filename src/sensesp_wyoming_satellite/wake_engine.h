#pragma once

// WakeEngine — ON-DEVICE wake word via esp-sr AFE + WakeNet.
//
// Runs the wake word detector on the panel itself from the raw mic (no
// network audio), which is far more reliable for far-field / small-MEMS-mic
// audio than streaming to a remote openWakeWord (that path scored the panel's
// mic near zero). esp-sr's AFE gives noise suppression + AGC tuned for exactly
// this embedded case.
//
// The model (e.g. wn9_jarvis_tts) is flashed to the "model" partition and
// loaded at runtime via esp_srmodel_init("model"). Which word is compiled in
// is chosen in sdkconfig (CONFIG_SR_WN_WN9_JARVIS_TTS etc.).
//
// SINGLE MIC CONSUMER: the engine owns the mic while listening. Its feed task
// is the only record_pcm() caller. On a detection it fires a callback; the
// owner (WyomingSatellite) PAUSES the engine (pause()) for the pipeline so
// run_mic() becomes the sole reader, then RESUMES it. pause()/resume() stop
// and restart the feed task cleanly — no concurrent capture.

#include <atomic>
#include <functional>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include "sensesp_cockpit_display/hal/audio_driver.h"

namespace sensesp_wyoming {

class WakeEngine {
 public:
  // on_detect is called (from the fetch task) when the wake word fires.
  // muted_fn (optional) returns true to suppress detection (privacy).
  using DetectFn = std::function<void()>;
  using MutedFn = std::function<bool()>;

  explicit WakeEngine(sensesp_cockpit_display::AudioDriver* audio)
      : audio_(audio) {}
  ~WakeEngine();

  void set_on_detect(DetectFn fn) { on_detect_ = std::move(fn); }
  void set_muted_fn(MutedFn fn) { muted_fn_ = std::move(fn); }
  // Pre-AFE software gain on the mic feed (1 = off). The onboard MEMS mic is
  // quiet; a small boost can help, but too much just amplifies the noise
  // floor and hurts detection. Set before start().
  void set_input_gain(int g) { input_gain_ = g; }
  // WakeNet detection threshold (0.4-0.9999; 0 = model default). Lower = more
  // sensitive. Set before start().
  void set_threshold(float t) { threshold_ = t; }

  // Initialise AFE + WakeNet from the "model" partition and start the feed +
  // fetch tasks. Returns false if no model is present or AFE init fails
  // (wake is then simply unavailable; the rest of the satellite is fine).
  bool start();
  void stop();

  // Pause / resume mic consumption. pause() blocks until the feed task has
  // released the mic (so the caller can hand it to run_mic()); resume()
  // restarts capture. Idempotent.
  void pause();
  void resume();

  bool running() const { return running_.load(); }
  bool listening() const { return listening_.load(); }  // capturing right now
  uint32_t detections() const { return detections_.load(); }
  const char* word() const { return word_; }

  // Diagnostic: copy the most recent ~2 s of the EXACT PCM fed to WakeNet
  // (post-gain) into `out` (up to max_samples), newest last. Returns the count
  // copied. Lets /mic_probe dump what the detector actually sees on-device.
  size_t pcm_snapshot(int16_t* out, size_t max_samples);
  // Drop any retained probe PCM (privacy: called when the mic is muted). The
  // feed loop stops filling the ring while muted and clears it on this call.
  void clear_probe();

 private:
  static void feed_task_tramp(void* arg);
  static void fetch_task_tramp(void* arg);
  void feed_loop();
  void fetch_loop();

  sensesp_cockpit_display::AudioDriver* audio_;
  DetectFn on_detect_;
  MutedFn muted_fn_;

  // esp-sr handles (void* to keep esp-sr headers out of this header).
  void* afe_handle_ = nullptr;   // const esp_afe_sr_iface_t*
  void* afe_data_ = nullptr;     // esp_afe_sr_data_t*
  int feed_chunk_ = 0;           // samples per feed (per channel)
  int feed_channels_ = 1;
  int input_gain_ = 1;
  float threshold_ = 0.0f;
  char word_[24] = "";

  TaskHandle_t feed_task_ = nullptr;
  TaskHandle_t fetch_task_ = nullptr;
  // Given once by each loop as it returns, so stop() JOINs the tasks before
  // freeing the AFE / releasing the mic (never vTaskDelete mid-inference).
  SemaphoreHandle_t feed_exited_ = nullptr;
  SemaphoreHandle_t fetch_exited_ = nullptr;
  // pause() ⇄ feed_loop() handshake: the feed loop gives this exactly once
  // when it parks *for a pause* (not for mute), and pause() takes it. A
  // generation counter makes each pause wait for a fresh park rather than a
  // stale token.
  SemaphoreHandle_t feed_paused_ = nullptr;

  std::atomic<bool> running_{false};
  std::atomic<bool> paused_{false};
  std::atomic<bool> listening_{false};
  std::atomic<uint32_t> detections_{0};

  // Diagnostic probe ring: the last ~2 s of post-gain PCM fed to WakeNet.
  static constexpr size_t kProbeSamples = 32000;  // 2 s @ 16 kHz
  int16_t* probe_ = nullptr;
  size_t probe_head_ = 0;
  size_t probe_filled_ = 0;
  SemaphoreHandle_t probe_mutex_ = nullptr;
  // Set while a pcm_snapshot() is copying, so stop() waits it out before
  // freeing the probe. Lets a /mic_probe race shutdown safely.
  std::atomic<bool> snapshot_active_{false};
};

}  // namespace sensesp_wyoming
