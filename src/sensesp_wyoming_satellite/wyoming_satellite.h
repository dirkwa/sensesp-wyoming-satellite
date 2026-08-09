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
//
// WAKE MODE (optional, config.wake_host set): a SECOND, OUTBOUND connection
// to a wake service (openWakeWord :10400). The satellite streams the mic to
// it continuously and, on `detection`, starts the same pipeline PTT does —
// hands-free. The mic has exactly ONE consumer at all times: the wake loop
// captures while idle and PAUSES itself for the duration of a pipeline
// stream (whether triggered by a detection or a PTT press), so run_mic() and
// the wake loop never call record_pcm() concurrently.

#include <atomic>
#include <functional>
#include <string>
#include <vector>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include "sensesp_cockpit_display/hal/audio_driver.h"
#include "sensesp_wyoming_satellite/protocol/events.h"
#include "sensesp_wyoming_satellite/wake_engine.h"

namespace sensesp_wyoming {

struct WyomingSatelliteConfig {
  uint16_t port = 10700;
  const char* name = "cockpit";
  // Playback format we advertise + expect in audio-start. Piper medium/high
  // voices are 22050 Hz; the driver reopens the codec to match. Set to
  // 16000 if you constrain the orchestrator to 16 kHz voices.
  uint32_t snd_rate = 22050;

  // Digital gain applied to the OUTBOUND STT mic stream (run_mic → audio-chunk)
  // only — independent of the wake feed's gain. The panel's MEMS mic reads
  // quiet; the orchestrator's energy-gate endpointer treats a chunk as speech
  // only above an absolute RMS floor (~700), so un-boosted panel audio never
  // trips end-of-utterance and the stream runs to its safety cap (~20 s) every
  // time. Lifting the stream here makes the gate fire ~1 s after you stop
  // talking. 1 = off. Applied with a saturating clamp (no wrap on loud peaks).
  // Keep modest: this also raises what whisper transcribes, so too much just
  // amplifies the noise floor.
  int mic_stream_gain = 3;

  // --- Hands-free wake word ------------------------------------------------
  // Two mutually exclusive back-ends:
  //
  // ON-DEVICE (default, recommended): esp-sr AFE + WakeNet runs the detector
  // on the panel from the raw mic. Its front-end (noise suppression + AGC) is
  // built for far-field embedded mics, unlike a remote openWakeWord which
  // scored this panel's mic near zero. The model (which word) is chosen in
  // sdkconfig + flashed to the "model" partition. No wake_host needed.
  bool on_device_wake = true;
  bool awake_cue = true;              // play a short blip on detection
  // Pre-AFE software gain for the on-device wake feed (1 = off). The panel's
  // live mic (MIC1) reads modest; a small boost lifts speech into WakeNet's
  // preferred range. Keep conservative — too much amplifies noise equally.
  int wake_input_gain = 1;
  // On-device WakeNet detection threshold (0.4-0.9999; 0 = model default).
  // Lower = more sensitive (helps a quiet mic), at the cost of more false
  // accepts.
  float wake_threshold = 0.0f;

  // NETWORK (legacy fallback, on_device_wake=false): open a SECOND, outbound
  // connection to a Wyoming wake service (openWakeWord, default :10400) and
  // stream the mic to it. Kept as a reference / for a future better remote
  // detector, but on-device is the working path on this hardware.
  std::string wake_host;              // e.g. the SK server host; "" = off
  uint16_t wake_port = 10400;         // wyoming-openwakeword default
  std::vector<std::string> wake_words;  // empty = listen for any word
  // Digital gain applied to the NETWORK wake stream only (the on-device AFE
  // has its own AGC and ignores this). 1.0 = off.
  float wake_gain = 6.0f;
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

  // Wake diagnostics for /hello (meaningful when a wake back-end is active).
  // On-device: "connected"/"capturing" reflect the engine; chunks is 0 (no
  // network stream). Network: the legacy counters below.
  // These run on whatever task serves /hello, while stop() may be tearing the
  // engine down — snapshot the atomic pointer once per call so a concurrent
  // delete-then-null can't turn a non-null check into a freed dereference.
  bool wake_enabled() const {
    return config_.on_device_wake ? wake_engine_.load() != nullptr
                                  : !config_.wake_host.empty();
  }
  bool wake_on_device() const { return config_.on_device_wake; }
  bool wake_connected() const {
    if (!config_.on_device_wake) return wake_connected_.load();
    WakeEngine* e = wake_engine_.load();
    return e && e->running();
  }
  bool wake_capturing() const {
    if (!config_.on_device_wake) return wake_capturing_.load();
    WakeEngine* e = wake_engine_.load();
    return e && e->listening();
  }
  // Monotonic count of audio-chunks streamed to the network wake service — a
  // nonzero and growing value proves the mic is feeding detection (network
  // path only; 0 for on-device).
  uint32_t wake_chunks() const { return wake_chunks_.load(); }
  uint32_t wake_detections() const {
    if (!config_.on_device_wake) return wake_detections_.load();
    WakeEngine* e = wake_engine_.load();
    return e ? e->detections() : 0;
  }
  const char* wake_word() const {
    if (!config_.on_device_wake) return "";
    WakeEngine* e = wake_engine_.load();
    return e ? e->word() : "";
  }
  // Peak |sample| seen in the most recent capture window (0..32767). Reading
  // it resets the running peak. NOTE: fed only by the NETWORK wake path — on
  // the on-device path (default) it stays 0. A near-zero network peak means
  // the mic is effectively silent; a few thousand+ on speech means the audio
  // is fine and the model isn't matching.
  uint16_t wake_peak() { return wake_peak_.exchange(0); }

  // Copy the most recent captured mic PCM into `out` — up to `max_samples`
  // int16 samples, newest run; returns the count copied. For an off-panel
  // /mic_probe WAV dump. capture_rate() is the sample rate. Thread-safe
  // (guarded by probe_mutex_). NOTE: the probe ring is filled only by the
  // NETWORK wake path — on-device wake returns an empty snapshot.
  size_t wake_pcm_snapshot(int16_t* out, size_t max_samples);
  // Milliseconds since the probe ring was last written, or UINT32_MAX if it has
  // never been written. Reports whichever backend wake_pcm_snapshot() reads:
  // the WakeEngine's ring on the on-device path, this class's ring on the
  // network path. A caller MUST check this — a ring is only fed while its
  // producer is capturing, so it goes stale (and keeps serving identical
  // bytes) whenever capture stops. Anything above ~2000 ms means the samples
  // are a frozen snapshot, not live audio.
  uint32_t wake_pcm_age_ms() const;

  // The audio driver this satellite captures through. Exposed so diagnostics
  // (e.g. sweeping the mic preamp gain) can reach the HAL without a second
  // global. Never null for a constructed satellite.
  sensesp_cockpit_display::AudioDriver* audio() const { return audio_; }
  // Drop any retained mic PCM. Call when the mic is muted so /mic_probe can't
  // surface audio captured before the mute (privacy).
  void wake_pcm_clear();

  // Diagnostic: measure the level of all four ES7210 mic inputs to find which
  // one(s) carry a live mic. Pauses on-device wake capture so the probe is the
  // sole mic reader, runs the driver's per-channel probe, then resumes wake.
  // Returns false if the driver has no probe path. Blocks ~1 s.
  bool probe_mic_levels(sensesp_cockpit_display::AudioDriver::MicLevels& out);

  // Optional UI hook: called (from the satellite task) with the recognised
  // text when a transcript arrives, so a widget can toast it. The callback
  // must not block. Null by default.
  using TranscriptFn = void (*)(void* ctx, const char* text);
  void set_transcript_cb(TranscriptFn cb, void* ctx) {
    transcript_cb_ = cb;
    transcript_ctx_ = ctx;
  }

  // Optional privacy gate: return true to suppress the mic (wake streaming
  // AND push-to-talk). The panel's mic-mute switch wires this. When it
  // returns true the wake loop stops sending audio to the wake service and a
  // PTT press is ignored. Null = never muted. Called from the wake/socket
  // tasks; must be cheap and non-blocking.
  void set_mic_muted_fn(std::function<bool()> fn) { mic_muted_fn_ = std::move(fn); }

 private:
  static void server_task(void* arg);
  void serve();                  // accept loop
  void handle_client(int sock);  // one connection's lifetime

  static void mic_task(void* arg);
  void run_mic();  // streams audio-chunk while listening_

  // Wake mode: an outbound client to the wake service. Reconnect loop + the
  // continuous capture that feeds it while idle.
  static void wake_task(void* arg);
  void run_wake();                     // connect/reconnect loop
  bool wake_session(int sock);         // one wake-service connection's life
  bool mic_muted() const {             // consult the privacy gate
    return mic_muted_fn_ && mic_muted_fn_();
  }
  // Request + await a pipeline toward the orchestrator (used by a detection).
  // Sets ptt_held_ so the socket task starts run_mic(); waits for that stream
  // to finish so the wake loop can resume capture as the sole mic consumer.
  void run_detection_pipeline();

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

  std::function<bool()> mic_muted_fn_;

  // On-device wake (config_.on_device_wake): esp-sr AFE + WakeNet. Owns the
  // mic while listening; on detection it fires start_wake_pipeline().
  // Atomic so /hello diagnostics can read it concurrently with stop()'s
  // detach-then-delete without a use-after-free.
  std::atomic<WakeEngine*> wake_engine_{nullptr};
  void start_wake_pipeline();  // detection -> pause engine -> run pipeline -> resume

  // Network wake (legacy). wake_task_ runs run_wake() when wake_host is set.
  TaskHandle_t wake_task_ = nullptr;
  SemaphoreHandle_t wake_done_ = nullptr;  // given when run_wake() exits
  // True while a detection-triggered pipeline is in flight, so the wake loop
  // knows to hold off capture until run_mic() returns (single mic consumer).
  std::atomic<bool> pipeline_active_{false};
  // Set by on_event() when a `detection` arrives; consumed by wake_session(),
  // which closes its own capture BEFORE running the pipeline so run_mic() is
  // the sole ADC reader. Never run the pipeline from inside on_event() — the
  // wake loop still owns the ADC at that point.
  std::atomic<bool> wake_detected_{false};
  // Diagnostics (for /hello): connection + whether we're actively capturing +
  // how many chunks we've streamed to the wake service + detections seen.
  std::atomic<bool> wake_connected_{false};
  std::atomic<bool> wake_capturing_{false};
  std::atomic<uint32_t> wake_chunks_{0};
  std::atomic<uint32_t> wake_detections_{0};
  std::atomic<uint16_t> wake_peak_{0};  // max |sample| since last read

  // Ring buffer of the most recent captured PCM (for /mic_probe). ~2 s at
  // 16 kHz. Written by the wake loop, read under probe_mutex_.
  static constexpr size_t kProbeSamples = 32000;  // 2 s @ 16 kHz
  int16_t* probe_buf_ = nullptr;      // lazily allocated on first capture
  size_t probe_head_ = 0;             // next write index
  size_t probe_filled_ = 0;           // valid samples (<= kProbeSamples)
  // Tick when the ring was last WRITTEN. The ring is only fed from the wake
  // loop's chunk path, so it FREEZES whenever capture stops (mid-pipeline, or
  // any time the loop isn't streaming) and keeps returning its last contents
  // forever. Without this a caller cannot tell live audio from a stale
  // snapshot — /mic_probe silently served the same 2 s clip on 15 consecutive
  // requests, which invalidated a whole afternoon of offline wake scoring.
  std::atomic<uint32_t> probe_last_write_{0};
  // Separate written-flag: tick 0 is a legitimate timestamp, so it cannot
  // double as a "never written" sentinel.
  std::atomic<bool> probe_written_{false};
  SemaphoreHandle_t probe_mutex_ = nullptr;
  // Lock-free privacy kill switch: wake_pcm_clear() sets it so a snapshot
  // returns nothing IMMEDIATELY, even if it can't grab probe_mutex_ to zero
  // the ring; the writer clears it when it resumes filling. snapshot() honors
  // it, so muted audio can never be read back.
  std::atomic<bool> probe_disabled_{false};
};

}  // namespace sensesp_wyoming
