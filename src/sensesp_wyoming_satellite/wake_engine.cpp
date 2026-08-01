#include "sensesp_wyoming_satellite/wake_engine.h"

#include <cstdlib>
#include <cstring>

#include "esp_log.h"

// esp-sr (AFE + WakeNet). Only this .cpp pulls the headers.
#include "esp_afe_config.h"
#include "esp_afe_sr_iface.h"
#include "esp_afe_sr_models.h"
#include "model_path.h"

namespace sensesp_wyoming {

namespace {
constexpr const char* kTag = "wake_engine";

const esp_afe_sr_iface_t* afe_if(void* h) {
  return static_cast<const esp_afe_sr_iface_t*>(h);
}
esp_afe_sr_data_t* afe_dat(void* d) {
  return static_cast<esp_afe_sr_data_t*>(d);
}
}  // namespace

WakeEngine::~WakeEngine() { stop(); }

bool WakeEngine::start() {
  if (running_.exchange(true)) return true;
  feed_paused_ = xSemaphoreCreateBinary();
  feed_exited_ = xSemaphoreCreateBinary();
  fetch_exited_ = xSemaphoreCreateBinary();
  probe_mutex_ = xSemaphoreCreateMutex();

  // NOTE: do NOT dereference srmodel_list_t fields here — mirror the esp-sr
  // Arduino HAL, which passes the opaque list straight to afe_config_init()
  // and lets AFE auto-select the wakenet model. (Reading models->num across
  // the prebuilt lib boundary returned garbage; the library's own code is the
  // safe place to walk the list.)
  srmodel_list_t* models = esp_srmodel_init("model");
  if (!models) {
    ESP_LOGE(kTag, "esp_srmodel_init('model') returned null — wake disabled");
    running_.store(false);
    return false;
  }

  // Single mic, no reference channel: input format "M" (one mic channel).
  afe_config_t* cfg =
      afe_config_init("M", models, AFE_TYPE_SR, AFE_MODE_LOW_COST);
  if (!cfg) {
    ESP_LOGE(kTag, "afe_config_init failed — wake disabled");
    running_.store(false);
    return false;
  }
  // afe_config_init auto-selects the wakenet from the loaded list; if none is
  // present (empty/incompatible model partition), disable wake gracefully.
  if (!cfg->wakenet_model_name) {
    ESP_LOGE(kTag, "no WakeNet model in partition — wake disabled");
    afe_config_free(cfg);
    running_.store(false);
    return false;
  }
  strncpy(word_, cfg->wakenet_model_name, sizeof(word_) - 1);
  // Disable the WebRTC VAD gate: on this quiet mic the VAD classified real
  // speech as noise and gated it out before WakeNet, so the wake word never
  // scored (detection was intermittent at best). WakeNet does its own gating;
  // feed it every frame. Pipeline becomes [input] -> WakeNet.
  cfg->vad_init = false;
  const esp_afe_sr_iface_t* handle = esp_afe_handle_from_config(cfg);
  esp_afe_sr_data_t* data = handle ? handle->create_from_config(cfg) : nullptr;
  ESP_LOGI(kTag, "AFE wakenet '%s'", word_);
  afe_config_free(cfg);
  if (!data) {
    ESP_LOGE(kTag, "AFE create failed — on-device wake disabled");
    running_.store(false);
    return false;
  }
  afe_handle_ = (void*)handle;
  afe_data_ = (void*)data;
  feed_chunk_ = handle->get_feed_chunksize(data);
  feed_channels_ = handle->get_channel_num(data);
  // Quiet mic -> bias toward sensitivity: a lower detection threshold trades a
  // few more false accepts for reliably catching the wake word. 0 = default.
  if (threshold_ > 0.0f && handle->set_wakenet_threshold) {
    handle->set_wakenet_threshold(data, 1, threshold_);
  }

  audio_->start_capture();
  listening_.store(true);

  // Feed + fetch on core 0 (SK WS / LVGL live on the app core). Feed needs a
  // modest stack; fetch runs WakeNet inference — give it room.
  xTaskCreatePinnedToCore(&WakeEngine::feed_task_tramp, "wake_feed", 4096, this,
                          5, &feed_task_, 0);
  xTaskCreatePinnedToCore(&WakeEngine::fetch_task_tramp, "wake_fetch", 4096,
                          this, 5, &fetch_task_, 0);
  ESP_LOGI(kTag, "on-device wake started (chunk=%d ch=%d)", feed_chunk_,
           feed_channels_);
  return true;
}

void WakeEngine::stop() {
  if (!running_.exchange(false)) return;
  // JOIN both tasks: they observe running_==false and return within a tick
  // (feed after its ≤32 ms record_pcm, fetch after its ≤2 s AFE fetch), then
  // their tramps give feed_exited_/fetch_exited_. Only after BOTH have fully
  // returned is it safe to destroy the AFE and release the mic — never
  // vTaskDelete a task that may be mid-WakeNet-inference or inside the
  // detection callback. The tasks self-delete, so no vTaskDelete here.
  if (feed_task_ && feed_exited_) {
    xSemaphoreTake(feed_exited_, pdMS_TO_TICKS(3000));
    feed_task_ = nullptr;
  }
  if (fetch_task_ && fetch_exited_) {
    xSemaphoreTake(fetch_exited_, pdMS_TO_TICKS(3000));
    fetch_task_ = nullptr;
  }
  if (afe_data_ && afe_handle_) {
    afe_if(afe_handle_)->destroy(afe_dat(afe_data_));
  }
  afe_data_ = nullptr;
  afe_handle_ = nullptr;
  if (listening_.exchange(false)) audio_->stop_capture();
  if (feed_paused_) { vSemaphoreDelete(feed_paused_); feed_paused_ = nullptr; }
  if (feed_exited_) { vSemaphoreDelete(feed_exited_); feed_exited_ = nullptr; }
  if (fetch_exited_) { vSemaphoreDelete(fetch_exited_); fetch_exited_ = nullptr; }
  // The probe buffer/mutex are touched by pcm_snapshot()/clear_probe() on OTHER
  // tasks (the /hello httpd path, the mic-mute widget) that stop() hasn't
  // joined. running_ was cleared at the top of stop(); those callers re-check
  // running_ under probe_busy_, so any that started after see false and bail.
  // Wait for probe_busy_ to drain (a snapshot copies ≤2 s of PCM, bounded) so
  // none is mid-access before we free the handles. Then reset the ring
  // metadata so a restarted engine can't report a stale fill over a fresh
  // (or null) buffer.
  for (int i = 0; i < 250 && probe_busy_.load() > 0; i++) {
    vTaskDelay(pdMS_TO_TICKS(2));
  }
  if (probe_mutex_) { vSemaphoreDelete(probe_mutex_); probe_mutex_ = nullptr; }
  if (probe_) { free(probe_); probe_ = nullptr; }
  probe_head_ = 0;
  probe_filled_ = 0;
}

size_t WakeEngine::pcm_snapshot(int16_t* out, size_t max_samples) {
  if (!out || max_samples == 0) return 0;
  // Register as busy BEFORE touching the probe, so stop() (on another task)
  // waits for us before freeing probe_mutex_/probe_. Then re-check running_:
  // if stop() already cleared it, back out — stop() will observe probe_busy_
  // and wait, so the handles are still valid for this check.
  probe_busy_.fetch_add(1);
  size_t n = 0;
  if (running_.load() && probe_mutex_ &&
      xSemaphoreTake(probe_mutex_, pdMS_TO_TICKS(200)) == pdTRUE) {
    if (probe_ && probe_filled_ > 0) {
      n = probe_filled_ < max_samples ? probe_filled_ : max_samples;
      size_t start = (probe_head_ + kProbeSamples - n) % kProbeSamples;
      for (size_t i = 0; i < n; i++)
        out[i] = probe_[(start + i) % kProbeSamples];
    }
    xSemaphoreGive(probe_mutex_);
  }
  probe_busy_.fetch_sub(1);
  return n;
}

void WakeEngine::clear_probe() {
  // Same busy-guard as pcm_snapshot: this runs on the mic-mute widget task and
  // must not touch probe_mutex_/probe_ after stop() frees them.
  probe_busy_.fetch_add(1);
  if (running_.load() && probe_mutex_ &&
      xSemaphoreTake(probe_mutex_, pdMS_TO_TICKS(200)) == pdTRUE) {
    probe_head_ = 0;
    probe_filled_ = 0;
    xSemaphoreGive(probe_mutex_);
  }
  probe_busy_.fetch_sub(1);
}

void WakeEngine::pause() {
  if (!running_.load() || paused_.exchange(true)) return;
  // Drain any stale token first (a previous pause/park could have left one),
  // then wait for the feed loop to observe paused_ and PARK for THIS pause —
  // guaranteeing it's out of record_pcm() before we release the mic and hand
  // it to run_mic() as the sole consumer.
  if (feed_paused_) {
    xSemaphoreTake(feed_paused_, 0);  // clear stale
    xSemaphoreTake(feed_paused_, pdMS_TO_TICKS(500));
  }
  if (listening_.exchange(false)) audio_->stop_capture();
}

void WakeEngine::resume() {
  if (!running_.load() || !paused_.exchange(false)) return;
  audio_->start_capture();
  listening_.store(true);
  // Re-arm WakeNet cleanly for the next word: reset the AFE input ring (drop
  // any mid-pipeline fragment) and toggle wakenet off→on so its detection
  // state doesn't stay latched from the word that just fired.
  if (afe_data_ && afe_handle_) {
    const esp_afe_sr_iface_t* h = afe_if(afe_handle_);
    esp_afe_sr_data_t* d = afe_dat(afe_data_);
    h->reset_buffer(d);
    if (h->disable_wakenet) h->disable_wakenet(d);
    if (h->enable_wakenet) h->enable_wakenet(d);
  }
}

void WakeEngine::feed_task_tramp(void* arg) {
  auto* self = static_cast<WakeEngine*>(arg);
  self->feed_loop();
  // Signal stop() the loop has fully returned before self-deleting, so it can
  // safely free the AFE / release the mic without racing this task.
  xSemaphoreGive(self->feed_exited_);
  vTaskDelete(nullptr);
}
void WakeEngine::fetch_task_tramp(void* arg) {
  auto* self = static_cast<WakeEngine*>(arg);
  self->fetch_loop();
  xSemaphoreGive(self->fetch_exited_);
  vTaskDelete(nullptr);
}

void WakeEngine::feed_loop() {
  const size_t n = (size_t)feed_chunk_ * feed_channels_;
  int16_t* buf = (int16_t*)malloc(n * sizeof(int16_t));
  if (!buf) { ESP_LOGE(kTag, "feed buffer oom"); return; }

  size_t filled = 0;  // valid samples accumulated toward a full chunk
  bool parked_for_pause = false;
  while (running_.load()) {
    const bool pausing = paused_.load();
    if (pausing || (muted_fn_ && muted_fn_())) {
      // Signal pause() ONCE per pause that the mic is now free. Do NOT give
      // the token for a mute park — pause() must only unblock on a real pause,
      // or a leftover mute token would let a later pause return while we're
      // still mid-read.
      if (pausing && !parked_for_pause) {
        parked_for_pause = true;
        if (feed_paused_) xSemaphoreGive(feed_paused_);
      }
      if (!pausing) parked_for_pause = false;
      filled = 0;  // drop a partial chunk across a pause/mute gap
      vTaskDelay(pdMS_TO_TICKS(30));
      continue;
    }
    parked_for_pause = false;

    // The engine owns the mic here — the ONLY record_pcm() caller while
    // running and not paused. Accumulate until a FULL chunk is available; a
    // short read must never hand stale/uninitialised tail samples to feed().
    size_t got = audio_->record_pcm(buf + filled, feed_chunk_ - (int)filled);
    if (got == 0) { vTaskDelay(pdMS_TO_TICKS(5)); continue; }
    filled += got;
    if ((int)filled < feed_chunk_) continue;  // need a whole chunk
    filled = 0;

    // Optional pre-AFE gain: the onboard MEMS mic reads quiet, so a modest
    // boost lifts speech into WakeNet's range. Kept conservative — too much
    // just amplifies the noise floor equally. 1 = off (rely on AFE's AGC).
    if (input_gain_ > 1) {
      for (size_t i = 0; i < n; i++) {
        int32_t v = (int32_t)buf[i] * input_gain_;
        if (v > 32767) v = 32767; else if (v < -32768) v = -32768;
        buf[i] = (int16_t)v;
      }
    }
    // Tee the exact post-gain PCM into the diagnostic ring (best-effort, non-
    // blocking) so /mic_probe can dump what WakeNet actually sees on-device.
    if (probe_mutex_ && xSemaphoreTake(probe_mutex_, 0) == pdTRUE) {
      if (!probe_) probe_ = (int16_t*)malloc(kProbeSamples * sizeof(int16_t));
      if (probe_) {
        for (int i = 0; i < feed_chunk_; i++) {
          probe_[probe_head_] = buf[i];
          probe_head_ = (probe_head_ + 1) % kProbeSamples;
          if (probe_filled_ < kProbeSamples) probe_filled_++;
        }
      }
      xSemaphoreGive(probe_mutex_);
    }
    afe_if(afe_handle_)->feed(afe_dat(afe_data_), buf);
    // Match Espressif's HAL cadence: a short yield after each feed keeps the
    // AFE feed/fetch ring in step (feeding flat-out can starve the fetch side).
    vTaskDelay(2);
  }
  free(buf);
}

void WakeEngine::fetch_loop() {
  while (running_.load()) {
    afe_fetch_result_t* r = afe_if(afe_handle_)->fetch(afe_dat(afe_data_));
    if (!r || r->ret_value == ESP_FAIL) {
      vTaskDelay(pdMS_TO_TICKS(10));
      continue;
    }
    if (r->wakeup_state == WAKENET_DETECTED && !paused_.load()) {
      detections_.fetch_add(1);
      ESP_LOGI(kTag, "wake word detected (%s)", word_);
      if (on_detect_) on_detect_();
    }
  }
}

}  // namespace sensesp_wyoming
