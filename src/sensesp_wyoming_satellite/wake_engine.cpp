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
  // afe_config_init's auto-scan for the wakenet has come up empty on this
  // esp-sr build even with the model in the partition; set it explicitly from
  // the loaded list (filter for the "wn" prefix) and force wakenet on.
  if (!cfg->wakenet_model_name) {
    ESP_LOGE(kTag, "no WakeNet model in partition — wake disabled");
    afe_config_free(cfg);
    running_.store(false);
    return false;
  }
  strncpy(word_, cfg->wakenet_model_name, sizeof(word_) - 1);
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
  // Tasks watch running_ and exit on their next tick; join by polling their
  // handles is overkill — a short settle plus capture release is enough here
  // since stop() is only called at OTA-quiesce / teardown.
  vTaskDelay(pdMS_TO_TICKS(50));
  if (feed_task_) { vTaskDelete(feed_task_); feed_task_ = nullptr; }
  if (fetch_task_) { vTaskDelete(fetch_task_); fetch_task_ = nullptr; }
  if (afe_data_ && afe_handle_) {
    afe_if(afe_handle_)->destroy(afe_dat(afe_data_));
  }
  afe_data_ = nullptr;
  afe_handle_ = nullptr;
  if (listening_.exchange(false)) audio_->stop_capture();
  if (feed_paused_) { vSemaphoreDelete(feed_paused_); feed_paused_ = nullptr; }
}

void WakeEngine::pause() {
  if (!running_.load() || paused_.exchange(true)) return;
  // Wait for the feed loop to observe paused_ and release the mic, so the
  // caller can hand record_pcm() to run_mic() as the sole consumer.
  if (feed_paused_) xSemaphoreTake(feed_paused_, pdMS_TO_TICKS(500));
  if (listening_.exchange(false)) audio_->stop_capture();
}

void WakeEngine::resume() {
  if (!running_.load() || !paused_.exchange(false)) return;
  audio_->start_capture();
  listening_.store(true);
  // Drop any stale AFE buffer so a mid-pipeline word fragment doesn't linger.
  if (afe_data_ && afe_handle_) afe_if(afe_handle_)->reset_buffer(afe_dat(afe_data_));
}

void WakeEngine::feed_task_tramp(void* arg) {
  static_cast<WakeEngine*>(arg)->feed_loop();
  vTaskDelete(nullptr);
}
void WakeEngine::fetch_task_tramp(void* arg) {
  static_cast<WakeEngine*>(arg)->fetch_loop();
  vTaskDelete(nullptr);
}

void WakeEngine::feed_loop() {
  const size_t n = (size_t)feed_chunk_ * feed_channels_;
  int16_t* buf = (int16_t*)malloc(n * sizeof(int16_t));
  if (!buf) { ESP_LOGE(kTag, "feed buffer oom"); return; }

  bool parked = false;
  while (running_.load()) {
    if (paused_.load() || (muted_fn_ && muted_fn_())) {
      if (!parked) {
        parked = true;
        // Signal pause() (only meaningful for the paused_ case; harmless for
        // mute) that the mic is now free.
        if (feed_paused_) xSemaphoreGive(feed_paused_);
      }
      vTaskDelay(pdMS_TO_TICKS(30));
      continue;
    }
    parked = false;

    // The engine owns the mic here — the ONLY record_pcm() caller while
    // running and not paused. get_feed_chunksize samples per channel; mono.
    size_t got = audio_->record_pcm(buf, feed_chunk_);
    if (got == 0) { vTaskDelay(pdMS_TO_TICKS(5)); continue; }
    // Optional pre-AFE gain: the onboard MEMS mic reads quiet, so a modest
    // boost lifts speech into WakeNet's range. Kept conservative — too much
    // just amplifies the noise floor equally and hurts the SNR the WebRTC VAD
    // needs. 1 = off (rely on the AFE's own AGC).
    if (input_gain_ > 1) {
      for (int i = 0; i < feed_chunk_; i++) {
        int32_t v = (int32_t)buf[i] * input_gain_;
        if (v > 32767) v = 32767; else if (v < -32768) v = -32768;
        buf[i] = (int16_t)v;
      }
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
