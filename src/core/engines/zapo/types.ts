/**
 * Per-session engine configuration, set through "config.engine" when creating
 * the session (WAHA_SESSION_CONFIG_* / POST /api/sessions).
 */
export interface ZapoConfig {
  /**
   * Generate thumbnails, probes and waveforms for outgoing media through
   * @zapo-js/media-utils. Needs sharp (bundled) and ffmpeg/ffprobe on PATH -
   * a missing binary only skips the affected step. Defaults to true.
   */
  media?: boolean;

  /**
   * Emit the WAM telemetry batches WhatsApp Web sends, through
   * @zapo-js/wam. Improves wire parity with the official client.
   * Defaults to true.
   */
  wam?: boolean;

  /**
   * Install the VoIP plugin (@zapo-js/voip), exposing client.voip.
   * Defaults to false: this engine does not map calls to the WAHA call
   * events yet, so the coordinator would have no consumer.
   *
   * Turning it on also requires installing its peer dependencies
   * (@roamhq/wrtc and libmlow-wasm) - the plugin fails to load without them.
   */
  voip?: boolean;
}
