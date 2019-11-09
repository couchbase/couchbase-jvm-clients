/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Volatile
public class DiagnosticsConfig {

  /**
   * By default, emit every 30 minutes.
   */
  private static final Duration DEFAULT_EMIT_INTERVAL = Duration.ofMinutes(30);

  /**
   * For now, diagnostics are disabled by default.
   */
  private static final boolean DEFAULT_ENABLED = false;

  /**
   * The configured emit interval.
   */
  private final Duration emitInterval;

  /**
   * If diagnostics are enabled or not.
   */
  private final boolean enabled;

  private DiagnosticsConfig(final Builder builder) {
    this.emitInterval = builder.emitInterval;
    this.enabled = builder.enabled;
  }

  public Duration emitInterval() {
    return emitInterval;
  }

  public boolean enabled() {
    return enabled;
  }

  public static Builder builder() {
    return new DiagnosticsConfig.Builder();
  }

  public static DiagnosticsConfig create() {
    return builder().build();
  }

  public static Builder emitInterval(final Duration emitInterval) {
    return builder().emitInterval(emitInterval);
  }

  public static Builder enabled(final boolean enabled) {
    return builder().enabled(enabled);
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("enabled", enabled);
    export.put("emitInterval", emitInterval.toString());
    return export;
  }

  @Stability.Volatile
  public static class Builder {

    public DiagnosticsConfig build() {
      return new DiagnosticsConfig(this);
    }

    private Duration emitInterval = DEFAULT_EMIT_INTERVAL;
    private boolean enabled = DEFAULT_ENABLED;

    public Builder enabled(final boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder emitInterval(final Duration emitInterval) {
      notNull(emitInterval, "EmitInterval");
      this.emitInterval = emitInterval;
      return this;
    }

  }
}
