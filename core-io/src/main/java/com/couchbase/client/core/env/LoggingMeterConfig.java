/*
 * Copyright (c) 2020 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.metrics.LoggingMeter;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Allows to configure the {@link LoggingMeter}.
 * <p>
 * Note: the metrics implementation is considered volatile, and so is this configuration. It is subject to
 * change at any time.
 */
public class LoggingMeterConfig {

  @Stability.Internal
  public static class Defaults {
    public static final boolean DEFAULT_ENABLED = true;
    public static final Duration DEFAULT_EMIT_INTERVAL = Duration.ofSeconds(600);
  }

  private final Duration emitInterval;
  private final boolean enabled;

  /**
   * @deprecated Instead of creating a new builder, please use
   * {@link CoreEnvironment.Builder#loggingMeterConfig(Consumer)}
   * and configure the builder passed to the consumer.
   * Note: CoreEnvironment is a base class; you'll
   * probably call that method via a subclass named
   * {@code ClusterEnvironment}.
   */
  @Deprecated
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @deprecated Instead, please use
   * {@link CoreEnvironment.Builder#loggingMeterConfig(Consumer)}
   * and configure the builder passed to the consumer.
   * Note: CoreEnvironment is a base class; you'll
   * probably call that method via a subclass named
   * {@code ClusterEnvironment}.
   */
  @Deprecated
  public static LoggingMeterConfig create() {
    return builder().build();
  }

  /**
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static LoggingMeterConfig disabled() {
    return enabled(false).build();
  }

  LoggingMeterConfig(final Builder builder) {
    emitInterval = builder.emitInterval;
    enabled = builder.enabled;
  }

  /**
   * Allows to customize the emit interval
   *
   * @param emitInterval the interval to use.
   * @return this builder for chaining.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder emitInterval(final Duration emitInterval) {
    return builder().emitInterval(emitInterval);
  }

  /**
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder enabled(final boolean enabled) {
    return builder().enabled(enabled);
  }

  public Duration emitInterval() {
    return emitInterval;
  }

  public boolean enabled() {
    return enabled;
  }

  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();

    export.put("enabled", enabled);
    export.put("emitIntervalMs", emitInterval.toMillis());

    return export;
  }

  public static class Builder {

    private Duration emitInterval = Defaults.DEFAULT_EMIT_INTERVAL;
    private boolean enabled = Defaults.DEFAULT_ENABLED;

    /**
     * Allows to customize the emit interval
     *
     * @param emitInterval the interval to use.
     * @return this builder for chaining.
     */
    public Builder emitInterval(final Duration emitInterval) {
      if (emitInterval.isZero()) {
        throw InvalidArgumentException.fromMessage("Emit interval must be greater than 0");
      }

      this.emitInterval = emitInterval;
      return this;
    }

    public Builder enabled(final boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public LoggingMeterConfig build() {
      return new LoggingMeterConfig(this);
    }
  }
}
