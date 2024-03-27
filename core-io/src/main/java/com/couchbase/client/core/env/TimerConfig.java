/*
 * Copyright (c) 2023 Couchbase, Inc.
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
import java.util.function.Consumer;

/**
 * Configures the internal Timer, which asynchronous retries and timeouts fire on.
 * <p>
 * Most users will have no reason to adjust these settings.  It is intended for situations
 * where testing has shown that the default settings are unable to, for example, fire high-throughput
 * timeouts or retries quickly enough or with accurate enough timing precision.
 */
@Stability.Volatile
public class TimerConfig {
  public static final int DEFAULT_NUM_TIMERS = 1;
  public static final Duration DEFAULT_TICK_DURATION = Duration.ofMillis(10);
  public static final int DEFAULT_NUM_BUCKETS = 512;

  private final int numTimers;
  private final Duration tickDuration;
  private final int numBuckets;

  private TimerConfig(Builder builder) {
    numTimers = builder.numTimers;
    tickDuration = builder.tickDuration;
    numBuckets = builder.numBuckets;
  }

  /**
   * @deprecated Instead, please use
   * {@link IoConfig.Builder#timerConfig(Consumer)}
   * and configure the builder passed to the consumer.
   */
  @Deprecated
  public static TimerConfig create() {
    return builder().build();
  }

  /**
   * @deprecated Instead of creating a new builder, please use
   * {@link IoConfig.Builder#timerConfig(Consumer)}
   * and configure the builder passed to the consumer.
   */
  @Deprecated
  public static Builder builder() {
    return new Builder();
  }

  public int numTimers() {
    return numTimers;
  }

  public Duration tickDuration() {
    return tickDuration;
  }

  public int numBuckets() {
    return numBuckets;
  }

  @Stability.Internal
  public Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("numTimers", numTimers);
    export.put("tickDurationMs", tickDuration.toMillis());
    export.put("numBuckets", numBuckets);
    return export;
  }

  public static class Builder {
    private int numTimers = DEFAULT_NUM_TIMERS;
    private Duration tickDuration = DEFAULT_TICK_DURATION;
    private int numBuckets = DEFAULT_NUM_BUCKETS;

    /**
     * Configures multiple parallel Timers, allowing more throughput of retries and timeouts.
     * <p>
     * It defaults to 1.
     */
    @Stability.Volatile
    public Builder numTimers(int numTimers) {
      this.numTimers = numTimers;
      return this;
    }

    /**
     * Configures the time between each 'tick' of the Timer(s).
     * <p>
     * Timeouts and retries can only fire as accurately as this resolution.  If reducing the value,
     * users may want to increase the {@link #numBuckets(int)} setting.
     * <p>
     * It defaults to 10 milliseconds.
     */
    @Stability.Volatile
    public Builder tickDuration(Duration tickDuration) {
      this.tickDuration = tickDuration;
      return this;
    }

    /**
     * Internally, each Timer divides time into a number of buckets, which this setting controls.
     * <p>
     * It defaults to 512.
     * <p>
     * If it's expected that many timeouts or retries will be firing, users may want to increase
     * this setting.
     */
    @Stability.Volatile
    public Builder numBuckets(int numBuckets) {
      this.numBuckets = numBuckets;
      return this;
    }

    public TimerConfig build() {
      return new TimerConfig(this);
    }
  }
}
