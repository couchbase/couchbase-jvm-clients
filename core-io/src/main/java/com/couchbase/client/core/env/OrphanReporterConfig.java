/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.OrphanReporter;
import com.couchbase.client.core.cnc.tracing.ThresholdRequestTracer;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Allows to customize the behavior of the {@link OrphanReporter}.
 */
public class OrphanReporterConfig {

  /**
   * Emit the event by default every 10 seconds.
   */
  private static final Duration DEFAULT_EMIT_INTERVAL = Duration.ofSeconds(10);

  /**
   * Only sample a maximum amount of 10 entries per interval.
   */
  private static final int DEFAULT_SAMPLE_SIZE = 10;

  /**
   * Only allow to enqueue a maximum of 1024 orphans that are waiting to be picked up by the reporter.
   */
  private static final int DEFAULT_QUEUE_LENGTH = 1024;

  /**
   * The currently configured emit interval.
   */
  private final Duration emitInterval;

  /**
   * The currently configured sample size.
   */
  private final int sampleSize;

  /**
   * The currently configured queue length.
   */
  private final int queueLength;

  /**
   * Creates a new {@link OrphanReporterConfig}.
   * <p>
   * Note that this method is private because users are supposed to construct it through the {@link Builder} or
   * the static factory methods like {@link #create()}.
   *
   * @param builder the builder which provides the config properties.
   */
  private OrphanReporterConfig(final Builder builder) {
    sampleSize = builder.sampleSize;
    emitInterval = builder.emitInterval;
    queueLength = builder.queueLength;
  }

  /**
   * Allows to configure a custom {@link OrphanReporterConfig} through a Builder API.
   *
   * @return the builder to customize the config.
   */
  public static OrphanReporterConfig.Builder builder() {
    return new OrphanReporterConfig.Builder();
  }

  /**
   * Creates the default config for the {@link OrphanReporter}.
   *
   * @return the default config.
   */
  public static OrphanReporterConfig create() {
    return builder().build();
  }

  /**
   * Allows to customize the sample size per service.
   *
   * @param sampleSize the sample size to set.
   * @return this builder for chaining.
   */
  public static Builder sampleSize(final int sampleSize) {
    return builder().sampleSize(sampleSize);
  }

  /**
   * Allows to customize the event emit interval.
   *
   * @param emitInterval the interval to use.
   * @return this builder for chaining.
   */
  public static Builder emitInterval(final Duration emitInterval) {
    return builder().emitInterval(emitInterval);
  }

  /**
   * Allows to configure the max queue size for the responses waiting to be analyzed for reporting.
   *
   * @param queueLength the queue size to use.
   * @return this builder for chaining.
   */
  public static Builder queueLength(final int queueLength) {
    return builder().queueLength(queueLength);
  }

  /**
   * Returns the configured emit interval.
   */
  public Duration emitInterval() {
    return emitInterval;
  }

  /**
   * Returns the configured sample size.
   */
  public int sampleSize() {
    return sampleSize;
  }

  /**
   * Returns the configured queue length.
   */
  public int queueLength() {
    return queueLength;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("emitIntervalMs", emitInterval.toMillis());
    export.put("sampleSize", sampleSize);
    export.put("queueLength", queueLength);
    return export;
  }

  /**
   * The builder which allows customization of the {@link OrphanReporterConfig}.
   */
  public static class Builder {

    private Duration emitInterval = DEFAULT_EMIT_INTERVAL;
    private int sampleSize = DEFAULT_SAMPLE_SIZE;
    private int queueLength = DEFAULT_QUEUE_LENGTH;

    /**
     * Allows to customize the event emit interval
     *
     * @param emitInterval the interval to use.
     * @return this builder for chaining.
     */
    public Builder emitInterval(final Duration emitInterval) {
      if (emitInterval.isZero()) {
        throw InvalidArgumentException.fromMessage("Emit interval needs to be greater than 0");
      }

      this.emitInterval = emitInterval;
      return this;
    }

    /**
     * Allows to configure the max queue size for the responses waiting to be analyzed for reporting.
     *
     * @param queueLength the queue size to use.
     * @return this builder for chaining.
     */
    public Builder queueLength(final int queueLength) {
      this.queueLength = queueLength;
      return this;
    }

    /**
     * Allows to customize the sample size per service.
     *
     * @param sampleSize the sample size to set.
     * @return this builder for chaining.
     */
    public Builder sampleSize(final int sampleSize) {
      this.sampleSize = sampleSize;
      return this;
    }

    /**
     * Creates a config out of this builder and freezes it effectively.
     *
     * @return the built config.
     */
    public OrphanReporterConfig build() {
      return new OrphanReporterConfig(this);
    }

  }

}
