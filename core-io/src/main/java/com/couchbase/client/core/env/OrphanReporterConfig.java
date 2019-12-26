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
import com.couchbase.client.core.cnc.tracing.ThresholdRequestTracer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class OrphanReporterConfig {

  private static final Duration DEFAULT_EMIT_INTERVAL = Duration.ofSeconds(10);
  private static final int DEFAULT_SAMPLE_SIZE = 10;
  private static final int DEFAULT_QUEUE_LENGTH = 1024;

  private final Duration emitInterval;
  private final int sampleSize;
  private final int queueLength;

  private OrphanReporterConfig(final Builder builder) {
    sampleSize = builder.sampleSize;
    emitInterval = builder.emitInterval;
    queueLength = builder.queueLength;
  }

  public static OrphanReporterConfig.Builder builder() {
    return new OrphanReporterConfig.Builder();
  }

  public static OrphanReporterConfig create() {
    return builder().build();
  }

  public static Builder sampleSize(final int sampleSize) {
    return builder().sampleSize(sampleSize);
  }

  public static Builder emitInterval(final Duration emitInterval) {
    return builder().emitInterval(emitInterval);
  }

  public static Builder queueLength(final int queueLength) {
    return builder().queueLength(queueLength);
  }

  public Duration emitInterval() {
    return emitInterval;
  }

  public int sampleSize() {
    return sampleSize;
  }

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

  public static class Builder {

    private Duration emitInterval = DEFAULT_EMIT_INTERVAL;
    private int sampleSize = DEFAULT_SAMPLE_SIZE;
    private int queueLength = DEFAULT_QUEUE_LENGTH;

    /**
     * Allows to customize the emit interval
     *
     * @param emitInterval the interval to use.
     * @return this builder for chaining.
     */
    public Builder emitInterval(final Duration emitInterval) {
      if (emitInterval.isZero()) {
        throw new IllegalArgumentException("Emit interval needs to be greater than 0");
      }

      this.emitInterval = emitInterval;
      return this;
    }

    /**
     * Allows to configure the queue size for the individual span queues
     * used to track the spans over threshold.
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

    public OrphanReporterConfig build() {
      return new OrphanReporterConfig(this);
    }
  }

}
