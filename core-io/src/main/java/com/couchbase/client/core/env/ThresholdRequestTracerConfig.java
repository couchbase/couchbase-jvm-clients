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
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class ThresholdRequestTracerConfig {

  private static final Duration DEFAULT_EMIT_INTERVAL = Duration.ofSeconds(10);
  private static final int DEFAULT_QUEUE_LENGTH = 1024;
  private static final Duration DEFAULT_KV_THRESHOLD = Duration.ofMillis(500);
  private static final Duration DEFAULT_QUERY_THRESHOLD = Duration.ofSeconds(1);
  private static final Duration DEFAULT_VIEW_THRESHOLD = Duration.ofSeconds(1);
  private static final Duration DEFAULT_SEARCH_THRESHOLD = Duration.ofSeconds(1);
  private static final Duration DEFAULT_ANALYTICS_THRESHOLD = Duration.ofSeconds(1);
  private static final int DEFAULT_SAMPLE_SIZE = 10;

  private final Duration emitInterval;
  private final int queueLength;
  private final int sampleSize;
  private final Duration kvThreshold;
  private final Duration queryThreshold;
  private final Duration viewThreshold;
  private final Duration searchThreshold;
  private final Duration analyticsThreshold;

  public static Builder builder() {
    return new Builder();
  }

  public static ThresholdRequestTracerConfig create() {
    return builder().build();
  }

  ThresholdRequestTracerConfig(final Builder builder) {
    emitInterval = builder.emitInterval;
    queueLength = builder.queueLength;
    sampleSize = builder.sampleSize;
    kvThreshold = builder.kvThreshold;
    queryThreshold = builder.queryThreshold;
    viewThreshold = builder.viewThreshold;
    searchThreshold = builder.searchThreshold;
    analyticsThreshold = builder.analyticsThreshold;
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
   * Allows to customize the emit interval
   *
   * @param emitInterval the interval to use.
   * @return this builder for chaining.
   */
  public static Builder emitInterval(final Duration emitInterval) {
    return builder().emitInterval(emitInterval);
  }

  /**
   * Allows to configure the queue size for the individual span queues
   * used to track the spans over threshold.
   *
   * @param queueLength the queue size to use.
   * @return this builder for chaining.
   */
  public static Builder queueLength(final int queueLength) {
    return builder().queueLength(queueLength);
  }

  /**
   * Allows to customize the kvThreshold.
   *
   * @param kvThreshold the threshold to set.
   * @return this builder for chaining.
   */
  public static Builder kvThreshold(final Duration kvThreshold) {
    return builder().kvThreshold(kvThreshold);
  }

  /**
   * Allows to customize the analyticsThreshold.
   *
   * @param analyticsThreshold the threshold to set.
   * @return this builder for chaining.
   */
  public static Builder analyticsThreshold(final Duration analyticsThreshold) {
    return builder().analyticsThreshold(analyticsThreshold);
  }

  /**
   * Allows to customize the n1qlThreshold.
   *
   * @param queryThreshold the threshold to set.
   * @return this builder for chaining.
   */
  public static Builder queryThreshold(final Duration queryThreshold) {
    return builder().queryThreshold(queryThreshold);
  }

  /**
   * Allows to customize the ftsThreshold.
   *
   * @param searchThreshold the threshold to set.
   * @return this builder for chaining.
   */
  public static Builder searchThreshold(final Duration searchThreshold) {
    return builder().searchThreshold(searchThreshold);
  }

  /**
   * Allows to customize the viewThreshold.
   *
   * @param viewThreshold the threshold to set.
   * @return this builder for chaining.
   */
  public static Builder viewThreshold(final Duration viewThreshold) {
    return builder().viewThreshold(viewThreshold);
  }

  public Duration emitInterval() {
    return emitInterval;
  }

  public int queueLength() {
    return queueLength;
  }

  public int sampleSize() {
    return sampleSize;
  }

  public Duration kvThreshold() {
    return kvThreshold;
  }

  public Duration queryThreshold() {
    return queryThreshold;
  }

  public Duration viewThreshold() {
    return viewThreshold;
  }

  public Duration searchThreshold() {
    return searchThreshold;
  }

  public Duration analyticsThreshold() {
    return analyticsThreshold;
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

    export.put("kvThresholdMs", kvThreshold.toMillis());
    export.put("queryThresholdMs", queryThreshold.toMillis());
    export.put("searchThresholdMs", searchThreshold.toMillis());
    export.put("analyticsThresholdMs", analyticsThreshold.toMillis());
    export.put("viewThresholdMs", viewThreshold.toMillis());

    return export;
  }

  public static class Builder {

    private Duration emitInterval = DEFAULT_EMIT_INTERVAL;
    private int queueLength = DEFAULT_QUEUE_LENGTH;
    private int sampleSize = DEFAULT_SAMPLE_SIZE;
    private Duration kvThreshold = DEFAULT_KV_THRESHOLD;
    private Duration queryThreshold = DEFAULT_QUERY_THRESHOLD;
    private Duration viewThreshold = DEFAULT_VIEW_THRESHOLD;
    private Duration searchThreshold = DEFAULT_SEARCH_THRESHOLD;
    private Duration analyticsThreshold = DEFAULT_ANALYTICS_THRESHOLD;

    /**
     * Allows to customize the emit interval
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
     * Allows to customize the kvThreshold.
     *
     * @param kvThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder kvThreshold(final Duration kvThreshold) {
      this.kvThreshold = kvThreshold;
      return this;
    }

    /**
     * Allows to customize the n1qlThreshold.
     *
     * @param queryThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder queryThreshold(final Duration queryThreshold) {
      this.queryThreshold = queryThreshold;
      return this;
    }

    /**
     * Allows to customize the viewThreshold.
     *
     * @param viewThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder viewThreshold(final Duration viewThreshold) {
      this.viewThreshold = viewThreshold;
      return this;
    }

    /**
     * Allows to customize the ftsThreshold.
     *
     * @param searchThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder searchThreshold(final Duration searchThreshold) {
      this.searchThreshold = searchThreshold;
      return this;
    }

    /**
     * Allows to customize the analyticsThreshold.
     *
     * @param analyticsThreshold the threshold to set.
     * @return this builder for chaining.
     */
    public Builder analyticsThreshold(final Duration analyticsThreshold) {
      this.analyticsThreshold = analyticsThreshold;
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

    public ThresholdRequestTracerConfig build() {
      return new ThresholdRequestTracerConfig(this);
    }

  }

}
