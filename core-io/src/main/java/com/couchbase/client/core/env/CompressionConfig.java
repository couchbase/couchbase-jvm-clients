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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Allows configuring and customizing the compression configuration.
 *
 * @since 2.0.0
 */
public class CompressionConfig {

  public static final boolean DEFAULT_ENABLED = true;
  public static final int DEFAULT_MIN_SIZE = 32;
  public static final double DEFAULT_MIN_RATIO = 0.83;

  /**
   * If compression is enabled or not.
   */
  private final boolean enabled;

  /**
   * The minimum size when compression should be performed.
   */
  private final int minSize;

  /**
   * The minimum ratio of when a compressed doc should be sent.
   */
  private final double minRatio;

  /**
   * Creates a {@link CompressionConfig} with default arguments.
   *
   * @return a new {@link CompressionConfig}.
   * @deprecated Instead, please use
   * {@link CoreEnvironment.Builder#compressionConfig(Consumer)}
   * and configure the builder passed to the consumer.
   * Note: CoreEnvironment is a base class; you'll
   * probably call that method via a subclass named
   * {@code ClusterEnvironment}.
   */
  @Deprecated
  public static CompressionConfig create() {
    return builder().build();
  }

  /**
   * This builder allows to customize a {@link CompressionConfig}.
   *
   * @return a builder to configure {@link CompressionConfig}.
   * @deprecated Instead of creating a new builder, please use
   * {@link CoreEnvironment.Builder#compressionConfig(Consumer)}
   * and configure the builder passed to the consumer.
   * Note: CoreEnvironment is a base class; you'll
   * probably call that method via a subclass named
   * {@code ClusterEnvironment}.
   */
  @Deprecated
  public static Builder builder() {
    return new CompressionConfig.Builder();
  }

  /**
   * If set to false, disabled compression.
   *
   * @param enabled true to enable, false otherwise.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder enable(boolean enabled) {
    return builder().enable(enabled);
  }

  /**
   * The minimum size after which compression is performed.
   *
   * <p>The default is 32 bytes.</p>
   *
   * @param minSize minimum size in bytes.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder minSize(int minSize) {
    return builder().minSize(minSize);
  }

  /**
   * The minimum ratio after which a compressed doc is sent compressed
   * versus the uncompressed version is sent for efficiency.
   *
   * <p>The default is 0.83.</p>
   *
   * @param minRatio the minimum ratio.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder minRatio(double minRatio) {
    return builder().minRatio(minRatio);
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("enabled", enabled);
    export.put("minRatio", minRatio);
    export.put("minSize", minSize);
    return export;
  }

  /**
   * Internal constructor for a compression config.
   *
   * @param builder the builder used to customize the options.
   */
  private CompressionConfig(final Builder builder) {
    this.enabled = builder.enabled;
    this.minRatio = builder.minRatio;
    this.minSize = builder.minSize;
  }

  /**
   * Returns the minimum configured compression size.
   *
   * @return the minimum compression size.
   */
  public int minSize() {
    return minSize;
  }

  /**
   * Returns the minimum effective ratio to send when compressed.
   *
   * @return the minimum effective ratio.
   */
  public double minRatio() {
    return minRatio;
  }

  /**
   * True if compression should be enabled, false otherwise.
   *
   * @return true if enabled, false otherwise.
   */
  public boolean enabled() {
    return enabled;
  }

  /**
   * This builder allows to customize the {@link CompressionConfig}.
   */
  public static class Builder {

    private boolean enabled = DEFAULT_ENABLED;
    private int minSize = DEFAULT_MIN_SIZE;
    private double minRatio = DEFAULT_MIN_RATIO;

    public CompressionConfig build() {
      return new CompressionConfig(this);
    }

    /**
     * If set to false, disabled compression.
     *
     * @param enabled true to enable, false otherwise.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enable(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * The minimum size after which compression is performed.
     *
     * @param minSize minimum size in bytes.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder minSize(int minSize) {
      this.minSize = minSize;
      return this;
    }

    /**
     * The minimum ratio after which a compressed doc is sent compressed
     * versus the uncompressed version is sent for efficiency.
     *
     * @param minRatio the minimum ratio.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder minRatio(double minRatio) {
      this.minRatio = minRatio;
      return this;
    }

  }

}
