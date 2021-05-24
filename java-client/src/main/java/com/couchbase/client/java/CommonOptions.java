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

package com.couchbase.client.java;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Common options that are used by most operations.
 *
 * @since 2.0.0
 */
public abstract class CommonOptions<SELF extends CommonOptions<SELF>> {

  /**
   * The timeout for the operation, if set.
   */
  private Optional<Duration> timeout = Optional.empty();

  /**
   * The custom retry strategy, if set.
   */
  private Optional<RetryStrategy> retryStrategy = Optional.empty();

  /**
   * The client context data, if set.
   */
  private Map<String, Object> clientContext = null;

  /**
   * If set holds the parent span that should be used for this request
   */
  private Optional<RequestSpan> parentSpan = Optional.empty();

  /**
   * Allows to return the right options builder instance for child implementations.
   */
  @SuppressWarnings({ "unchecked" })
  protected SELF self() {
    return (SELF) this;
  }

  /**
   * Specifies a custom per-operation timeout.
   *
   * <p>Note: if a custom timeout is provided through this builder, it will override the default set
   * on the environment.</p>
   *
   * @param timeout the timeout to use for this operation.
   * @return this options builder for chaining purposes.
   */
  public SELF timeout(final Duration timeout) {
    this.timeout = Optional.ofNullable(timeout);
    return self();
  }

  /**
   * Specifies a custom {@link RetryStrategy} for this operation.
   *
   * <p>Note: if a custom strategy is provided through this builder, it will override the default set
   * on the environment.</p>
   *
   * @param retryStrategy the retry strategy to use for this operation.
   * @return this options builder for chaining purposes.
   */
  public SELF retryStrategy(final RetryStrategy retryStrategy) {
    this.retryStrategy = Optional.ofNullable(retryStrategy);
    return self();
  }

  /**
   * Specifies custom, client domain specific context metadata with this operation.
   *
   * @param clientContext the client context information as a map.
   * @return this options builder for chaining purposes.
   */
  public SELF clientContext(final Map<String, Object> clientContext) {
    this.clientContext = clientContext;
    return self();
  }

  /**
   * Allows to specify a parent span that should be used on top of this request.
   * <p>
   * Note that this only has impact when using a tracing implementation that can actually deal with the notion
   * of a parent. You likely want to use this if you want to wire up your application with OpenTracing or
   * OpenTelemetry - use the support separate modules for that.
   * <p>
   * IMPORTANT: this is a volatile, likely to change API!
   *
   * @param parentSpan the parent span for this request.
   * @return this options builder for chaining purposes.
   */
  @Stability.Volatile
  public SELF parentSpan(final RequestSpan parentSpan) {
    this.parentSpan = Optional.ofNullable(parentSpan);
    return self();
  }

  @Stability.Internal
  public abstract class BuiltCommonOptions implements CoreCommonOptions {

    /**
     * Returns the custom retry strategy if provided.
     */
    public Optional<RetryStrategy> retryStrategy() {
      return retryStrategy;
    }

    /**
     * Returns the custom timeout if provided.
     */
    public Optional<Duration> timeout() {
      return timeout;
    }

    /**
     * Returns the client context, or null if not present.
     */
    public Map<String, Object> clientContext() {
      return clientContext;
    }

    /**
     * Returns the parent span provided by the user if present.
     */
    public Optional<RequestSpan> parentSpan() {
      return parentSpan;
    }

  }

}
