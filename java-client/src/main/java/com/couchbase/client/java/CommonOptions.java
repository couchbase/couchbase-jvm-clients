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

import com.couchbase.client.core.retry.RetryStrategy;
import io.opentracing.Span;

import java.time.Duration;
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
   * The OpenTracing span to be used as a parent.
   */
  private Optional<Span> parentSpan = Optional.empty();

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
   * Returns the custom timeout if provided.
   */
  public Optional<Duration> timeout() {
    return timeout;
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
   * Returns the custom retry strategy if provided.
   */
  public Optional<RetryStrategy> retryStrategy() {
    return retryStrategy;
  }

  /**
   * Specifies a parent OpenTracing {@link Span} for this operation.
   *
   * <p>If no parent span is provided, depending on the tracer implementation used, a new one might be created
   * which is not associated to any other parent.</p>
   *
   * @param span the parent span to use for this operation.
   * @return this options builder for chaining purposes.
   */
  public SELF parentSpan(final Span span) {
    this.parentSpan = Optional.ofNullable(span);
    return self();
  }

  /**
   * Returns the parent span if provided.
   */
  public Optional<Span> parentSpan() {
    return parentSpan;
  }

}
