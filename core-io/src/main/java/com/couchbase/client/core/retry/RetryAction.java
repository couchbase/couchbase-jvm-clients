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

package com.couchbase.client.core.retry;

import com.couchbase.client.core.annotation.Stability;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * The {@link RetryAction} describes how and when a request should be retried.
 */
public class RetryAction {

  /**
   * Default retry action if the request should not be retried.
   */
  private static final RetryAction NO_RETRY = new RetryAction(Optional.empty(), Function.identity());

  /**
   * Stores the duration (if present) of the next retry delay.
   */
  private final Optional<Duration> duration;

  /**
   * Translates the default cancellation exception into the desired type.
   */
  private final Function<Throwable, Throwable> exceptionTranslator;

  /**
   * Creates a new {@link RetryAction} with the required duration.
   *
   * @param duration the duration when (and if) the operation should be retried again.
   * @param exceptionTranslator transformation to apply to the original request cancellation exception.
   */
  private RetryAction(final Optional<Duration> duration, final Function<Throwable, Throwable> exceptionTranslator) {
    this.duration = requireNonNull(duration);
    this.exceptionTranslator = requireNonNull(exceptionTranslator);
  }

  /**
   * Constructs a new {@link RetryAction} indicating that the request should be retried after the given duration.
   *
   * @param duration the duration after which the request should be retried.
   * @return a new {@link RetryAction} indicating retry.
   */
  public static RetryAction withDuration(final Duration duration) {
    return new RetryAction(Optional.of(duration), Function.identity());
  }

  /**
   * Constructs a new {@link RetryAction} indicating that the request should not be retried.
   *
   * @return a new {@link RetryAction} indicating no retry.
   */
  public static RetryAction noRetry() {
    return NO_RETRY;
  }

  /**
   * Constructs a new {@link RetryAction} indicating that the request should not be retried,
   * and a specific exception should be used to fail the request.
   *
   * @param exceptionTranslator customizes the exception used to fail the request.
   * If the translator creates a new exception, to preserve the error context it should
   * use the function argument as the new exception's cause.
   *
   * @return a new {@link RetryAction} indicating no retry and a custom exception.
   */
  @Stability.Volatile
  public static RetryAction noRetry(Function<Throwable, Throwable> exceptionTranslator) {
    return new RetryAction(Optional.empty(), exceptionTranslator);
  }

  /**
   * If present, the operation should be retried after the given duration.
   *
   * @return the duration indicating if (and when) the request should be retried.
   */
  public Optional<Duration> duration() {
    return duration;
  }

  /**
   * If a request is cancelled as the result of this retry action, the
   * throwable used to fail the request should be translated by this function.
   */
  public Function<Throwable, Throwable> exceptionTranslator() {
    return exceptionTranslator;
  }

}
