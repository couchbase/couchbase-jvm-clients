/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.CircuitBreaker.State;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * A circuit breaker that judges individual dispatch attempts,
 * and may be shared by many concurrent attempts.
 * <p>
 * Each attempt asks for a {@link Permit} before dispatching,
 * and reports its outcome through that permit.
 */
@NullMarked
@Stability.Internal
public interface AttemptCircuitBreaker {

  /**
   * Returns a circuit breaker that follows the given config,
   * or one that never trips if the config says the circuit breaker is disabled.
   *
   * @param name what the breaker protects, for log messages. For example, "query service on node1:8093".
   */
  static AttemptCircuitBreaker from(CircuitBreakerConfig config, String name) {
    return config.enabled()
      ? new ConcurrentCircuitBreaker(config, name)
      : NoopAttemptCircuitBreaker.INSTANCE;
  }

  /**
   * Asks permission to dispatch an attempt.
   * <p>
   * If permission is granted, the caller must eventually call either
   * {@link Permit#complete(boolean)} or {@link Permit#release()} on the returned permit.
   *
   * @return a permit, or null if the circuit is open and the attempt must not be dispatched.
   */
  @Nullable Permit tryAcquire();

  State state();

  /**
   * Permission to dispatch one attempt. Report the outcome exactly once, by calling
   * either {@link #complete(boolean)} or {@link #release()}. Later calls are ignored.
   */
  interface Permit {
    /**
     * Records the outcome of the dispatched attempt.
     */
    void complete(boolean success);

    /**
     * Gives up the permit without recording an outcome; for example, because the
     * attempt was cancelled for reasons that say nothing about the server's health.
     */
    void release();

    /**
     * Returns false if the outcome is ignored, in which case the caller
     * need not bother working out whether the attempt succeeded.
     */
    default boolean tracksOutcome() {
      return true;
    }
  }
}
