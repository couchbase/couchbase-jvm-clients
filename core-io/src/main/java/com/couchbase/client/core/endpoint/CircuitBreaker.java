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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.msg.Response;

import java.util.concurrent.CompletableFuture;

/**
 * The {@link CircuitBreaker} interface defines the external integration points between an
 * implementation and its calling {@link BaseEndpoint}.
 *
 * <p>See the individual implementation for details on how they work. Note that they are all
 * configured through the {@link CircuitBreakerConfig} that is configured on the environment on
 * a per service basis.</p>
 *
 * @since 2.0.0
 */
interface CircuitBreaker {

  /**
   * Tracks the given response for either success or failure.
   *
   * @param response the response to track.
   */
  void track(CompletableFuture<? extends Response> response);

  /**
   * Resets this circuit breaker to its initial state.
   */
  void reset();

  /**
   * Returns true if requests are allowed to go through and be tracked.
   */
  boolean allowsRequest();

  /**
   * Returns the current state of the circuit breaker.
   */
  State state();

  /**
   * Represents all the states a circuit breaker can be in, possibly.
   */
  enum State {
    /**
     * The circuit breaker is disabled.
     */
    DISABLED,
    /**
     * The circuit breaker is tracking and closed.
     */
    CLOSED,
    /**
     * The circuit breaker is half-open (likely a canary is in-flight).
     */
    HALF_OPEN,
    /**
     * The circuit breaker is open because it has tripped.
     */
    OPEN
  }
}