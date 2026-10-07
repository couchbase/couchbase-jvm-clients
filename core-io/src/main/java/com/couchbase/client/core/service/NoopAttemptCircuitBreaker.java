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

import com.couchbase.client.core.endpoint.CircuitBreaker.State;
import org.jspecify.annotations.NullMarked;

/**
 * A circuit breaker that always allows attempts, and ignores their outcomes.
 * Used when the circuit breaker is disabled.
 */
@NullMarked
final class NoopAttemptCircuitBreaker implements AttemptCircuitBreaker {
  static final NoopAttemptCircuitBreaker INSTANCE = new NoopAttemptCircuitBreaker();

  private static final Permit PERMIT = new Permit() {
    @Override
    public void complete(boolean success) {
    }

    @Override
    public void release() {
    }

    @Override
    public boolean tracksOutcome() {
      return false;
    }

    @Override
    public String toString() {
      return "NoopPermit";
    }
  };

  private NoopAttemptCircuitBreaker() {
  }

  @Override
  public Permit tryAcquire() {
    return PERMIT;
  }

  @Override
  public State state() {
    return State.DISABLED;
  }

  @Override
  public String toString() {
    return "NoopAttemptCircuitBreaker";
  }
}
