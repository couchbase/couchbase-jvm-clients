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

/**
 * A simple noop implementation of the {@link CircuitBreaker} if disabled by the user.
 *
 * @since 2.0.0
 */
public class NoopCircuitBreaker implements CircuitBreaker {

  public static final NoopCircuitBreaker INSTANCE = new NoopCircuitBreaker();

  private NoopCircuitBreaker() {}

  @Override
  public void track() { }

  @Override
  public void markSuccess() { }

  @Override
  public void markFailure() { }

  @Override
  public void reset() { }

  @Override
  public boolean allowsRequest() {
    return true;
  }

  @Override
  public State state() {
    return State.DISABLED;
  }

}
