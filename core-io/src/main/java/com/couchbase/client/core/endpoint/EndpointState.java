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
 * Represents all states an {@link Endpoint} can be in.
 *
 * @since 2.0.0
 */
public enum EndpointState {

  /**
   * The endpoint is disconnected and not trying to connect.
   */
  DISCONNECTED,

  /**
   * The endpoint is disconnected but trying to connect right now.
   */
  CONNECTING,

  /**
   * The endpoint is connected and the circuit breaker is closed.
   *
   * <p>This is the only state where it can actually try to serve traffic in the
   * expected way.</p>
   */
  CONNECTED_CIRCUIT_CLOSED,

  /**
   * The endpoint is connected, and the circuit is in a half-open state.
   *
   * <p>Between the circuit being closed and open, half-open allows it to "try" certain
   * requests but reject others. If the canary succeeds it might go back to open, if
   * not it might go back to closed.</p>
   */
  CONNECTED_CIRCUIT_HALF_OPEN,

  /**
   * The endpoint is connected, but the circuit breaker has found the need to open
   * the circuit and as a result no traffic can be served.
   */
  CONNECTED_CIRCUIT_OPEN,

  /**
   * The endpoint is currently disconnecting.
   */
  DISCONNECTING

}
