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

import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.util.Stateful;

/**
 * The parent interface for all endpoints.
 *
 * <p>Note that while this interface has been around since the 1.x days, it has been changed
 * up quite a bit to make it simpler and provide more functionality based on real world experience
 * with the first iteration.</p>
 *
 * @since 1.0.0
 */
public interface Endpoint extends Stateful<EndpointState> {

  /**
   * Instruct this {@link Endpoint} to connect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the endpoint, signaling potential successful connection
   * attempts.</p>
   */
  void connect();

  /**
   * Instruct this {@link Endpoint} to disconnect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the endpoint, signaling potential successful disconnection
   * attempts.</p>
   */
  void disconnect();

  /**
   * Sends the request into this {@link Endpoint}.
   *
   * <p>Note that there is no guarantee that the request will actually dispatched, based on the
   * state this endpoint is in.</p>
   *
   * @param request the request to send.
   */
  <R extends Request<? extends Response>> void send(R request);

  /**
   * If new requests can be written to this endpoint
   *
   * @return true if free, false otherwise.
   */
  boolean freeToWrite();

  /**
   * If this endpoint has one or more outstanding requests.
   *
   * @return the number of outstanding requests
   */
  long outstandingRequests();

  /**
   * Holds the timestamp of the last response received (or 0 if no request ever sent).
   *
   * @return the timestamp of the last response received, in nanoseconds.
   */
  long lastResponseReceived();

  /**
   * Returns the timestamp when the endpoint was last connected successfully (nanoseconds).
   *
   * @return the timestamp when the endpoint was last connected, in nanoseconds.
   */
  long lastConnectedAt();

  /**
   * Returns diagnostics information for this endpoint.
   */
  EndpointDiagnostics diagnostics();

  /**
   * On this endpoint {@link #disconnect()} has been called on.
   * <p>
   * This is different from an endpoint just being disconnected on the remote side and continuing reconnect
   * attempts. Once this returns true, it is never coming back.
   *
   * @return true if {@link #disconnect()} has been called.
   */
  boolean receivedDisconnectSignal();

  /**
   * Returns the context for this endpoint.
   *
   * @return the context.
   */
  EndpointContext context();

}
