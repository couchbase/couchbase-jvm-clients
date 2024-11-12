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

package com.couchbase.client.core.service;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.Stateful;

import java.util.stream.Stream;

/**
 * The parent interface for all service implementations.
 *
 * <p>Note that while this interface has been around since the 1.x days, it has been changed
 * up quite a bit to make it simpler and provide more functionality based on real world experience
 * with the first iteration.</p>
 *
 * @since 1.0.0
 */
public interface Service extends Stateful<ServiceState> {

  /**
   * Instruct this {@link Service} to connect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the service, signaling potential successful connection
   * attempts.</p>
   */
  void connect();

  /**
   * Instruct this {@link Service} to disconnect.
   *
   * <p>This method is async and will return immediately. Use the other methods available to
   * inspect the current state of the service, signaling potential successful disconnection
   * attempts.</p>
   */
  void disconnect();

  /**
   * Sends the request into this {@link Service}.
   *
   * <p>Note that there is no guarantee that the request will actually dispatched, based on the
   * state this service is in.</p>
   *
   * @param request the request to send.
   */
  <R extends Request<? extends Response>> void send(R request);

  /**
   * Returns the underlying contextual metadata for this service.
   */
  ServiceContext context();

  /**
   * Represents the service type for this service.
   */
  ServiceType type();

  /**
   * Returns diagnostics information for this service.
   */
  Stream<EndpointDiagnostics> diagnostics();

  /**
   * Returns the remote address for this service.
   */
  default HostAndPort address() {
    return context().remote();
  }

  @Stability.Internal
  Stream<InternalEndpointDiagnostics> internalDiagnostics();
}
