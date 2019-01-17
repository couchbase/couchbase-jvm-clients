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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;

import java.util.Map;

public class EndpointContext extends CoreContext {

  /**
   * The hostname of this endpoint.
   */
  private final NetworkAddress remoteHostname;

  /**
   * The port of this endpoint.
   */
  private final int remotePort;

  /**
   * The circuit breaker used for this endpoint.
   */
  private final CircuitBreaker circuitBreaker;

  /**
   * The service type of this endpoint.
   */
  private final ServiceType serviceType;

  /**
   * Creates a new {@link EndpointContext}.
   *
   * @param ctx the parent context to use.
   * @param remoteHostname the remote hostname.
   * @param remotePort the remote port.
   */
  public EndpointContext(CoreContext ctx, NetworkAddress remoteHostname, int remotePort,
                         CircuitBreaker circuitBreaker, ServiceType serviceType) {
    super(ctx.core(), ctx.id(), ctx.environment());
    this.remoteHostname = remoteHostname;
    this.remotePort = remotePort;
    this.circuitBreaker = circuitBreaker;
    this.serviceType = serviceType;
  }

  @Override
  protected void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("remote", remoteHostname().nameOrAddress() + ":" + remotePort());
    input.put("circuitBreaker", circuitBreaker.state().toString());
    input.put("type", serviceType);
  }

  public NetworkAddress remoteHostname() {
    return remoteHostname;
  }

  public int remotePort() {
    return remotePort;
  }

  public CircuitBreaker circuitBreaker() {
    return circuitBreaker;
  }

  public ServiceType serviceType() {
    return serviceType;
  }
}
