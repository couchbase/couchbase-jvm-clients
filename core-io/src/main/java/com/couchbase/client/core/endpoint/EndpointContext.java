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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;

import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

public class EndpointContext extends CoreContext {

  /**
   * The hostname of this endpoint.
   */
  private final HostAndPort remoteSocket;

  private final Optional<HostAndPort> localSocket;

  /**
   * The circuit breaker used for this endpoint.
   */
  private final CircuitBreaker circuitBreaker;

  /**
   * The service type of this endpoint.
   */
  private final ServiceType serviceType;

  private final Optional<String> bucket;

  private final Optional<String> channelId;

  private volatile AuthenticationStatus authenticationStatus = AuthenticationStatus.UNKNOWN;

  /**
   * Helper method to duplicate the endpoint context (useful for extension).
   *
   * @param ctx the context to copy from.
   */
  public EndpointContext(final EndpointContext ctx) {
    this(ctx, ctx.remoteSocket, ctx.circuitBreaker, ctx.serviceType, ctx.localSocket, ctx.bucket, ctx.channelId);
  }

  /**
   * Creates a new {@link EndpointContext}.
   */
  public EndpointContext(CoreContext ctx, HostAndPort remoteSocket,
                         CircuitBreaker circuitBreaker, ServiceType serviceType,
                         Optional<HostAndPort> localSocket, Optional<String> bucket, Optional<String> channelId) {
    super(ctx.core(), ctx.id(), ctx.environment(), ctx.authenticator());
    this.remoteSocket = remoteSocket;
    this.circuitBreaker = circuitBreaker;
    this.serviceType = serviceType;
    this.bucket = bucket;
    this.localSocket = localSocket;
    this.channelId = channelId;
  }

  @Stability.Internal
  public void authenticationStatus(AuthenticationStatus authenticationStatus) {
    this.authenticationStatus = requireNonNull(authenticationStatus);
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("remote", redactSystem(remoteSocket()));
    localSocket.ifPresent(s -> input.put("local", redactSystem(s)));
    if (circuitBreaker != null) {
      input.put("circuitBreaker", circuitBreaker.state().toString());
    }
    input.put("type", serviceType);
    bucket.ifPresent(b -> input.put("bucket", redactMeta(b)));
    channelId.ifPresent(i -> input.put("channelId", i));
  }

  public CircuitBreaker circuitBreaker() {
    return circuitBreaker;
  }

  public Optional<HostAndPort> localSocket() {
    return localSocket;
  }

  public HostAndPort remoteSocket() {
    return remoteSocket;
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  public Optional<String> bucket() {
    return bucket;
  }

  public Optional<String> channelId() {
    return channelId;
  }

  @Stability.Internal
  public AuthenticationStatus authenticationStatus() {
    return authenticationStatus;
  }
}
