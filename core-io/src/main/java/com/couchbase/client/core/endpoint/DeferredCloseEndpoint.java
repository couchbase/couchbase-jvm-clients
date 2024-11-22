/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectDelayedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectResumedEvent;
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;

/**
 * Allows in-flight requests to finish before disconnecting.
 */
abstract class DeferredCloseEndpoint extends BaseEndpoint {
  /**
   * Close chanel when outstanding requests are done?
   */
  private volatile boolean closeWhenDone = false;

  DeferredCloseEndpoint(
    String hostname,
    int port,
    EventLoopGroup eventLoopGroup,
    ServiceContext serviceContext,
    CircuitBreakerConfig circuitBreakerConfig,
    ServiceType serviceType,
    boolean pipelined
  ) {
    super(hostname, port, eventLoopGroup, serviceContext, circuitBreakerConfig, serviceType, pipelined);
  }

  @Override
  public synchronized void disconnect() {
    if (this.outstandingRequests() > 0) {
      closeWhenDone();
    } else {
      super.disconnect();
    }
  }

  private void closeWhenDone() {
    closeWhenDone = true;
    endpointContext.get().environment().eventBus().publish(new EndpointDisconnectDelayedEvent(endpointContext.get()));
  }

  @Stability.Internal
  @Override
  public synchronized void markRequestCompletion() {
    maybeResumeDisconnect();
    super.markRequestCompletion();
  }

  @Stability.Internal
  @Override
  public synchronized void notifyChannelInactive() {
    maybeResumeDisconnect();
    super.notifyChannelInactive();
  }

  private void maybeResumeDisconnect() {
    if (closeWhenDone) {
      endpointContext.get().environment().eventBus().publish(new EndpointDisconnectResumedEvent(endpointContext.get()));
      super.disconnect();
      closeWhenDone = false;
    }
  }
}
