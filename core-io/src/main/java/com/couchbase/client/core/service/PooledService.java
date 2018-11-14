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

import com.couchbase.client.core.cnc.events.service.ServiceConnectInitiated;
import com.couchbase.client.core.cnc.events.service.ServiceDisconnectInitiated;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link PooledService} is a flexible implementation to pool endpoints based on the
 * given configuration.
 *
 * <p>This implementation is closely related to the older PooledService part of the 1.x series,
 * but has been adapted to the slightly new semantics of the endpoints and their behaviors. The pool
 * now has more authority on the lifetime of the endpoint since it also has more knowledge of
 * the related ones.</p>
 *
 * @since 2.0.0
 */
abstract class PooledService implements Service {

  /**
   * Holds the config for this service.
   */
  private final ServiceConfig serviceConfig;

  /**
   * Holds all currently tracked endpoints in this pool.
   */
  private final List<Endpoint> endpoints;

  /**
   * Holds the initial state for this pooled service.
   */
  private final ServiceState initialState;

  /**
   * The context for this service.
   */
  private final ServiceContext serviceContext;

  /**
   * If disconnect called by a caller, set to true.
   */
  private volatile AtomicBoolean disconnected;

  PooledService(final ServiceConfig serviceConfig, ServiceContext serviceContext) {
    this.serviceConfig = serviceConfig;
    this.endpoints = new CopyOnWriteArrayList<>();
    this.initialState = serviceConfig.minEndpoints() > 0
      ? ServiceState.DISCONNECTED
      : ServiceState.IDLE;
    this.disconnected = new AtomicBoolean(false);
    this.serviceContext = serviceContext;
  }

  /**
   * Subclass implements this method to create new endpoints.
   *
   * @return the created endpoint.
   */
  protected abstract Endpoint createEndpoint();

  /**
   * Subclass implements this method to pick their selection strategy of choice.
   *
   * @return the selection strategy.
   */
  protected abstract EndpointSelectionStrategy selectionStrategy();

  @Override
  public <R extends Request<? extends Response>> void send(final R request) {
    if (request.completed()) {
      return;
    }

    Endpoint selected = selectionStrategy().select(request, endpoints);

    if (selected != null) {
      selected.send(request);
    } else {
      // todo: what to do if endpoint not found?
    }

    // TODO: implement actual dynamic pooling and handling of pipelining!!
  }

  @Override
  public synchronized void connect() {
    if (state() == ServiceState.DISCONNECTED && !disconnected.get()) {
      serviceContext.environment().eventBus().publish(new ServiceConnectInitiated(
        serviceContext,
        serviceConfig.minEndpoints()
      ));

      for (int i = 0; i < serviceConfig.minEndpoints(); i++) {
        Endpoint endpoint = createEndpoint();
        endpoint.connect();
        endpoints.add(endpoint);
      }
    }
  }

  @Override
  public synchronized void disconnect() {
    if (disconnected.compareAndSet(false, true)) {
      serviceContext.environment().eventBus().publish(new ServiceDisconnectInitiated(
        serviceContext,
        endpoints.size()
      ));

      for (Endpoint endpoint : endpoints) {
        endpoint.disconnect();
      }
      endpoints.clear();
    }
  }

  @Override
  public ServiceState state() {
    if (endpoints.isEmpty()) {
      return initialState;
    }

    ServiceState state = ServiceState.DISCONNECTED;
    int connected = 0;
    int connecting = 0;
    int disconnecting = 0;
    for (Endpoint endpoint : endpoints) {
        switch (endpoint.state()) {
          case CONNECTED:
            connected++;
            break;
          case CONNECTING:
            connecting++;
            break;
          case DISCONNECTING:
            disconnecting++;
            break;
          default:
            // ignore
        }
    }

    if (endpoints.size() == connected) {
      state = ServiceState.CONNECTED;
    } else if (connected > 0) {
      state = ServiceState.DEGRADED;
    } else if (connecting > 0) {
      state = ServiceState.CONNECTING;
    } else if (disconnecting > 0) {
      state = ServiceState.DISCONNECTING;
    }

    return state;
  }

}
