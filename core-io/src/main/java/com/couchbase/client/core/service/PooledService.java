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
import com.couchbase.client.core.retry.RetryOrchestrator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
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
   * If the pool cannot grow because min and max are the same.
   */
  private final boolean fixedPool;

  /**
   * If disconnect called by a caller, set to true.
   */
  private volatile AtomicBoolean disconnected;

  /**
   * Creates a new {@link PooledService}.
   *
   * @param serviceConfig the underlying service config.
   * @param serviceContext the service context.
   */
  PooledService(final ServiceConfig serviceConfig, final ServiceContext serviceContext) {
    this.serviceConfig = serviceConfig;
    this.endpoints = new CopyOnWriteArrayList<>();
    this.initialState = serviceConfig.minEndpoints() > 0
      ? ServiceState.DISCONNECTED
      : ServiceState.IDLE;
    this.disconnected = new AtomicBoolean(false);
    this.serviceContext = serviceContext;
    this.fixedPool = serviceConfig.minEndpoints() == serviceConfig.maxEndpoints();

    scheduleCleanIdleConnections();
  }

  /**
   * Helper method to schedule cleaning up idle connections per interval.
   */
  private void scheduleCleanIdleConnections() {
    final Duration idleTime = serviceConfig.idleTime();
    if (idleTime != null && !idleTime.isZero()) {
      serviceContext.environment().timer().schedule(this::cleanIdleConnections, idleTime);
    }
  }

  /**
   * Go through the connections and clean up all the idle connections.
   */
  private synchronized void cleanIdleConnections() {
    if (disconnected.get()) {
      return;
    }

    List<Endpoint> endpoints = new ArrayList<>(this.endpoints);
    Collections.shuffle(endpoints);

    for (Endpoint endpoint : endpoints) {
      if (this.endpoints.size() == serviceConfig.minEndpoints()) {
        break;
      }

      long timespan = TimeUnit.NANOSECONDS.toSeconds(
        System.nanoTime() - endpoint.lastResponseReceived()
      );

      if (endpoint.free() && timespan >= serviceConfig.idleTime().toNanos()) {
        this.endpoints.remove(endpoint);
        endpoint.disconnect();
      }
    }

    scheduleCleanIdleConnections();
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

    Endpoint found = endpoints.isEmpty() ? null : selectionStrategy().select(request, endpoints);

    if (found != null) {
      found.send(request);
      return;
    }

    if (!fixedPool && endpoints.size() < serviceConfig.maxEndpoints()) {
      synchronized (this) {
        if (!disconnected.get()) {
          Endpoint endpoint = createEndpoint();
          endpoint.connect();
          endpoints.add(endpoint);
        }
      }
    }

    RetryOrchestrator.maybeRetry(serviceContext, request);
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
