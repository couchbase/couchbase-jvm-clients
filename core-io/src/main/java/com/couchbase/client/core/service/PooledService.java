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

import com.couchbase.client.core.cnc.events.service.IdleEndpointRemovedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceConnectInitiatedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceDisconnectInitiatedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceStateChangedEvent;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.CompositeStateful;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

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
   * The interval when to check if idle sockets are to be cleaned up.
   */
  public static final Duration DEFAULT_IDLE_TIME_CHECK_INTERVAL = Duration.ofMillis(100);

  /**
   * Holds the config for this service.
   */
  private final ServiceConfig serviceConfig;

  /**
   * Holds all currently tracked endpoints in this pool.
   */
  private final List<Endpoint> endpoints;

  /**
   * Holds the endpoint states and as a result the internal service state.
   */
  private final CompositeStateful<Endpoint, EndpointState, ServiceState> endpointStates;

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
  private final AtomicBoolean disconnected;

  /**
   * Creates a new {@link PooledService}.
   *
   * @param serviceConfig the underlying service config.
   * @param serviceContext the service context.
   */
  PooledService(final ServiceConfig serviceConfig, final ServiceContext serviceContext) {
    this.serviceConfig = serviceConfig;
    this.endpoints = new CopyOnWriteArrayList<>();

    final ServiceState initialState = serviceConfig.minEndpoints() > 0
      ? ServiceState.DISCONNECTED
      : ServiceState.IDLE;

    this.endpointStates = CompositeStateful.create(initialState, endpointStates -> {
      if (endpointStates.isEmpty()) {
        return initialState;
      }

      ServiceState state = ServiceState.DISCONNECTED;
      int connected = 0;
      int connecting = 0;
      int disconnecting = 0;
      for (EndpointState endpointState : endpointStates) {
        switch (endpointState) {
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

      if (endpointStates.size() == connected) {
        state = ServiceState.CONNECTED;
      } else if (connected > 0) {
        state = ServiceState.DEGRADED;
      } else if (connecting > 0) {
        state = ServiceState.CONNECTING;
      } else if (disconnecting > 0) {
        state = ServiceState.DISCONNECTING;
      }

      return state;
    }, (from, to) ->
      serviceContext.environment().eventBus().publish(new ServiceStateChangedEvent(serviceContext, from, to))
    );

    this.disconnected = new AtomicBoolean(false);
    this.serviceContext = serviceContext;
    this.fixedPool = serviceConfig.minEndpoints() == serviceConfig.maxEndpoints();

    scheduleCleanIdleConnections();
  }

  /**
   * Returns the created {@link ServiceContext} for implementations to use.
   */
  protected ServiceContext serviceContext() {
    return serviceContext;
  }

  /**
   * Helper method to schedule cleaning up idle connections per interval.
   */
  private void scheduleCleanIdleConnections() {
    final Duration idleTime = serviceConfig.idleTime();
    if (idleTime != null && !idleTime.isZero()) {
      serviceContext.environment().timer().schedule(this::cleanIdleConnections, idleTimeCheckInterval());
    }
  }

  /**
   * Can be overridden for unit tests.
   */
  protected Duration idleTimeCheckInterval() {
    return DEFAULT_IDLE_TIME_CHECK_INTERVAL;
  }

  /**
   * Go through the connections and clean up all the idle connections.
   */
  private synchronized void cleanIdleConnections() {
    if (disconnected.get()) {
      return;
    }

    final List<Endpoint> endpoints = new ArrayList<>(this.endpoints);
    Collections.shuffle(endpoints);

    for (Endpoint endpoint : endpoints) {
      if (this.endpoints.size() == serviceConfig.minEndpoints()) {
        break;
      }

      long lastResponseReceived = endpoint.lastResponseReceived();
      long actualIdleTime;
      if (lastResponseReceived != 0) {
        actualIdleTime = System.nanoTime() - endpoint.lastResponseReceived();
      } else {
        // If we did not receive a last response timestamp, it could be the case that a socket is
        // connected but no request has been sent into it yet. If this is the case, take the timestamp
        // when the socket got last connected as a reference point to determine if it is idle.
        long lastConnected = endpoint.lastConnectedAt();
        if (lastConnected != 0) {
          actualIdleTime = System.nanoTime() - lastConnected;
        } else {
          // No last connected timestamp, so the endpoint isn't even fully connected yet
          continue;
        }
      }

      // we also check if an endpoint received a hard disconnect signal and is still lingering around
      boolean receivedDisconnect = endpoint.receivedDisconnectSignal();
      boolean idleTooLong = endpoint.outstandingRequests() == 0 && actualIdleTime >= serviceConfig.idleTime().toNanos();
      if (receivedDisconnect || idleTooLong) {
        this.endpoints.remove(endpoint);
        endpointStates.deregister(endpoint);
        if (!receivedDisconnect) {
          endpoint.disconnect();
        }
        publishIdleEndpointRemovedEvent(endpoint, actualIdleTime);
      }
    }

    scheduleCleanIdleConnections();
  }

  /**
   * Helper method to publish an event with enriched context when an idle endpoint has been removed.
   *
   * @param endpoint the endpoint that got removed.
   * @param actualIdleTime the actual idle time of that endpoint.
   */
  private void publishIdleEndpointRemovedEvent(final Endpoint endpoint, final long actualIdleTime) {
    if (endpoint.context() != null) {
      final EndpointContext enrichedContext = new EndpointContext(endpoint.context()) {
        @Override
        public void injectExportableParams(final Map<String, Object> input) {
          super.injectExportableParams(input);

          Map<String, Object> serviceInfo = new HashMap<>();
          input.put("actualIdleTimeMillis", TimeUnit.NANOSECONDS.toMillis(actualIdleTime));
          serviceInfo.put("remainingEndpoints", endpoints.size());
          serviceInfo.put("state", state());
          input.put("service", serviceInfo);
        }
      };
      serviceContext.environment().eventBus().publish(new IdleEndpointRemovedEvent(enrichedContext));
    }
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
          endpointStates.register(endpoint, endpoint);
          endpoint.connect();
          endpoints.add(endpoint);
        }
      }
      RetryOrchestrator.maybeRetry(serviceContext, request, RetryReason.ENDPOINT_TEMPORARILY_NOT_AVAILABLE);
    } else {
      RetryOrchestrator.maybeRetry(serviceContext, request, RetryReason.ENDPOINT_NOT_AVAILABLE);
    }
  }

  @Override
  public synchronized void connect() {
    if (state() == ServiceState.DISCONNECTED && !disconnected.get()) {
      serviceContext.environment().eventBus().publish(new ServiceConnectInitiatedEvent(
        serviceContext,
        serviceConfig.minEndpoints()
      ));

      for (int i = 0; i < serviceConfig.minEndpoints(); i++) {
        Endpoint endpoint = createEndpoint();
        endpointStates.register(endpoint, endpoint);
        endpoint.connect();
        endpoints.add(endpoint);
      }
    }
  }

  @Override
  public synchronized void disconnect() {
    if (disconnected.compareAndSet(false, true)) {
      serviceContext.environment().eventBus().publish(new ServiceDisconnectInitiatedEvent(
        serviceContext,
        endpoints.size()
      ));

      for (Endpoint endpoint : endpoints) {
        endpoint.disconnect();
        endpointStates.deregister(endpoint);
      }
      endpoints.clear();
    }
  }

  @Override
  public ServiceContext context() {
    return serviceContext;
  }

  @Override
  public ServiceState state() {
    return endpointStates.state();
  }

  @Override
  public Flux<ServiceState> states() {
    return endpointStates.states();
  }

  @Override
  public Stream<EndpointDiagnostics> diagnostics() {
    return endpoints.stream().map(Endpoint::diagnostics);
  }

}
