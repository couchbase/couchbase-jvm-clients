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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.events.service.ServiceConnectInitiatedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceDisconnectInitiatedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceStateChangedEvent;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.cnc.SimpleEventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import reactor.core.publisher.DirectProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link PooledService}.
 *
 * @since 2.0.0
 */
class PooledServiceTest {

  private CoreEnvironment environment;
  private SimpleEventBus eventBus;
  private ServiceContext serviceContext;
  private final Authenticator authenticator = mock(Authenticator.class);

  @BeforeEach
  void beforeEach() {
    eventBus = new SimpleEventBus(true, Collections.singletonList(ServiceStateChangedEvent.class));
    environment = CoreEnvironment.builder().eventBus(eventBus).build();
    CoreContext coreContext = new CoreContext(mock(Core.class), 1, environment, authenticator);
    serviceContext = new ServiceContext(coreContext, "127.0.0.1", 1234,
      ServiceType.KV, Optional.empty());
  }

  @AfterEach
  void afterEach() {
    environment.shutdown();
  }

  /**
   * If we have a pool with 0 minimum endpoints, make sure it is idle after
   * creation.
   */
  @Test
  void startsIdleWithNoMinEndpoints() {
    MockedService service = new MockedService(new MockedServiceConfig(0, 1), () -> null);
    assertEquals(ServiceState.IDLE, service.state());
  }

  /**
   * If there are minimum number of endpoints, it should be in a disconnected
   * state upfront.
   */
  @Test
  void startsDisconnectedWithMinEndpoints() {
    MockedService service = new MockedService(new MockedServiceConfig(1, 1), () -> null);
    assertEquals(ServiceState.DISCONNECTED, service.state());
  }

  /**
   * Even after a connect call, if there are no min endpoints there is nothing
   * to do and the pool should be idle.
   */
  @Test
  void stillIdleAfterConnectWithNoMinEndpoints() {
    MockedService service = new MockedService(new MockedServiceConfig(0, 1), () -> null);
    service.connect();
    assertEquals(ServiceState.IDLE, service.state());
  }

  /**
   * After a connect call, the minimum number of endpoints should be created
   * and the overall state of the service should track their state.
   */
  @Test
  void tracksMinEndpointStateAfterConnect() {
    int minEndpoints = 2;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTING);
    when(mock1.states()).thenReturn(DirectProcessor.create());
    Endpoint mock2 = mock(Endpoint.class);
    when(mock2.state()).thenReturn(EndpointState.CONNECTING);
    when(mock2.states()).thenReturn(DirectProcessor.create());

    final List<Endpoint> mocks = Arrays.asList(mock1, mock2);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 10),
      () -> mocks.get(invocation.getAndIncrement())
    );
    service.connect();

    assertEquals(minEndpoints, service.trackedEndpoints().size());
    assertEquals(ServiceState.CONNECTING, service.state());
    verify(mock1, times(1)).connect();
    verify(mock2, times(1)).connect();

    ServiceConnectInitiatedEvent event = (ServiceConnectInitiatedEvent) eventBus.publishedEvents().get(0);
    assertEquals(2, event.minimumEndpoints());
    assertEquals(
      "Starting to connect service with 2 minimum endpoints",
      event.description()
    );
  }

  /**
   * Make sure that when disconnect is called, all current endpoints are disconnected
   * and the service is put into a disconnected state.
   */
  @Test
  void disconnectsAllEndpoints() {
    int minEndpoints = 2;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    when(mock1.states()).thenReturn(DirectProcessor.create());
    Endpoint mock2 = mock(Endpoint.class);
    when(mock2.state()).thenReturn(EndpointState.CONNECTED);
    when(mock2.states()).thenReturn(DirectProcessor.create());

    final List<Endpoint> mocks = Arrays.asList(mock1, mock2);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2),
      () -> mocks.get(invocation.getAndIncrement())
    );
    service.connect();

    assertEquals(minEndpoints, service.trackedEndpoints().size());
    assertEquals(ServiceState.CONNECTED, service.state());

    service.disconnect();
    assertEquals(ServiceState.DISCONNECTED, service.state());
    verify(mock1, times(1)).disconnect();
    verify(mock2, times(1)).disconnect();

    assertEquals(2, eventBus.publishedEvents().size());
    ServiceDisconnectInitiatedEvent event = (ServiceDisconnectInitiatedEvent) eventBus.publishedEvents().get(1);
    assertEquals(2, event.disconnectingEndpoints());
    assertEquals(
      "Starting to disconnect service with 2 underlying endpoints",
      event.description()
    );
  }

  /**
   * Once disconnected, make sure that another connect attempt is plainly
   * ignored.
   */
  @Test
  void ignoresConnectAfterDisconnect() {
    int minEndpoints = 2;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.states()).thenReturn(DirectProcessor.create());
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    Endpoint mock2 = mock(Endpoint.class);
    when(mock2.state()).thenReturn(EndpointState.CONNECTED);
    when(mock2.states()).thenReturn(DirectProcessor.create());

    final List<Endpoint> mocks = Arrays.asList(mock1, mock2);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2),
      () -> mocks.get(invocation.getAndIncrement())
    );
    service.connect();
    service.disconnect();
    service.connect();

    assertEquals(minEndpoints, service.trackedEndpoints().size());
    assertEquals(2, eventBus.publishedEvents().size());
  }

  /**
   * Most basic test to check if we can dispatch into the endpoint.
   */
  @Test
  @SuppressWarnings({ "unchecked" })
  void sendsRequestIntoEndpoint() {
    int minEndpoints = 2;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    when(mock1.states()).thenReturn(DirectProcessor.create());
    when(mock1.outstandingRequests()).thenReturn(0L);
    Endpoint mock2 = mock(Endpoint.class);
    when(mock2.state()).thenReturn(EndpointState.CONNECTED);
    when(mock2.states()).thenReturn(DirectProcessor.create());

    final List<Endpoint> mocks = Arrays.asList(mock1, mock2);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    Request<? extends Response> request = mock(Request.class);
    service.send(request);

    verify(mock1, times(1)).send(request);
    verify(mock2, never()).send(request);
  }

  /**
   * If a request is completed already, do not attempt to send it in the first place.
   */
  @Test
  @SuppressWarnings({ "unchecked" })
  void doNotSendRequestIfCompleted() {
    int minEndpoints = 2;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    when(mock1.states()).thenReturn(DirectProcessor.create());

    Endpoint mock2 = mock(Endpoint.class);
    when(mock2.state()).thenReturn(EndpointState.CONNECTED);
    when(mock2.states()).thenReturn(DirectProcessor.create());

    final List<Endpoint> mocks = Arrays.asList(mock1, mock2);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    Request<? extends Response> request = mock(Request.class);
    when(request.completed()).thenReturn(true);
    service.send(request);

    verify(mock1, never()).send(request);
    verify(mock2, never()).send(request);
  }

  @Test
  void dispatchesDirectlyIfSlotAvailable() {
    int minEndpoints = 0;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);

    DirectProcessor<EndpointState> states = DirectProcessor.create();
    when(mock1.states()).thenReturn(states);
    when(mock1.outstandingRequests()).thenReturn(0L);

    final List<Endpoint> mocks = Collections.singletonList(mock1);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2, Duration.ofMillis(500), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request);

    // Simulate the connecting and connected
    states.onNext(EndpointState.CONNECTING);
    states.onNext(EndpointState.CONNECTED);

    waitUntilCondition(() -> !service.trackedEndpoints.isEmpty());
    assertEquals(1, service.trackedEndpoints().size());
    assertEquals(0, request.context().retryAttempts());

    waitUntilCondition(() -> {
      Collection<Invocation> invocations = Mockito.mockingDetails(mock1).getInvocations();
      for (Invocation inv : invocations) {
        if (inv.getMethod().getName().equals("send")) {
          if (inv.getArgument(0) == request) {
            return true;
          }
        }
      }
      return false;
    });

    verify(mock1, times(1)).send(request);
  }

  @Test
  void retriesIfNoSlotAvailable() {
    int minEndpoints = 0;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    when(mock1.outstandingRequests()).thenReturn(1L);

    DirectProcessor<EndpointState> states = DirectProcessor.create();
    when(mock1.states()).thenReturn(states);


    final List<Endpoint> mocks = Collections.singletonList(mock1);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 1, Duration.ofMillis(500), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request1 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    NoopRequest request2 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request1);
    service.send(request2);
    assertEquals(1, service.trackedEndpoints().size());

    // The first request is sent into the free slot without retrying
    assertEquals(0, request1.context().retryAttempts());
    // No more slots available, this one goes into retry
    assertTrue(request2.context().retryAttempts() > 0);

    // Simulate the connecting and connected
    states.onNext(EndpointState.CONNECTING);
    states.onNext(EndpointState.CONNECTED);

    waitUntilCondition(() -> {
      Collection<Invocation> invocations = Mockito.mockingDetails(mock1).getInvocations();
      for (Invocation inv : invocations) {
        if (inv.getMethod().getName().equals("send")) {
          if (inv.getArgument(0) == request1) {
            return true;
          }
        }
      }
      return false;
    });

    verify(mock1, never()).send(request2);
  }

  @Test
  void retriesIfFixedSize() {
    int minEndpoints = 1;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    when(mock1.states()).thenReturn(DirectProcessor.create());
    when(mock1.outstandingRequests()).thenReturn(1L);

    final List<Endpoint> mocks = Collections.singletonList(mock1);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 1, Duration.ofMillis(500), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertFalse(service.trackedEndpoints().isEmpty());

    NoopRequest request = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request);
    assertEquals(1, service.trackedEndpoints().size());
    assertTrue(request.context().retryAttempts() > 0);
    verify(mock1, never()).send(request);
  }

  @Test
  void cleansIdleConnections() throws Exception {
    int minEndpoints = 0;
    long now = System.nanoTime();

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    DirectProcessor<EndpointState> states = DirectProcessor.create();
    when(mock1.states()).thenReturn(states);
    when(mock1.outstandingRequests()).thenReturn(1L);
    when(mock1.lastResponseReceived()).thenReturn(now);

    Endpoint mock2 = mock(Endpoint.class);
    when(mock2.state()).thenReturn(EndpointState.CONNECTED);
    when(mock2.states()).thenReturn(DirectProcessor.create());
    when(mock2.outstandingRequests()).thenReturn(1L);
    when(mock2.lastResponseReceived()).thenReturn(now);

    final List<Endpoint> mocks = Arrays.asList(mock1, mock2);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2, Duration.ofMillis(500), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request1 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request1);
    NoopRequest request2 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request2);

    waitUntilCondition(() -> service.trackedEndpoints.size() == 2);

    // Simulate the connecting and connected
    states.onNext(EndpointState.CONNECTING);
    states.onNext(EndpointState.CONNECTED);

    when(mock1.outstandingRequests()).thenReturn(0L);

    Thread.sleep(600);

    verify(mock1, times(1)).disconnect();
    verify(mock2, never()).disconnect();
  }

  /**
   * Double check that terminated / disconnected connections (by the sdk) are also double checked and
   * cleaned up to prevent leaking.
   */
  @Test
  void cleansDisconnectedEndpoints() {
    int minEndpoints = 0;
    long now = System.nanoTime();

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    DirectProcessor<EndpointState> states1 = DirectProcessor.create();
    when(mock1.states()).thenReturn(states1);
    when(mock1.outstandingRequests()).thenReturn(1L);
    doReturn(now).when(mock1).lastResponseReceived(); // trying different format due to a CI error with mockito

    Endpoint mock2 = mock(Endpoint.class);
    when(mock2.state()).thenReturn(EndpointState.CONNECTED);
    DirectProcessor<EndpointState> states2 = DirectProcessor.create();
    when(mock2.states()).thenReturn(states2);
    when(mock2.outstandingRequests()).thenReturn(1L);
    doReturn(now).when(mock2).lastResponseReceived(); // trying different format due to a CI error with mockito

    final List<Endpoint> mocks = Arrays.asList(mock1, mock2);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2, Duration.ofMillis(500), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request1 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request1);

    states1.onNext(EndpointState.CONNECTING);
    states1.onNext(EndpointState.CONNECTED);

    NoopRequest request2 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request2);

    states2.onNext(EndpointState.CONNECTING);
    states2.onNext(EndpointState.CONNECTED);

    waitUntilCondition(() -> service.trackedEndpoints.size() == 2);

    when(mock1.receivedDisconnectSignal()).thenReturn(true);
    when(mock2.receivedDisconnectSignal()).thenReturn(true);

    waitUntilCondition(() -> service.state() == ServiceState.IDLE);
  }

  /**
   * This test makes sure that when a socket has just been opened and no operation has gone through yet,
   * it is not cleaned up by the idle cleaner.
   * <p>
   * This is a regression test for JVMCBC-856.
   */
  @Test
  void doesNotCleanJustOpenedConnections() throws Exception {
    long now = System.nanoTime();

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    when(mock1.states()).thenReturn(DirectProcessor.create());
    when(mock1.outstandingRequests()).thenReturn(0L);
    when(mock1.lastResponseReceived()).thenReturn(0L);
    when(mock1.lastConnectedAt()).thenReturn(now);

    final List<Endpoint> mocks = Collections.singletonList(mock1);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(0, 2, Duration.ofMillis(5000), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request1 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request1);

    waitUntilCondition(() -> service.trackedEndpoints.size() == 1);

    Thread.sleep(300);

    verify(mock1, never()).disconnect();
  }

  @Test
  void cleansUpNeverUsedIdleConnections() {
    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);
    DirectProcessor<EndpointState> states = DirectProcessor.create();
    when(mock1.states()).thenReturn(states);
    when(mock1.outstandingRequests()).thenReturn(0L);
    when(mock1.lastResponseReceived()).thenReturn(0L);
    when(mock1.lastConnectedAt()).thenReturn(0L);

    final List<Endpoint> mocks = Collections.singletonList(mock1);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(0, 2, Duration.ofMillis(1000), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request1 = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request1);

    // Simulate the connecting and connected
    states.onNext(EndpointState.CONNECTING);
    states.onNext(EndpointState.CONNECTED);

    when(mock1.lastConnectedAt()).thenReturn(System.nanoTime());
    waitUntilCondition(() -> service.trackedEndpoints.size() == 1);

    when(mock1.receivedDisconnectSignal()).thenReturn(true);
    waitUntilCondition(() -> service.state() == ServiceState.IDLE);
  }

  /**
   * With direct dispatch in the pool it is possible that endpoint for which the socket is
   * waiting for to be dispatched never goes into a connected state but rather into disconnected
   * (and then subsequent reconnect). As soon as we observe a disconnect state we need to retry the
   * op so that it has a chance to complete somewhere else.
   */
  @Test
  void retriesRequestIfEndpointCannotConnect() {
    int minEndpoints = 0;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.DISCONNECTED);

    DirectProcessor<EndpointState> states = DirectProcessor.create();
    when(mock1.states()).thenReturn(states);
    when(mock1.outstandingRequests()).thenReturn(0L);

    final List<Endpoint> mocks = Collections.singletonList(mock1);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2, Duration.ofMillis(500), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request);

    // Simulate the connecting and connected
    states.onNext(EndpointState.CONNECTING);
    states.onNext(EndpointState.DISCONNECTED);

    waitUntilCondition(() -> !service.trackedEndpoints.isEmpty());
    assertEquals(1, service.trackedEndpoints().size());
    assertTrue(request.context().retryAttempts() >= 1);
    verify(mock1, never()).send(request);
  }

  /**
   * It can happen that while the reserved endpoint connects,
   * the overall pool got the disconnect signal in the meantime.
   * <p>
   * If this happens, make sure we clean up everything properly.
   */
  @Test
  void cleansUpReservedEndpointIfDisconnected() {
    int minEndpoints = 0;

    Endpoint mock1 = mock(Endpoint.class);
    when(mock1.state()).thenReturn(EndpointState.CONNECTED);

    DirectProcessor<EndpointState> states = DirectProcessor.create();
    when(mock1.states()).thenReturn(states);
    when(mock1.outstandingRequests()).thenReturn(0L);

    final List<Endpoint> mocks = Collections.singletonList(mock1);
    final AtomicInteger invocation = new AtomicInteger();
    MockedService service = new MockedService(
      new MockedServiceConfig(minEndpoints, 2, Duration.ofMillis(500), false),
      () -> mocks.get(invocation.getAndIncrement()),
      new FirstEndpointSelectionStrategy()
    );
    service.connect();

    assertTrue(service.trackedEndpoints().isEmpty());

    NoopRequest request = new NoopRequest(
      Duration.ofSeconds(1),
      serviceContext,
      BestEffortRetryStrategy.INSTANCE,
      CollectionIdentifier.fromDefault("bucket")
    );
    service.send(request);

    service.disconnect();

    // Simulate the connecting and connected
    states.onNext(EndpointState.CONNECTING);
    states.onNext(EndpointState.DISCONNECTED);

    waitUntilCondition(() -> request.context().retryAttempts() >= 1);
    verify(mock1, never()).send(request);
    verify(mock1, atLeastOnce()).disconnect();
  }

  class MockedService extends PooledService {

    List<Endpoint> trackedEndpoints = new ArrayList<>();

    private final Supplier<Endpoint> endpointSupplier;
    private final EndpointSelectionStrategy endpointSelectionStrategy;

    MockedService(final ServiceConfig serviceConfig, Supplier<Endpoint> endpointSupplier) {
      this(serviceConfig, endpointSupplier, new NoneEndpointSelectionStrategy());
    }

    MockedService(final ServiceConfig serviceConfig, Supplier<Endpoint> endpointSupplier,
                  EndpointSelectionStrategy endpointSelectionStrategy) {
      super(serviceConfig, serviceContext);
      this.endpointSupplier = endpointSupplier;
      this.endpointSelectionStrategy = endpointSelectionStrategy;
    }

    @Override
    protected Endpoint createEndpoint() {
      Endpoint endpoint = endpointSupplier.get();
      trackedEndpoints.add(endpoint);
      return endpoint;
    }

    @Override
    protected EndpointSelectionStrategy selectionStrategy() {
      return endpointSelectionStrategy;
    }

    List<Endpoint> trackedEndpoints() {
      return trackedEndpoints;
    }

    @Override
    public ServiceType type() {
      return ServiceType.KV;
    }

    @Override
    protected Duration idleTimeCheckInterval() {
      return Duration.ofMillis(10);
    }
  }

  static class MockedServiceConfig implements ServiceConfig {

    private final int min;
    private final int max;
    private final Duration idle;
    private final boolean pipelined;

    MockedServiceConfig(int min, int max) {
      this(min, max, Duration.ofSeconds(30), false);
    }

    MockedServiceConfig(int min, int max, Duration idle, boolean pipelined) {
      this.max = max;
      this.min = min;
      this.idle = idle;
      this.pipelined = pipelined;
    }

    @Override
    public int minEndpoints() {
      return min;
    }

    @Override
    public int maxEndpoints() {
      return max;
    }

    @Override
    public Duration idleTime() {
      return idle;
    }

    @Override
    public boolean pipelined() {
      return pipelined;
    }

  }

  static class FirstEndpointSelectionStrategy implements EndpointSelectionStrategy {
    @Override
    public <R extends Request<? extends Response>> Endpoint select(R r, List<Endpoint> endpoints) {
      for (Endpoint ep : endpoints) {
        if (ep.outstandingRequests() == 0) {
          return ep;
        }
      }
      return null;
    }
  }

  static class NoneEndpointSelectionStrategy implements EndpointSelectionStrategy {
    @Override
    public <R extends Request<? extends Response>> Endpoint select(R r, List<Endpoint> endpoints) {
      return null;
    }
  }

}