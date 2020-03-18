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

package com.couchbase.client.core.node;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.node.NodeConnectedEvent;
import com.couchbase.client.core.cnc.events.node.NodeDisconnectIgnoredEvent;
import com.couchbase.client.core.cnc.events.node.NodeDisconnectedEvent;
import com.couchbase.client.core.cnc.events.node.NodeStateChangedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceAddIgnoredEvent;
import com.couchbase.client.core.cnc.events.service.ServiceAddedEvent;
import com.couchbase.client.core.cnc.events.service.ServiceRemoveIgnoredEvent;
import com.couchbase.client.core.cnc.events.service.ServiceRemovedEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.service.Service;
import com.couchbase.client.core.service.ServiceState;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.cnc.SimpleEventBus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.DirectProcessor;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link Node}.
 *
 * @since 2.0.0
 */
class NodeTest {

  private static CoreEnvironment ENV;
  private static CoreContext CTX;

  private static final Optional<String> NO_ALTERNATE = Optional.empty();

  @BeforeAll
  static void beforeAll() {
    Core core = mock(Core.class);
    ENV = CoreEnvironment
      .builder()
      .build();
    CTX = new CoreContext(core, 1, ENV, mock(Authenticator.class));
  }

  @AfterAll
  static void afterAll() {
    ENV.shutdown();
  }

  @Test
  void disconnectedOnInit() {
    Node node = Node.create(
      CTX,
      mock(NodeIdentifier.class),
      NO_ALTERNATE
    );
    assertEquals(NodeState.DISCONNECTED, node.state());
  }

  @Test
  void idleIfAllIdle() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(ServiceState.IDLE);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));

    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.IDLE, node.state());
  }

  @Test
  void canAddAndRemoveServices() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        when(s.type()).thenReturn(serviceType);
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.QUERY));
    assertEquals(NodeState.CONNECTED, node.state());
    node.removeService(ServiceType.QUERY, Optional.empty()).block();
    assertFalse(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.DISCONNECTED, node.state());
  }

  @Test
  void connectedIfOneConnected() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    node.addService(ServiceType.KV, 1234, Optional.of("bucket")).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));

    assertEquals(NodeState.CONNECTED, node.state());
  }

  @Test
  void connectedIfAllConnected() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));

    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.CONNECTED, node.state());
  }

  @Test
  void connectedIfSomeIdleAndRestConnected() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      final AtomicInteger counter = new AtomicInteger();
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(counter.incrementAndGet() % 2 == 0
          ? ServiceState.IDLE
          : ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));

    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.QUERY, 8093, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.QUERY));

    assertFalse(node.serviceEnabled(ServiceType.VIEWS));
    node.addService(ServiceType.VIEWS, 8092, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.VIEWS));

    assertEquals(NodeState.CONNECTED, node.state());
  }

  @Test
  void degradedIfAtLeastOneConnected() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      final AtomicInteger counter = new AtomicInteger();
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(counter.incrementAndGet() > 1
          ? ServiceState.CONNECTED
          : ServiceState.DISCONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.QUERY, 8093, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.DEGRADED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.VIEWS));
    node.addService(ServiceType.VIEWS, 8092, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.VIEWS));

    assertEquals(NodeState.DEGRADED, node.state());
  }

  @Test
  void connectingIfAllConnecting() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(ServiceState.CONNECTING);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));

    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.CONNECTING, node.state());
  }

  @Test
  void disconnectingIfAllDisconnecting() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(ServiceState.DISCONNECTING);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));

    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.DISCONNECTING, node.state());
  }

  @Test
  void performsDisconnect() {
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        Service s = mock(Service.class);
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        when(s.type()).thenReturn(serviceType);
        return s;
      }
    };

    assertEquals(NodeState.DISCONNECTED, node.state());

    assertFalse(node.serviceEnabled(ServiceType.KV));
    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();
    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();
    assertTrue(node.serviceEnabled(ServiceType.KV));
    assertTrue(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.CONNECTED, node.state());

    node.disconnect().block();

    assertFalse(node.serviceEnabled(ServiceType.KV));
    assertFalse(node.serviceEnabled(ServiceType.QUERY));

    assertEquals(NodeState.DISCONNECTED, node.state());

    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();
    assertFalse(node.serviceEnabled(ServiceType.QUERY));
    assertEquals(NodeState.DISCONNECTED, node.state());
  }

  @Test
  @SuppressWarnings({"unchecked"})
  void sendsToFoundLocalService() {
    final Service s = mock(Service.class);
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        when(s.type()).thenReturn(serviceType);
        return s;
      }
    };

    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();

    KeyValueRequest r = mock(KeyValueRequest.class);
    when(r.serviceType()).thenReturn(ServiceType.KV);
    when(r.bucket()).thenReturn("bucket");
    when(r.context()).thenReturn(new RequestContext(CTX, r));
    node.send(r);

    verify(s, times(1)).send(eq(r));
  }

  @Test
  void sendsToFoundGlobalService() {
    final Service s = mock(Service.class);
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.type()).thenReturn(serviceType);
        when(s.states()).thenReturn(DirectProcessor.create());
        return s;
      }
    };

    node.addService(ServiceType.QUERY, 8091, Optional.empty()).block();

    QueryRequest r = mock(QueryRequest.class);
    when(r.serviceType()).thenReturn(ServiceType.QUERY);
    when(r.context()).thenReturn(new RequestContext(CTX, r));
    node.send(r);

    verify(s, times(1)).send(eq(r));
  }

  @Test
  @SuppressWarnings({"unchecked"})
  void retriesIfLocalServiceNotFound() {
    final Service s = mock(Service.class);
    final AtomicReference<Request<?>> retried = new AtomicReference<>();
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        when(s.type()).thenReturn(serviceType);
        return s;
      }

      @Override
      protected <R extends Request<? extends Response>> void sendIntoRetry(R request) {
        retried.set(request);
      }
    };

    node.addService(ServiceType.KV, 11210, Optional.of("bucket")).block();

    KeyValueRequest r = mock(KeyValueRequest.class);
    when(r.serviceType()).thenReturn(ServiceType.KV);
    when(r.bucket()).thenReturn("other_bucket");
    node.send(r);

    verify(s, never()).send(eq(r));
    assertEquals(r, retried.get());
  }

  @Test
  void retriesIfGlobalServiceNotFound() {
    final Service s = mock(Service.class);
    final AtomicReference<Request<?>> retried = new AtomicReference<>();
    Node node = new Node(CTX, mock(NodeIdentifier.class), NO_ALTERNATE) {
      @Override
      protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
        when(s.state()).thenReturn(ServiceState.CONNECTED);
        when(s.states()).thenReturn(DirectProcessor.create());
        when(s.type()).thenReturn(serviceType);
        return s;
      }

      @Override
      protected <R extends Request<? extends Response>> void sendIntoRetry(R request) {
        retried.set(request);
      }
    };

    QueryRequest r = mock(QueryRequest.class);
    when(r.serviceType()).thenReturn(ServiceType.QUERY);
    node.send(r);

    verify(s, never()).send(eq(r));
    assertEquals(r, retried.get());
  }

  @Test
  void sendsEventsIntoEventBus() {
    Core core = mock(Core.class);
    SimpleEventBus eventBus = new SimpleEventBus(true, Collections.singletonList(NodeStateChangedEvent.class));
    CoreEnvironment env = CoreEnvironment
      .builder()
      .eventBus(eventBus)
      .build();
    CoreContext ctx = new CoreContext(core, 1, env, mock(Authenticator.class));

    try {
      Node node = new Node(ctx, mock(NodeIdentifier.class), NO_ALTERNATE) {
        @Override
        protected Service createService(ServiceType serviceType, int port, Optional<String> bucket) {
          Service s = mock(Service.class);
          when(s.type()).thenReturn(serviceType);
          when(s.states()).thenReturn(DirectProcessor.create());
          when(s.state()).thenReturn(ServiceState.IDLE);
          return s;
        }
      };

      node.addService(ServiceType.QUERY, 2, Optional.empty()).block();
      node.addService(ServiceType.KV, 1, Optional.of("bucket")).block();
      node.addService(ServiceType.KV, 1, Optional.of("bucket")).block();

      node.removeService(ServiceType.KV, Optional.of("bucket")).block();
      node.removeService(ServiceType.QUERY, Optional.empty()).block();
      node.removeService(ServiceType.QUERY, Optional.empty()).block();

      node.disconnect().block();
      node.disconnect().block();

      node.addService(ServiceType.QUERY, 2, Optional.empty()).block();
      node.removeService(ServiceType.QUERY, Optional.empty()).block();

      List<Event> events = eventBus.publishedEvents();

      assertTrue(events.remove(0) instanceof NodeConnectedEvent);

      assertTrue(events.get(0) instanceof ServiceAddedEvent);
      assertTrue(events.get(1) instanceof ServiceAddedEvent);
      assertTrue(events.get(2) instanceof ServiceAddIgnoredEvent);

      assertTrue(events.get(3) instanceof ServiceRemovedEvent);
      assertTrue(events.get(4) instanceof ServiceRemovedEvent);
      assertTrue(events.get(5) instanceof ServiceRemoveIgnoredEvent);

      assertTrue(events.get(6) instanceof NodeDisconnectedEvent);
      assertTrue(events.get(7) instanceof NodeDisconnectIgnoredEvent);

      assertTrue(events.get(8) instanceof ServiceAddIgnoredEvent);
      assertTrue(events.get(9) instanceof ServiceRemoveIgnoredEvent);
    } finally {
      env.shutdown();
    }
  }

}