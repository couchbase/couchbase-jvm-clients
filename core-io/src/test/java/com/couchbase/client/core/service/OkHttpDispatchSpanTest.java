/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.service;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.metrics.AbstractMeter;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.cnc.tracing.ObservabilitySemanticConvention.DATABASE_DUP;
import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * The dispatch span of a request sent by an OkHttp-based service.
 */
class OkHttpDispatchSpanTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final HostAndPort CANONICAL_NODE = new HostAndPort("canonical.example.com", 8091);

  private final RecordingTracer tracer = new RecordingTracer();

  private CoreEnvironment env;
  private CouchbaseOkHttpClient okHttpClient;
  private TestHttpServer server;
  private Core core;

  @BeforeEach
  void setup() throws IOException {
    env = CoreEnvironment.create();
    okHttpClient = newTestOkHttpClient(env);
    server = TestHttpServer.startHttp();

    core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);

    // Both conventions, to check the attributes each one sets.
    CoreResources resources = new CoreResources(
      tracer,
      new TracingDecorator(listOf(DATABASE_DUP)),
      new AbstractMeter(NoopMeter.INSTANCE, new ArrayList<>())
    );
    when(core.coreResources()).thenReturn(resources);
    when(core.context().coreResources()).thenReturn(resources);
  }

  @AfterEach
  void teardown() {
    try {
      okHttpClient.close();
    } finally {
      try {
        server.close();
      } finally {
        env.shutdown();
      }
    }
  }

  private CoreHttpRequest send(int port) {
    CoreHttpRequest request = CoreHttpRequest.builder(
        CoreCommonOptions.of(TIMEOUT, FailFastRetryStrategy.INSTANCE, null),
        core.context(),
        HttpMethod.GET,
        CoreHttpPath.path("/admin/ping"),
        RequestTarget.query()
      )
      .trace("test")
      .build();

    // Done by the node, in real life.
    request.context().lastDispatchedToNode(new NodeIdentifier(CANONICAL_NODE, TestHttpServer.HOST_NAME));

    new OkHttpQueryService(
      QueryServiceConfig.maxEndpoints(4).build(),
      core.context(),
      new HostAndPort(TestHttpServer.HOST_NAME, port)
    ).send(request);

    return request;
  }

  @Test
  void dispatchSpanHasTheExpectedAttributes() throws Exception {
    server.enqueue("ok");
    CoreHttpRequest request = send(server.port());
    request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    RecordingSpan span = only(tracer.dispatchSpans());
    assertTrue(span.ended);

    // The connection the request was sent on, as the request context reports it.
    RequestContext ctx = request.context();
    String channelId = ctx.lastChannelId();
    HostAndPort local = ctx.lastDispatchedFrom();
    assertNotNull(channelId);
    assertNotNull(local);

    // Older convention
    assertEquals(channelId, span.attributes.get("db.couchbase.local_id"));
    assertEquals(local.host(), span.attributes.get("net.host.name"));
    assertEquals((long) local.port(), span.attributes.get("net.host.port"));
    assertEquals(CANONICAL_NODE.host(), span.attributes.get("net.peer.name"));
    assertEquals((long) server.port(), span.attributes.get("net.peer.port")); // the service's port, not the canonical port

    // Newer convention
    assertEquals(channelId, span.attributes.get("couchbase.local_id"));
    assertEquals(CANONICAL_NODE.host(), span.attributes.get("server.address"));
    assertEquals((long) server.port(), span.attributes.get("server.port"));
    assertEquals(TestHttpServer.HOST_NAME, span.attributes.get("network.peer.address"));
    assertEquals((long) server.port(), span.attributes.get("network.peer.port"));
  }

  @Test
  void requestThatCantConnectHasNoDispatchSpan() throws Exception {
    CoreHttpRequest request = send(TestHttpServer.unusedPort());
    assertThrows(ExecutionException.class, () -> request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

    assertEquals(listOf(), tracer.dispatchSpans());
  }

  private static <T> T only(List<T> list) {
    assertEquals(1, list.size(), "expected exactly one, but got: " + list);
    return list.get(0);
  }

  private static final class RecordingTracer implements RequestTracer {
    final List<RecordingSpan> spans = new CopyOnWriteArrayList<>();

    List<RecordingSpan> dispatchSpans() {
      return spans.stream()
        .filter(it -> it.name.equals(TracingIdentifiers.SPAN_DISPATCH))
        .collect(toList());
    }

    @Override
    public RequestSpan requestSpan(String name, RequestSpan parent) {
      RecordingSpan span = new RecordingSpan(name);
      spans.add(span);
      return span;
    }

    @Override
    public Mono<Void> start() {
      return Mono.empty();
    }

    @Override
    public Mono<Void> stop(Duration timeout) {
      return Mono.empty();
    }
  }

  private static final class RecordingSpan implements RequestSpan {
    final String name;
    final Map<String, Object> attributes = new ConcurrentHashMap<>();
    volatile boolean ended;

    RecordingSpan(String name) {
      this.name = name;
    }

    @Override
    public void attribute(String key, String value) {
      attributes.put(key, value);
    }

    @Override
    public void attribute(String key, boolean value) {
      attributes.put(key, value);
    }

    @Override
    public void attribute(String key, long value) {
      attributes.put(key, value);
    }

    @Override
    public void event(String name, Instant timestamp) {
    }

    @Override
    public void status(StatusCode status) {
    }

    @Override
    public void end() {
      ended = true;
    }

    @Override
    public void requestContext(RequestContext requestContext) {
    }

    @Override
    public String toString() {
      return name + attributes;
    }
  }
}
