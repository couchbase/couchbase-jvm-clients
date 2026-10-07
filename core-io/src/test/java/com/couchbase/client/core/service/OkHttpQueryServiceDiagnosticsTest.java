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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Checks the dispatch details {@link OkHttpQueryService} reports for HTTP requests:
 * the values a ping report is built from ({@link CoreHttpResponse#channelId()} and
 * the request context's {@code lastDispatchedFrom} / {@code lastDispatchedTo}),
 * and that they agree with the service's endpoint diagnostics.
 */
class OkHttpQueryServiceDiagnosticsTest {

  private static CoreEnvironment env;

  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private OkHttpQueryService service;

  @BeforeAll
  static void beforeAll() {
    env = CoreEnvironment.create();
  }

  @AfterAll
  static void afterAll() {
    env.shutdown();
  }

  @BeforeEach
  void setup() throws IOException {
    server = TestHttpServer.startHttp();

    okHttpClient = newTestOkHttpClient(env);

    core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);

    service = new OkHttpQueryService(
      QueryServiceConfig.maxEndpoints(4).build(),
      core.context(),
      new HostAndPort(TestHttpServer.HOST_NAME, server.port()),
      Duration.ofSeconds(15)
    );
  }

  @AfterEach
  void teardown() {
    try {
      okHttpClient.close();
    } finally {
      server.close();
    }
  }

  private CoreHttpRequest newPingRequest() {
    return CoreHttpRequest.builder(
      CoreCommonOptions.of(Duration.ofSeconds(5), BestEffortRetryStrategy.INSTANCE, null),
      core.context(),
      HttpMethod.GET,
      CoreHttpPath.path("/admin/ping"),
      RequestTarget.query()
    ).build();
  }

  private CoreHttpResponse send(CoreHttpRequest request) throws Exception {
    service.send(request);
    return request.response().get(10, TimeUnit.SECONDS);
  }

  private List<EndpointDiagnostics> connected() {
    return service.diagnostics()
      .filter(d -> d.state() == EndpointState.CONNECTED)
      .collect(Collectors.toList());
  }

  @Test
  void pingReportDetailsMatchDiagnostics() throws Exception {
    server.enqueue("OK");
    CoreHttpRequest request = newPingRequest();
    CoreHttpResponse response = send(request);

    assertEquals(ResponseStatus.SUCCESS, response.status());

    List<EndpointDiagnostics> connected = connected();
    assertEquals(1, connected.size(), "expected one connection, but got: " + connected);
    EndpointDiagnostics endpoint = connected.get(0);
    String endpointId = endpoint.id().orElseThrow(AssertionError::new);

    // The ping report's ID is "0x" + response.channelId(), like the Netty implementation.
    assertEquals(endpointId, "0x" + response.channelId());
    assertEquals(endpointId, request.context().lastChannelId());

    // The ping report's "local" comes from lastDispatchedFrom.
    HostAndPort local = request.context().lastDispatchedFrom();
    assertNotNull(local, "lastDispatchedFrom should be set");
    assertTrue(endpoint.local().endsWith(":" + local.port()),
      "diagnostics local " + endpoint.local() + " should match dispatched-from " + local);

    // The ping report's "remote" comes from lastDispatchedTo: the node's configured address, as with Netty.
    assertEquals(new HostAndPort(TestHttpServer.HOST_NAME, server.port()), request.context().lastDispatchedTo());
  }

  @Test
  void refusedConnectionLeavesDispatchDetailsUnset() throws Exception {
    OkHttpQueryService deadNodeService = new OkHttpQueryService(
      QueryServiceConfig.maxEndpoints(4).build(),
      core.context(),
      new HostAndPort(TestHttpServer.HOST_NAME, TestHttpServer.unusedPort()),
      Duration.ofSeconds(15)
    );

    // Fail fast, so the request completes instead of being retried.
    CoreHttpRequest request = CoreHttpRequest.builder(
      CoreCommonOptions.of(Duration.ofSeconds(5), FailFastRetryStrategy.INSTANCE, null),
      core.context(),
      HttpMethod.GET,
      CoreHttpPath.path("/admin/ping"),
      RequestTarget.query()
    ).build();

    deadNodeService.send(request);
    ExecutionException e = assertThrows(ExecutionException.class, () -> request.response().get(10, TimeUnit.SECONDS));
    assertInstanceOf(RequestCanceledException.class, e.getCause());

    // As with Netty, a request that never got a connection was never dispatched.
    // (Error contexts fall back to lastDispatchedToNode, so the target node is still reported.)
    assertNull(request.context().lastDispatchedTo());
    assertNull(request.context().lastDispatchedFrom());
    assertNull(request.context().lastChannelId());
  }

  @Test
  void requestsOnSameConnectionReportSameId() throws Exception {
    server.enqueue("one");
    server.enqueue("two");

    CoreHttpResponse first = send(newPingRequest());
    CoreHttpResponse second = send(newPingRequest());

    assertEquals(1, connected().size());
    assertEquals(first.channelId(), second.channelId());
  }

  @Test
  void requestsOnDifferentConnectionsReportDifferentIds() throws Exception {
    // Slow responses, so the two requests are in flight at the same time and need two connections.
    server.enqueue("one", Duration.ofMillis(500));
    server.enqueue("two", Duration.ofMillis(500));

    CoreHttpRequest first = newPingRequest();
    CoreHttpRequest second = newPingRequest();
    service.send(first);
    service.send(second);

    CoreHttpResponse firstResponse = first.response().get(10, TimeUnit.SECONDS);
    CoreHttpResponse secondResponse = second.response().get(10, TimeUnit.SECONDS);

    assertNotEquals(firstResponse.channelId(), secondResponse.channelId());
  }

  @Test
  void responseWithUnknownChannelId() {
    CoreHttpResponse response = new CoreHttpResponse(
      ResponseStatus.SUCCESS,
      new byte[0],
      200,
      (String) null,
      mock(RequestContext.class)
    );
    assertEquals("unknown", response.channelId());
  }
}
