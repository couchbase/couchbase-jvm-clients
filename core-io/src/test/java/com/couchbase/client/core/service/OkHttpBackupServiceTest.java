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
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.TestHttpServer.RecordedRequest;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Sends requests through {@link OkHttpBackupService} to a real HTTP server.
 */
class OkHttpBackupServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static CoreEnvironment env;

  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private OkHttpBackupService service;

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

    service = new OkHttpBackupService(core.context(), new HostAndPort(TestHttpServer.HOST_NAME, server.port()));
  }

  @AfterEach
  void teardown() {
    try {
      okHttpClient.close();
    } finally {
      server.close();
    }
  }

  private CoreHttpRequest newRequest() {
    return CoreHttpRequest.builder(CoreCommonOptions.DEFAULT, core.context(), HttpMethod.GET,
        CoreHttpPath.path("/api/v1/plan/{name}", singletonMap("name", "my-plan")), RequestTarget.backup())
      .build();
  }

  @Test
  void roundTrip() throws Exception {
    server.enqueue("{\"name\":\"my-plan\"}");

    CoreHttpRequest request = newRequest();
    service.send(request);
    CoreHttpResponse response = request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);

    RecordedRequest sent = server.requests().get(0);
    assertEquals("GET", sent.method);
    assertEquals("/api/v1/plan/my-plan", sent.path);
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");

    assertEquals(ResponseStatus.SUCCESS, response.status());
    assertEquals("{\"name\":\"my-plan\"}", new String(response.content(), UTF_8));
  }

  @Test
  void errorIsReportedAsHttpStatusCodeException() {
    // Like the Netty implementation: no service-specific translation.
    server.enqueue(404, "{\"status\":404,\"msg\":\"plan not found\"}", Duration.ZERO);

    CoreHttpRequest request = newRequest();
    service.send(request);
    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));

    HttpStatusCodeException cause = assertInstanceOf(HttpStatusCodeException.class, e.getCause());
    assertEquals(404, cause.httpStatusCode());
    assertEquals("{\"status\":404,\"msg\":\"plan not found\"}", cause.content());
  }

  @Test
  void disconnectReportsDisconnectedAndCompletesStates() {
    List<ServiceState> states = new CopyOnWriteArrayList<>();
    AtomicBoolean completed = new AtomicBoolean();
    service.states().subscribe(states::add, e -> {}, () -> completed.set(true));
    assertEquals(ServiceState.CONNECTED, service.state());

    service.disconnect();
    service.disconnect(); // no effect the second time

    assertEquals(ServiceState.DISCONNECTED, service.state());
    assertEquals(asList(ServiceState.CONNECTED, ServiceState.DISCONNECTED), states);
    assertTrue(completed.get(), "states() should complete, so subscribers know the service is gone");
  }

  @Test
  void disconnectLetsRequestsInFlightFinish() throws Exception {
    server.enqueue("{\"name\":\"my-plan\"}", Duration.ofMillis(500));

    CoreHttpRequest request = newRequest();
    service.send(request);
    service.disconnect(); // while the server is still "thinking"

    CoreHttpResponse response = request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);
    assertEquals(ResponseStatus.SUCCESS, response.status());
  }

  @Test
  void rejectsOtherRequestTypes() {
    QueryRequest request = new QueryRequest(TIMEOUT, core.context(), BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(), "SELECT 1", "{}".getBytes(UTF_8), true, null, null, null, null, null, false);
    assertThrows(IllegalArgumentException.class, () -> service.send(request));
  }
}
