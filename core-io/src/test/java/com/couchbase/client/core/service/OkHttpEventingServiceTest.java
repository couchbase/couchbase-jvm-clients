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
import com.couchbase.client.core.error.EventingFunctionNotFoundException;
import com.couchbase.client.core.error.context.EventingErrorContext;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Sends requests through {@link OkHttpEventingService} to a real HTTP server.
 */
class OkHttpEventingServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static CoreEnvironment env;

  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private OkHttpEventingService service;

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

    service = new OkHttpEventingService(core.context(), new HostAndPort(TestHttpServer.HOST_NAME, server.port()));
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
        CoreHttpPath.path("/api/v1/functions/{name}", singletonMap("name", "my-function")), RequestTarget.eventing())
      .build();
  }

  @Test
  void roundTrip() throws Exception {
    server.enqueue("{\"appname\":\"my-function\"}");

    CoreHttpRequest request = newRequest();
    service.send(request);
    CoreHttpResponse response = request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);

    RecordedRequest sent = server.requests().get(0);
    assertEquals("GET", sent.method);
    assertEquals("/api/v1/functions/my-function", sent.path);
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");

    assertEquals(ResponseStatus.SUCCESS, response.status());
    assertEquals("{\"appname\":\"my-function\"}", new String(response.content(), UTF_8));
  }

  @Test
  void errorIsTranslated() {
    server.enqueue(404, "{\"name\":\"ERR_APP_NOT_FOUND_TS\",\"code\":24,\"description\":\"Function not found\"}", Duration.ZERO);

    CoreHttpRequest request = newRequest();
    service.send(request);
    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));

    EventingFunctionNotFoundException cause = assertInstanceOf(EventingFunctionNotFoundException.class, e.getCause());
    EventingErrorContext context = (EventingErrorContext) cause.context();
    assertEquals(404, context.httpStatus());
  }

  @Test
  void rejectsOtherRequestTypes() {
    QueryRequest request = new QueryRequest(TIMEOUT, core.context(), BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(), "SELECT 1", "{}".getBytes(UTF_8), true, null, null, null, null, null, false);
    assertThrows(IllegalArgumentException.class, () -> service.send(request));
  }
}
