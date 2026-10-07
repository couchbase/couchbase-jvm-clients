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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CoreErrorCodeAndMessageException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.TestHttpServer.RecordedRequest;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Sends analytics requests through {@link OkHttpAnalyticsService} to a real HTTP server,
 * and checks the request it sends and how it handles the response.
 */
class OkHttpAnalyticsServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String STATEMENT_JSON = "{\"statement\":\"SELECT 1\"}";

  private static CoreEnvironment env;

  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private OkHttpAnalyticsService service;

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

    service = new OkHttpAnalyticsService(
      AnalyticsServiceConfig.maxEndpoints(4).build(),
      core.context(),
      new HostAndPort(TestHttpServer.HOST_NAME, server.port()),
      Duration.ofSeconds(15)
    );

    // Retried requests come back through the core; send them to the same service.
    doAnswer(invocation -> {
      service.send((Request<?>) invocation.getArgument(0));
      return null;
    }).when(core).send(any(), anyBoolean());
  }

  @AfterEach
  void teardown() {
    try {
      okHttpClient.close();
    } finally {
      server.close();
    }
  }

  private AnalyticsRequest newAnalyticsRequest(int priority) {
    return new AnalyticsRequest(
      TIMEOUT,
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      STATEMENT_JSON.getBytes(UTF_8),
      priority,
      true,
      "my-context-id",
      "SELECT 1",
      null,
      null,
      null
    );
  }

  private AnalyticsRequest newAnalyticsRequest() {
    return newAnalyticsRequest(AnalyticsRequest.NO_PRIORITY);
  }

  private AnalyticsResponse send(AnalyticsRequest request) throws Exception {
    service.send(request);
    return request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);
  }

  private Throwable sendExpectingFailure(AnalyticsRequest request) {
    service.send(request);
    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    return e.getCause();
  }

  private static List<String> rows(AnalyticsResponse response) {
    return response.rows()
      .map(AnalyticsChunkRow::data)
      .map(it -> new String(it, UTF_8))
      .collectList()
      .block(Duration.ofSeconds(30));
  }

  @Test
  void roundTrip() throws Exception {
    server.enqueue("{" +
      "\"requestID\":\"abc-123\"," +
      "\"clientContextID\":\"my-context-id\"," +
      "\"signature\":{\"*\":\"*\"}," +
      "\"results\":[{\"a\":1},{\"a\":2}]," +
      "\"plans\":{}," +
      "\"status\":\"success\"," +
      "\"metrics\":{\"resultCount\":2}" +
      "}");

    AnalyticsResponse response = send(newAnalyticsRequest(-1));

    // What the service sent.
    RecordedRequest sent = server.requests().get(0);
    assertEquals("POST", sent.method);
    assertEquals("/analytics/service", sent.path);
    assertEquals(STATEMENT_JSON, sent.bodyAsString());
    assertTrue(sent.header("Content-Type").startsWith("application/json"), sent.header("Content-Type"));
    assertEquals("-1", sent.header("Analytics-Priority"));
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");
    assertEquals("test-user-agent", sent.header("User-Agent"));

    // What the service made of the response.
    assertEquals("abc-123", response.header().requestId());
    assertEquals("my-context-id", response.header().clientContextId().orElse(null));
    assertTrue(response.header().signature().isPresent());
    assertEquals(asList("{\"a\":1}", "{\"a\":2}"), rows(response));

    AnalyticsChunkTrailer trailer = response.trailer().block(Duration.ofSeconds(30));
    assertNotNull(trailer);
    assertEquals("success", trailer.status());
    assertEquals("{\"resultCount\":2}", new String(trailer.metrics(), UTF_8));
    assertTrue(trailer.plans().isPresent());
    assertFalse(trailer.errors().isPresent());
  }

  @Test
  void noPriorityHeaderByDefault() throws Exception {
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[],\"status\":\"success\"}");

    send(newAnalyticsRequest());
    assertNull(server.requests().get(0).header("Analytics-Priority"));
  }

  @Test
  void customPathAndMethodWithoutBody() throws Exception {
    // Like the JDBC driver.
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1}],\"status\":\"success\"}");

    AnalyticsRequest request = new AnalyticsRequest(
      TIMEOUT,
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      null,
      AnalyticsRequest.NO_PRIORITY,
      true,
      "my-context-id",
      "SELECT 1",
      null,
      null,
      null,
      "/api/v1/some/path?x=1",
      HttpMethod.GET
    );
    assertEquals(singletonList("{\"a\":1}"), rows(send(request)));

    RecordedRequest sent = server.requests().get(0);
    assertEquals("GET", sent.method);
    assertEquals("/api/v1/some/path?x=1", sent.path);
    assertEquals("", sent.bodyAsString());
  }

  @Test
  void errorStatusFailsRequest() {
    server.enqueue(400, "{" +
      "\"requestID\":\"abc-123\"," +
      "\"errors\":[{\"code\":24000,\"msg\":\"Syntax error\"}]," +
      "\"status\":\"fatal\"" +
      "}", Duration.ZERO);

    assertInstanceOf(ParsingFailureException.class, sendExpectingFailure(newAnalyticsRequest()));
    assertEquals(1, server.requests().size());
  }

  @Test
  void errorsAreNotTranslatedIfRequestSaysNotTo() {
    // Like the Columnar SDK.
    server.enqueue(400, "{\"errors\":[{\"code\":24000,\"msg\":\"Syntax error\"}],\"status\":\"fatal\"}", Duration.ZERO);

    AnalyticsRequest request = new AnalyticsRequest(
      TIMEOUT,
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      STATEMENT_JSON.getBytes(UTF_8),
      AnalyticsRequest.NO_PRIORITY,
      true,
      "my-context-id",
      "SELECT 1",
      null,
      null,
      null,
      false,
      1
    );
    assertInstanceOf(CoreErrorCodeAndMessageException.class, sendExpectingFailure(request));
    assertEquals("/api/v1/request", server.requests().get(0).path);
  }

  @Test
  void temporaryFailureIsRetried() throws Exception {
    server.enqueue(503, "{\"errors\":[{\"code\":23000,\"msg\":\"Analytics Service is temporarily unavailable\"}],\"status\":\"fatal\"}", Duration.ZERO);
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1}],\"status\":\"success\"}");

    AnalyticsRequest request = newAnalyticsRequest();
    assertEquals(singletonList("{\"a\":1}"), rows(send(request)));
    assertEquals(2, server.requests().size());
    assertTrue(request.context().retryReasons().contains(RetryReason.ANALYTICS_TEMPORARY_FAILURE), String.valueOf(request.context().retryReasons()));
  }

  @Test
  void errorStatusWithoutErrorsFieldReportsBody() {
    server.enqueue(500, "{\"something\":\"unexpected\"}", Duration.ZERO);

    Throwable cause = sendExpectingFailure(newAnalyticsRequest());
    assertEquals(CouchbaseException.class, cause.getClass());
    assertTrue(cause.getMessage().contains("HTTP status code: 500"), cause.getMessage());
    assertTrue(cause.getMessage().contains("{\"something\":\"unexpected\"}"), cause.getMessage());
  }

  @Test
  void errorStatusWithNonJsonBodyReportsStartOfBody() {
    String start = "<html><body>Bad Gateway: " + repeat('x', 1024);
    String head = start.substring(0, 1024);
    server.enqueue(502, start + "THE-END-SHOULD-NOT-BE-REPORTED</body></html>", Duration.ZERO);

    Throwable cause = sendExpectingFailure(newAnalyticsRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    String message = cause.getMessage();
    assertTrue(message.contains("HTTP status code: 502"), message);
    assertTrue(message.contains(head), "should include the first 1 KiB of the response body, but got: " + message);
    assertFalse(message.contains("THE-END-SHOULD-NOT-BE-REPORTED"), "should include only the first 1 KiB: " + message);
  }

  @Test
  void successStatusWithNonJsonBodyReportsStartOfBody() {
    String start = "<html><body>Welcome to the proxy: " + repeat('x', 1024);
    String head = start.substring(0, 1024);
    server.enqueue(start + "THE-END-SHOULD-NOT-BE-REPORTED</body></html>");

    Throwable cause = sendExpectingFailure(newAnalyticsRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    String message = cause.getMessage();
    assertTrue(message.contains("HTTP status code: 200"), message);
    assertTrue(message.contains(head), "should include the first 1 KiB of the response body, but got: " + message);
    assertFalse(message.contains("THE-END-SHOULD-NOT-BE-REPORTED"), "should include only the first 1 KiB: " + message);
  }

  @Test
  void successStatusWithMalformedJsonDoesNotReportBody() {
    server.enqueue("{\"requestID\":\"abc-123\", SECRET-SHOULD-NOT-BE-REPORTED");

    Throwable cause = sendExpectingFailure(newAnalyticsRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    assertTrue(cause.getMessage().contains("Failed to process analytics response"), cause.getMessage());
    assertFalse(cause.getMessage().contains("SECRET-SHOULD-NOT-BE-REPORTED"), cause.getMessage());
  }

  @Test
  void errorsInSuccessfulResponseFailTheRows() throws Exception {
    server.enqueue("{" +
      "\"requestID\":\"abc-123\"," +
      "\"results\":[{\"a\":1}]," +
      "\"errors\":[{\"code\":24000,\"msg\":\"Syntax error\"}]," +
      "\"status\":\"errors\"" +
      "}");

    AnalyticsResponse response = send(newAnalyticsRequest());

    List<String> received = response.rows()
      .map(row -> new String(row.data(), UTF_8))
      .onErrorResume(e -> Flux.just("<error: " + e.getClass().getSimpleName() + ">"))
      .collectList()
      .block(Duration.ofSeconds(30));

    assertEquals(asList("{\"a\":1}", "<error: ParsingFailureException>"), received);

    // Like the Netty implementation, the trailer still succeeds, and describes the error.
    AnalyticsChunkTrailer trailer = response.trailer().block(Duration.ofSeconds(30));
    assertNotNull(trailer);
    assertEquals("errors", trailer.status());
    assertTrue(trailer.errors().isPresent());
  }

  private static String repeat(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}
