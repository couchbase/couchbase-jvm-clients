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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
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
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Sends query requests through {@link OkHttpQueryService} to a real HTTP server,
 * and checks the request it sends and how it handles the response.
 */
class OkHttpQueryServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String STATEMENT_JSON = "{\"statement\":\"SELECT 1\"}";

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
      new HostAndPort(TestHttpServer.HOST_NAME, server.port())
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

  private QueryRequest newQueryRequest() {
    return newQueryRequest(TIMEOUT);
  }

  private QueryRequest newQueryRequest(Duration timeout) {
    return newQueryRequest(timeout, true);
  }

  private QueryRequest newQueryRequest(Duration timeout, boolean idempotent) {
    return new QueryRequest(
      timeout,
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      "SELECT 1",
      STATEMENT_JSON.getBytes(UTF_8),
      idempotent,
      null,
      null,
      null,
      null,
      null,
      false
    );
  }

  private QueryResponse send(QueryRequest request) throws Exception {
    service.send(request);
    return request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);
  }

  private static List<String> rows(QueryResponse response) {
    return response.rows()
      .map(QueryChunkRow::data)
      .map(it -> new String(it, UTF_8))
      .collectList()
      .block(Duration.ofSeconds(30));
  }

  @Test
  void requestWhoseDeadlineHasPassedIsNotSent() throws Exception {
    // Like the Netty endpoints. (No timer cancels the request here; only its deadline has passed.)
    QueryRequest request = newQueryRequest(Duration.ofNanos(1));
    Thread.sleep(1);
    service.send(request);

    assertTrue(request.completed());
    assertEquals(CancellationReason.TIMEOUT, request.cancellationReason());
    assertTrue(server.requests().isEmpty(), "shouldn't have sent the request");
  }

  @Test
  void roundTrip() throws Exception {
    server.enqueue("{" +
      "\"requestID\":\"abc-123\"," +
      "\"signature\":{\"*\":\"*\"}," +
      "\"results\":[{\"a\":1},{\"a\":2}]," +
      "\"status\":\"success\"," +
      "\"metrics\":{\"resultCount\":2}" +
      "}");

    QueryResponse response = send(newQueryRequest());

    // What the service sent.
    RecordedRequest sent = server.requests().get(0);
    assertEquals("POST", sent.method);
    assertEquals("/query/service", sent.path);
    assertEquals(STATEMENT_JSON, sent.bodyAsString());
    assertTrue(sent.header("Content-Type").startsWith("application/json"), sent.header("Content-Type"));
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");
    assertEquals("test-user-agent", sent.header("User-Agent"));

    // What the service made of the response.
    assertEquals("abc-123", response.header().requestId());
    assertTrue(response.header().signature().isPresent());
    assertEquals(asList("{\"a\":1}", "{\"a\":2}"), rows(response));

    QueryChunkTrailer trailer = response.trailer().block(Duration.ofSeconds(30));
    assertNotNull(trailer);
    assertEquals("success", trailer.status());
    assertEquals("{\"resultCount\":2}", new String(trailer.metrics().orElseThrow(AssertionError::new), UTF_8));
  }

  @Test
  void noRows() throws Exception {
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[],\"status\":\"success\"}");

    QueryResponse response = send(newQueryRequest());

    assertTrue(rows(response).isEmpty());
    assertEquals("success", response.trailer().block(Duration.ofSeconds(30)).status());
  }

  @Test
  void errorStatusFailsRequest() {
    server.enqueue(400, "{" +
      "\"requestID\":\"abc-123\"," +
      "\"errors\":[{\"code\":3000,\"msg\":\"syntax error - line 1, column 1\"}]," +
      "\"status\":\"fatal\"" +
      "}", Duration.ZERO);

    QueryRequest request = newQueryRequest();
    service.send(request);

    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    assertInstanceOf(ParsingFailureException.class, e.getCause());
  }

  @Test
  void errorStatusWithNonJsonBodyReportsStartOfBody() {
    // For example, an error page from a proxy. Longer than 1 KiB, so only the start should be reported.
    String start = "<html><body>Bad Request: " + repeat('x', 1024);
    String head = start.substring(0, 1024);
    String body = start + "THE-END-SHOULD-NOT-BE-REPORTED</body></html>";
    server.enqueue(400, body, Duration.ZERO);

    QueryRequest request = newQueryRequest();
    service.send(request);

    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    Throwable cause = e.getCause();
    assertInstanceOf(CouchbaseException.class, cause);

    String message = cause.getMessage();
    assertTrue(message.contains(head), "should include the first 1 KiB of the response body, but got: " + message);
    assertFalse(message.contains("THE-END-SHOULD-NOT-BE-REPORTED"), "should include only the first 1 KiB: " + message);
  }

  @Test
  void successStatusWithNonJsonBodyReportsStartOfBody() {
    // For example, a proxy that answers with 200 and an HTML page.
    String start = "<html><body>Welcome to the proxy: " + repeat('x', 1024);
    String head = start.substring(0, 1024);
    server.enqueue(start + "THE-END-SHOULD-NOT-BE-REPORTED</body></html>");

    QueryRequest request = newQueryRequest();
    service.send(request);

    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    assertInstanceOf(DecodingFailureException.class, e.getCause());

    String message = e.getCause().getMessage();
    assertTrue(message.contains("HTTP status code: 200 OK"), message);
    assertTrue(message.contains(head), "should include the first 1 KiB of the response body, but got: " + message);
    assertFalse(message.contains("THE-END-SHOULD-NOT-BE-REPORTED"), "should include only the first 1 KiB: " + message);
  }

  @Test
  void successStatusWithMalformedJsonDoesNotReportBody() {
    // Breaks before the header is complete, but it's JSON, so the body isn't reported.
    server.enqueue("{\"requestID\":\"abc-123\", SECRET-SHOULD-NOT-BE-REPORTED");

    QueryRequest request = newQueryRequest();
    service.send(request);

    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    assertInstanceOf(DecodingFailureException.class, e.getCause());

    String message = e.getCause().getMessage();
    assertTrue(message.contains("Failed to process query response"), message);
    assertFalse(message.contains("SECRET-SHOULD-NOT-BE-REPORTED"), message);
  }

  private static final String START_OF_RESULT = "{\"requestID\":\"abc-123\",\"signature\":{\"*\":\"*\"},\"results\":[";

  private OkHttpQueryService serviceFor(BrokenOffHttpServer brokenOffServer) {
    return new OkHttpQueryService(
      QueryServiceConfig.maxEndpoints(4).build(),
      core.context(),
      new HostAndPort("127.0.0.1", brokenOffServer.port())
    );
  }

  /**
   * The query service sends the start of the response right away, and the first row once it has one,
   * so this can happen any time while the query executes (for example, the node fails over).
   * Like the Netty implementation, the request is retried if it's idempotent.
   */
  @Test
  void connectionBreakingBeforeFirstRowRetriesIdempotentRequest() throws Exception {
    try (BrokenOffHttpServer brokenOff = new BrokenOffHttpServer(200, START_OF_RESULT)) {
      QueryRequest request = newQueryRequest();
      serviceFor(brokenOff).send(request);

      waitUntilCondition(() -> request.context().retryReasons() != null);
      assertEquals(singleton(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT), request.context().retryReasons());
      assertFalse(request.completed()); // waiting to be retried (by the real core; the mock doesn't)
      request.cancel(CancellationReason.STOPPED_LISTENING);
    }
  }

  @Test
  void connectionBreakingBeforeFirstRowCancelsNonIdempotentRequest() throws Exception {
    try (BrokenOffHttpServer brokenOff = new BrokenOffHttpServer(200, START_OF_RESULT)) {
      QueryRequest request = newQueryRequest(TIMEOUT, false);
      serviceFor(brokenOff).send(request);
      ExecutionException e = assertThrows(ExecutionException.class,
        () -> request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

      // The server may have executed it, so it isn't retried, and the outcome is unknown.
      assertInstanceOf(RequestCanceledException.class, e.getCause());
      assertEquals(CancellationReason.noMoreRetries(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT), request.cancellationReason());
    }
  }

  @Test
  void connectionBreakingDuringErrorResponseRetriesIdempotentRequest() throws Exception {
    try (BrokenOffHttpServer brokenOff = new BrokenOffHttpServer(500, "{\"requestID\":\"abc-123\",\"errors\":[{\"code\":")) {
      QueryRequest request = newQueryRequest();
      serviceFor(brokenOff).send(request);

      waitUntilCondition(() -> request.context().retryReasons() != null);
      assertEquals(singleton(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT), request.context().retryReasons());
      request.cancel(CancellationReason.STOPPED_LISTENING);
    }
  }

  @Test
  void connectionBreakingAfterFirstRowFailsTheRows() throws Exception {
    // The request has completed (with the first row), so it can't be retried.
    try (BrokenOffHttpServer brokenOff = new BrokenOffHttpServer(200, "{\"requestID\":\"abc-123\",\"results\":[{\"a\":1},")) {
      QueryRequest request = newQueryRequest();
      serviceFor(brokenOff).send(request);
      QueryResponse response = request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

      StepVerifier.create(response.rows().map(row -> new String(row.data(), UTF_8)))
        .expectNext("{\"a\":1}")
        .expectError(DecodingFailureException.class)
        .verify(Duration.ofSeconds(30));
      assertNull(request.context().retryReasons());
    }
  }

  @Test
  void bodyThatBreaksAfterFirstRowFailsTheRows() throws Exception {
    // The response future succeeds with the first row; then the body stops making sense.
    // The body is JSON, so it isn't reported.
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1}, not-json");

    QueryResponse response = send(newQueryRequest());

    List<String> received = response.rows()
      .map(row -> new String(row.data(), UTF_8))
      .onErrorResume(e -> {
        assertInstanceOf(DecodingFailureException.class, e);
        assertTrue(e.getMessage().contains("Failed to process query response"), e.getMessage());
        return Flux.just("<decoding failure>");
      })
      .collectList()
      .block(Duration.ofSeconds(30));

    assertEquals(asList("{\"a\":1}", "<decoding failure>"), received);
    assertThrows(DecodingFailureException.class, () -> response.trailer().block(Duration.ofSeconds(30)));
  }

  @Test
  void successStatusWithoutResultsOrStatusFailsPromptly() {
    // Not something a well-behaved server does. The request fails right away instead of waiting for its timeout.
    server.enqueue("{\"requestID\":\"abc-123\"}");

    QueryRequest request = newQueryRequest();
    service.send(request);

    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() / 2, TimeUnit.MILLISECONDS));
    assertInstanceOf(DecodingFailureException.class, e.getCause());
    assertTrue(e.getCause().getMessage().contains("ended before any results or status arrived"), e.getCause().getMessage());
  }

  @Test
  void successStatusWithOnlyErrorsReportsTheError() {
    server.enqueue("{\"requestID\":\"abc-123\",\"errors\":[{\"code\":3000,\"msg\":\"syntax error\"}]}");

    QueryRequest request = newQueryRequest();
    service.send(request);

    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() / 2, TimeUnit.MILLISECONDS));
    assertInstanceOf(ParsingFailureException.class, e.getCause());
  }

  private static String repeat(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }

  @Test
  void errorsInSuccessfulResponseFailTheRows() throws Exception {
    // The status code says success, but the body reports an error after the first row.
    server.enqueue("{" +
      "\"requestID\":\"abc-123\"," +
      "\"results\":[{\"a\":1}]," +
      "\"errors\":[{\"code\":3000,\"msg\":\"syntax error\"}]," +
      "\"status\":\"errors\"" +
      "}");

    QueryResponse response = send(newQueryRequest());

    List<String> received = response.rows()
      .map(row -> new String(row.data(), UTF_8))
      .onErrorResume(e -> {
        assertInstanceOf(CouchbaseException.class, e);
        return Flux.just("<error: " + e.getClass().getSimpleName() + ">");
      })
      .collectList()
      .block(Duration.ofSeconds(30));

    assertEquals(asList("{\"a\":1}", "<error: ParsingFailureException>"), received);

    // Like the Netty implementation, the trailer still succeeds, and describes the error.
    QueryChunkTrailer trailer = response.trailer().block(Duration.ofSeconds(30));
    assertNotNull(trailer);
    assertEquals("errors", trailer.status());
    assertTrue(trailer.errors().isPresent());
  }
}
