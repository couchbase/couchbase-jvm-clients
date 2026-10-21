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
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.msg.BlockingCaller;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.TestHttpServer.RecordedRequest;
import com.couchbase.client.core.util.CbThreads;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
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

      // Like a request that can't be retried after its connection breaks: the outcome is unknown.
      StepVerifier.create(response.rows().map(row -> new String(row.data(), UTF_8)))
        .expectNext("{\"a\":1}")
        .expectErrorSatisfies(OkHttpQueryServiceTest::assertConnectionClosed)
        .verify(Duration.ofSeconds(30));
      assertConnectionClosed(assertThrows(RequestCanceledException.class, () -> response.trailer().block(Duration.ofSeconds(30))));
      assertNull(request.context().retryReasons());
    }
  }

  private static void assertConnectionClosed(Throwable t) {
    RequestCanceledException e = assertInstanceOf(RequestCanceledException.class, t);
    assertEquals(CancellationReason.noMoreRetries(RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT), e.reason());
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

  // ---- Blocking caller ----

  @Test
  void blockingCallerRunsTheCallOnItsOwnThread() {
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1}],\"status\":\"success\"}");
    QueryRequest request = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    request.blockingCaller(caller);
    AtomicReference<Thread> completedOn = new AtomicReference<>();
    request.response().whenComplete((response, error) -> completedOn.set(Thread.currentThread()));

    service.send(request);

    // Done by the time send returns, on this thread.
    assertTrue(request.completed());
    assertSame(Thread.currentThread(), completedOn.get());
    assertEquals(singletonList("{\"a\":1}"), rows(caller.await(request.response())));
  }

  @Test
  void blockingCallerWaitsOutTheRetryDelayAndRetriesOnItsOwnThread() {
    server.enqueue(503, "{" +
      "\"requestID\":\"abc-123\"," +
      "\"errors\":[{\"code\":12345,\"msg\":\"try again\",\"retry\":true}]," +
      "\"status\":\"errors\"" +
      "}", Duration.ZERO);
    server.enqueue("{\"requestID\":\"abc-124\",\"results\":[{\"a\":1}],\"status\":\"success\"}");

    // Like the real core, which sends the retry to a node's service (here, the only one).
    List<Thread> retriedOn = new CopyOnWriteArrayList<>();
    doAnswer(invocation -> {
      retriedOn.add(Thread.currentThread());
      service.send(invocation.<Request<? extends Response>>getArgument(0));
      return null;
    }).when(core).send(any(), anyBoolean());

    QueryRequest request = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    request.blockingCaller(caller);
    service.send(request); // fails, and hands the retry to the caller
    assertFalse(request.completed());

    QueryResponse response = caller.await(request.response());
    assertEquals("abc-124", response.header().requestId());
    assertEquals(singletonList(Thread.currentThread()), retriedOn);
    assertEquals(singleton(RetryReason.QUERY_ERROR_RETRYABLE), request.context().retryReasons());
    assertEquals(2, server.requests().size());
  }

  @Test
  @SuppressWarnings("unchecked")
  void timeoutCancelsACallRunningOnTheBlockingCallersThread() {
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[],\"status\":\"success\"}", Duration.ofSeconds(5));
    QueryRequest request = newQueryRequest(Duration.ofMillis(200));
    BlockingCaller caller = new BlockingCaller();
    request.blockingCaller(caller);
    env.timer().register((Request<Response>) (Request<?>) request); // as the core does when it first sends a request

    long start = System.nanoTime();
    service.send(request); // returns once the timeout cancels the call

    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3), "should stop waiting for the response");
    assertEquals(CancellationReason.TIMEOUT, request.cancellationReason());
    assertThrows(UnambiguousTimeoutException.class, () -> caller.await(request.response()));
  }

  @Test
  void interruptingAPlatformThreadWaitingForTheResponseCancelsTheRequest() {
    // A platform thread's socket read can't be interrupted, so the interrupt watcher cancels the request.
    assertFalse(CbThreads.isVirtual(Thread.currentThread()));
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[],\"status\":\"success\"}", Duration.ofSeconds(5));
    QueryRequest request = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    request.blockingCaller(caller);

    ScheduledExecutorService interrupter = Executors.newSingleThreadScheduledExecutor();
    try {
      Thread callerThread = Thread.currentThread();
      interrupter.schedule(callerThread::interrupt, 200, TimeUnit.MILLISECONDS);

      long start = System.nanoTime();
      service.send(request); // returns once the watcher cancels the call
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3), "should stop waiting for the response");
      assertEquals(CancellationReason.STOPPED_LISTENING, request.cancellationReason());

      RuntimeException e = assertThrows(RuntimeException.class, () -> caller.await(request.response()));
      InterruptedException interrupted = assertInstanceOf(InterruptedException.class, e.getCause());
      assertInstanceOf(RequestCanceledException.class, interrupted.getSuppressed()[0]);
      assertFalse(Thread.currentThread().isInterrupted());

    } finally {
      interrupter.shutdownNow();
      Thread.interrupted();
    }
  }

  @Test
  void interruptingTheBlockingCallerWhileItReadsTheRowsReportsTheInterrupt() {
    // The interrupt makes reading the rows fail. The caller reports the interrupt, not that failure.
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1},{\"a\":2},{\"a\":3}],\"status\":\"success\"}");
    QueryRequest request = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    request.blockingCaller(caller);

    // Like the blocking query API, collects the rows as they arrive (on this thread). Interrupted by the first one.
    CompletableFuture<List<String>> rows = request.response().thenCompose(response -> response.rows()
      .map(row -> {
        Thread.currentThread().interrupt();
        return new String(row.data(), UTF_8);
      })
      .collectList()
      .toFuture());

    service.send(request);

    RuntimeException e = assertThrows(RuntimeException.class, () -> caller.await(rows));
    InterruptedException interrupted = assertInstanceOf(InterruptedException.class, e.getCause());
    assertEquals(1, interrupted.getSuppressed().length, "should report what the interrupt caused");
    assertFalse(Thread.currentThread().isInterrupted());
  }

  // ---- Blocking caller with a row action ----

  @Test
  void blockingCallerWithARowActionTakesTheRowsAsTheyAreRead() {
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1},{\"a\":2}],\"status\":\"success\"}");
    QueryRequest request = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    List<String> taken = new ArrayList<>();
    List<Thread> takenOn = new ArrayList<>();
    caller.rowAction((QueryChunkRow row) -> {
      taken.add(new String(row.data(), UTF_8));
      takenOn.add(Thread.currentThread());
    });
    request.blockingCaller(caller);

    service.send(request);

    // Taken by the time send returns, on this thread.
    assertEquals(asList("{\"a\":1}", "{\"a\":2}"), taken);
    assertEquals(asList(Thread.currentThread(), Thread.currentThread()), takenOn);

    // The response's rows end without any.
    QueryResponse response = caller.await(request.response());
    assertTrue(caller.rowsWentToAction());
    assertEquals(emptyList(), rows(response));
    assertEquals("success", requireNonNull(response.trailer().block()).status());
    assertNull(caller.rowActionFailure());
  }

  @Test
  void rowActionThatThrowsStopsReadingTheResponse() {
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1},{\"a\":2},{\"a\":3}],\"status\":\"success\"}");
    QueryRequest request = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    AtomicInteger calls = new AtomicInteger();
    RuntimeException failure = new RuntimeException("row action failed");
    caller.rowAction(row -> {
      calls.incrementAndGet();
      throw failure;
    });
    request.blockingCaller(caller);

    service.send(request);

    assertEquals(1, calls.get());
    assertSame(failure, caller.rowActionFailure());

    // The request succeeded when the header arrived. The rows and trailer end with the failure.
    QueryResponse response = caller.await(request.response());
    StepVerifier.create(response.rows()).expectError(BlockingCaller.RowActionException.class).verify();
    StepVerifier.create(response.trailer()).expectError(BlockingCaller.RowActionException.class).verify();
  }

  @Test
  void rowsArriveTheUsualWayWhenAnotherThreadReadsTheResponse() throws Exception {
    // For example, a prepared query retried on another thread: the row action only runs on the caller's thread.
    server.enqueue("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1}],\"status\":\"success\"}");
    QueryRequest request = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    AtomicInteger calls = new AtomicInteger();
    caller.rowAction(row -> calls.incrementAndGet());
    request.blockingCaller(caller);

    CompletableFuture.runAsync(() -> service.send(request)).get(); // not the caller's thread, so the call is enqueued

    QueryResponse response = request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertFalse(caller.rowsWentToAction());
    assertEquals(singletonList("{\"a\":1}"), rows(response));
    assertEquals(0, calls.get());
  }
}
