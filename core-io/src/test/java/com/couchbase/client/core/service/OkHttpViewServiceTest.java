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
import com.couchbase.client.core.error.ViewNotFoundException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.view.ViewChunkRow;
import com.couchbase.client.core.msg.view.ViewChunkTrailer;
import com.couchbase.client.core.msg.view.ViewError;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.msg.view.ViewResponse;
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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Sends view requests through {@link OkHttpViewService} to a real HTTP server,
 * and checks the request it sends and how it handles the response.
 */
class OkHttpViewServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static CoreEnvironment env;

  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private OkHttpViewService service;

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

    service = new OkHttpViewService(
      ViewServiceConfig.maxEndpoints(4).build(),
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

  private ViewRequest newViewRequest(Optional<byte[]> keysJson, boolean development) {
    return new ViewRequest(
      TIMEOUT,
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      "my-bucket",
      "my-design",
      "my-view",
      "limit=10&stale=false",
      keysJson,
      development,
      null
    );
  }

  private ViewRequest newViewRequest() {
    return newViewRequest(Optional.empty(), false);
  }

  private ViewResponse send(ViewRequest request) throws Exception {
    service.send(request);
    return request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);
  }

  private Throwable sendExpectingFailure(ViewRequest request) {
    service.send(request);
    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    return e.getCause();
  }

  private static List<String> rows(ViewResponse response) {
    return response.rows()
      .map(ViewChunkRow::data)
      .map(it -> new String(it, UTF_8))
      .collectList()
      .block(Duration.ofSeconds(30));
  }

  @Test
  void roundTrip() throws Exception {
    server.enqueue("{" +
      "\"total_rows\":2," +
      "\"rows\":[{\"id\":\"a\",\"key\":1,\"value\":null},{\"id\":\"b\",\"key\":2,\"value\":null}]," +
      "\"debug_info\":{\"x\":1}" +
      "}");

    ViewResponse response = send(newViewRequest());

    // What the service sent.
    RecordedRequest sent = server.requests().get(0);
    assertEquals("GET", sent.method);
    assertEquals("/my-bucket/_design/my-design/_view/my-view?limit=10&stale=false", sent.path);
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");
    assertEquals("test-user-agent", sent.header("User-Agent"));

    // What the service made of the response.
    assertEquals(2, response.header().totalRows());
    assertFalse(response.header().debug().isPresent(), "debug_info arrived after the header was complete");
    assertEquals(asList("{\"id\":\"a\",\"key\":1,\"value\":null}", "{\"id\":\"b\",\"key\":2,\"value\":null}"), rows(response));

    ViewChunkTrailer trailer = response.trailer().block(Duration.ofSeconds(30));
    assertNotNull(trailer);
    assertFalse(trailer.error().isPresent());
  }

  @Test
  void keysArePostedToDevelopmentView() throws Exception {
    server.enqueue("{\"total_rows\":0,\"rows\":[]}");

    ViewResponse response = send(newViewRequest(Optional.of("{\"keys\":[1,2]}".getBytes(UTF_8)), true));
    assertTrue(rows(response).isEmpty());

    RecordedRequest sent = server.requests().get(0);
    assertEquals("POST", sent.method);
    assertEquals("/my-bucket/_design/dev_my-design/_view/my-view?limit=10&stale=false", sent.path);
    assertEquals("{\"keys\":[1,2]}", sent.bodyAsString());
    assertTrue(sent.header("Content-Type").startsWith("application/json"), sent.header("Content-Type"));
  }

  @Test
  void reduceResultWithoutTotalRowsStillHasHeader() throws Exception {
    // Reduce results have no total_rows. Like Netty, the header is complete at the end of the body if not before.
    server.enqueue("{\"rows\":[]}");

    ViewResponse response = send(newViewRequest());
    assertEquals(0, response.header().totalRows());
    assertEquals(emptyList(), rows(response));
  }

  @Test
  void notFoundFailsRequest() {
    server.enqueue(404, "{\"error\":\"not_found\",\"reason\":\"missing_named_view\"}", Duration.ZERO);

    assertInstanceOf(ViewNotFoundException.class, sendExpectingFailure(newViewRequest()));
    assertEquals(1, server.requests().size());
  }

  @Test
  void unprovisionedNodeIsRetried() throws Exception {
    server.enqueue(404, "{\"error\":\"not_found\",\"reason\":\"missing\"}", Duration.ZERO);
    server.enqueue("{\"total_rows\":1,\"rows\":[{\"id\":\"a\"}]}");

    ViewRequest request = newViewRequest();
    assertEquals(singletonList("{\"id\":\"a\"}"), rows(send(request)));
    assertEquals(2, server.requests().size());
    assertTrue(request.context().retryReasons().contains(RetryReason.VIEWS_TEMPORARY_FAILURE), String.valueOf(request.context().retryReasons()));
  }

  @Test
  void redirectIsRetriedNotFollowed() throws Exception {
    server.enqueue(302, "{\"error\":\"no_active_partition\",\"reason\":\"none\"}", singletonMap("Location", "/somewhere-else"));
    server.enqueue("{\"total_rows\":1,\"rows\":[{\"id\":\"a\"}]}");

    ViewRequest request = newViewRequest();
    assertEquals(singletonList("{\"id\":\"a\"}"), rows(send(request)));

    List<RecordedRequest> sent = server.requests();
    assertEquals(2, sent.size());
    assertEquals(sent.get(0).path, sent.get(1).path, "should retry the original request, not follow the redirect");
    assertTrue(request.context().retryReasons().contains(RetryReason.VIEWS_NO_ACTIVE_PARTITION), String.valueOf(request.context().retryReasons()));
  }

  @Test
  void errorStatusWithoutErrorFieldReportsBody() {
    server.enqueue(400, "{\"something\":\"unexpected\"}", Duration.ZERO);

    Throwable cause = sendExpectingFailure(newViewRequest());
    assertEquals(CouchbaseException.class, cause.getClass());
    assertTrue(cause.getMessage().contains("HTTP status code: 400"), cause.getMessage());
    assertTrue(cause.getMessage().contains("{\"something\":\"unexpected\"}"), cause.getMessage());
  }

  @Test
  void errorStatusWithNonJsonBodyReportsStartOfBody() {
    String start = "<html><body>Bad Gateway: " + repeat('x', 1024);
    String head = start.substring(0, 1024);
    server.enqueue(400, start + "THE-END-SHOULD-NOT-BE-REPORTED</body></html>", Duration.ZERO);

    Throwable cause = sendExpectingFailure(newViewRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    String message = cause.getMessage();
    assertTrue(message.contains("HTTP status code: 400"), message);
    assertTrue(message.contains(head), "should include the first 1 KiB of the response body, but got: " + message);
    assertFalse(message.contains("THE-END-SHOULD-NOT-BE-REPORTED"), "should include only the first 1 KiB: " + message);
  }

  @Test
  void successStatusWithNonJsonBodyReportsStartOfBody() {
    String start = "<html><body>Welcome to the proxy: " + repeat('x', 1024);
    String head = start.substring(0, 1024);
    server.enqueue(start + "THE-END-SHOULD-NOT-BE-REPORTED</body></html>");

    Throwable cause = sendExpectingFailure(newViewRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    String message = cause.getMessage();
    assertTrue(message.contains("HTTP status code: 200"), message);
    assertTrue(message.contains(head), "should include the first 1 KiB of the response body, but got: " + message);
    assertFalse(message.contains("THE-END-SHOULD-NOT-BE-REPORTED"), "should include only the first 1 KiB: " + message);
  }

  @Test
  void successStatusWithMalformedJsonDoesNotReportBody() {
    server.enqueue("{\"debug_info\":{}, SECRET-SHOULD-NOT-BE-REPORTED");

    Throwable cause = sendExpectingFailure(newViewRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    assertTrue(cause.getMessage().contains("Failed to process view response"), cause.getMessage());
    assertFalse(cause.getMessage().contains("SECRET-SHOULD-NOT-BE-REPORTED"), cause.getMessage());
  }

  @Test
  void errorInSuccessfulResponseFailsTheRows() throws Exception {
    server.enqueue("{" +
      "\"total_rows\":1," +
      "\"rows\":[{\"id\":\"a\"}]," +
      "\"error\":\"something_bad\"," +
      "\"reason\":\"it broke\"" +
      "}");

    ViewResponse response = send(newViewRequest());

    List<String> received = response.rows()
      .map(row -> new String(row.data(), UTF_8))
      .onErrorResume(e -> {
        assertTrue(e.getMessage().startsWith("Unknown view error: ViewError{error='something_bad', reason='it broke'}"), e.getMessage());
        return Flux.just("<error>");
      })
      .collectList()
      .block(Duration.ofSeconds(30));

    assertEquals(asList("{\"id\":\"a\"}", "<error>"), received);

    // Like the Netty implementation, the trailer still succeeds, and describes the error.
    ViewChunkTrailer trailer = response.trailer().block(Duration.ofSeconds(30));
    assertNotNull(trailer);
    ViewError error = trailer.error().orElseThrow(AssertionError::new);
    assertEquals("something_bad", error.error());
    assertEquals("it broke", error.reason());
  }

  private static String repeat(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}
