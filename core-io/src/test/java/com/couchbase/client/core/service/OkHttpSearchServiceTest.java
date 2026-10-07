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
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;
import com.couchbase.client.core.msg.search.SearchResponse;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.TestHttpServer.RecordedRequest;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.Nullable;
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
 * Sends search requests through {@link OkHttpSearchService} to a real HTTP server,
 * and checks the request it sends and how it handles the response.
 */
class OkHttpSearchServiceTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String QUERY_JSON = "{\"query\":{\"match\":\"hello\"}}";

  private static CoreEnvironment env;

  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;
  private OkHttpSearchService service;

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

    service = new OkHttpSearchService(
      SearchServiceConfig.maxEndpoints(4).build(),
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

  private ServerSearchRequest newSearchRequest(String indexName, @Nullable CoreBucketAndScope scope) {
    return new ServerSearchRequest(
      TIMEOUT,
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      indexName,
      QUERY_JSON.getBytes(UTF_8),
      null,
      scope
    );
  }

  private ServerSearchRequest newSearchRequest() {
    return newSearchRequest("my-index", null);
  }

  private SearchResponse send(ServerSearchRequest request) throws Exception {
    service.send(request);
    return request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);
  }

  private static Throwable sendExpectingFailure(OkHttpSearchService service, ServerSearchRequest request) {
    service.send(request);
    ExecutionException e = assertThrows(ExecutionException.class,
      () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    return e.getCause();
  }

  private static List<String> rows(SearchResponse response) {
    return response.rows()
      .map(SearchChunkRow::data)
      .map(it -> new String(it, UTF_8))
      .collectList()
      .block(Duration.ofSeconds(30));
  }

  @Test
  void roundTrip() throws Exception {
    server.enqueue("{" +
      "\"status\":{\"total\":1,\"failed\":0,\"successful\":1}," +
      "\"request\":{}," +
      "\"hits\":[{\"id\":\"a\"},{\"id\":\"b\"}]," +
      "\"total_hits\":2," +
      "\"max_score\":1.5," +
      "\"took\":123," +
      "\"facets\":{\"type\":{}}" +
      "}");

    SearchResponse response = send(newSearchRequest());

    // What the service sent.
    RecordedRequest sent = server.requests().get(0);
    assertEquals("POST", sent.method);
    assertEquals("/api/index/my-index/query", sent.path);
    assertEquals(QUERY_JSON, sent.bodyAsString());
    assertTrue(sent.header("Content-Type").startsWith("application/json"), sent.header("Content-Type"));
    assertTrue(sent.header("Authorization").startsWith("Basic "), "should authenticate");
    assertEquals("test-user-agent", sent.header("User-Agent"));

    // What the service made of the response.
    assertEquals("{\"total\":1,\"failed\":0,\"successful\":1}", new String(response.header().getStatus(), UTF_8));
    assertEquals(asList("{\"id\":\"a\"}", "{\"id\":\"b\"}"), rows(response));

    SearchChunkTrailer trailer = response.trailer().block(Duration.ofSeconds(30));
    assertNotNull(trailer);
    assertEquals(2, trailer.totalRows());
    assertEquals(1.5, trailer.maxScore());
    assertEquals(123, trailer.took());
    assertEquals("{\"type\":{}}", new String(trailer.facets(), UTF_8));
  }

  @Test
  void scopedIndexPath() throws Exception {
    server.enqueue("{\"status\":{},\"hits\":[],\"total_hits\":0}");

    SearchResponse response = send(newSearchRequest("my index", new CoreBucketAndScope("my-bucket", "my-scope")));
    assertTrue(rows(response).isEmpty());

    // Same encoding as CoreHttpPath.formatPath, used by the Netty implementation.
    assertEquals("/api/bucket/my-bucket/scope/my-scope/index/my%20index/query", server.requests().get(0).path);
  }

  @Test
  void errorStatusFailsRequest() {
    server.enqueue(400, "{\"error\":\"rest_auth: preparePerms, err: index not found\",\"request\":{},\"status\":\"fail\"}", Duration.ZERO);

    assertInstanceOf(IndexNotFoundException.class, sendExpectingFailure(service, newSearchRequest()));
  }

  @Test
  void rateLimitedIsNotRetried() {
    server.enqueue(429, "{\"error\":\"num_concurrent_requests limit exceeded\",\"status\":\"fail\"}", Duration.ZERO);

    ServerSearchRequest request = newSearchRequest();
    assertInstanceOf(RateLimitedException.class, sendExpectingFailure(service, request));
    assertEquals(1, server.requests().size());
  }

  @Test
  void tooManyRequestsIsRetried() throws Exception {
    server.enqueue(429, "{\"error\":\"too busy\",\"status\":\"fail\"}", Duration.ZERO);
    server.enqueue("{\"status\":{},\"hits\":[{\"id\":\"a\"}],\"total_hits\":1}");

    ServerSearchRequest request = newSearchRequest();
    SearchResponse response = send(request);

    assertEquals(asList("{\"id\":\"a\"}"), rows(response));
    assertEquals(2, server.requests().size());
    assertTrue(request.context().retryReasons().contains(RetryReason.SEARCH_TOO_MANY_REQUESTS), String.valueOf(request.context().retryReasons()));
  }

  @Test
  void errorStatusWithoutErrorFieldReportsBody() {
    server.enqueue(500, "{\"something\":\"unexpected\"}", Duration.ZERO);

    Throwable cause = sendExpectingFailure(service, newSearchRequest());
    assertEquals(CouchbaseException.class, cause.getClass());
    assertTrue(cause.getMessage().contains("HTTP status code: 500"), cause.getMessage());
    assertTrue(cause.getMessage().contains("{\"something\":\"unexpected\"}"), cause.getMessage());
  }

  @Test
  void errorStatusWithNonJsonBodyReportsStartOfBody() {
    String start = "<html><body>Bad Gateway: " + repeat('x', 1024);
    String head = start.substring(0, 1024);
    server.enqueue(502, start + "THE-END-SHOULD-NOT-BE-REPORTED</body></html>", Duration.ZERO);

    Throwable cause = sendExpectingFailure(service, newSearchRequest());
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

    Throwable cause = sendExpectingFailure(service, newSearchRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    String message = cause.getMessage();
    assertTrue(message.contains("HTTP status code: 200"), message);
    assertTrue(message.contains(head), "should include the first 1 KiB of the response body, but got: " + message);
    assertFalse(message.contains("THE-END-SHOULD-NOT-BE-REPORTED"), "should include only the first 1 KiB: " + message);
  }

  @Test
  void successStatusWithMalformedJsonDoesNotReportBody() {
    server.enqueue("{\"request\":{}, SECRET-SHOULD-NOT-BE-REPORTED");

    Throwable cause = sendExpectingFailure(service, newSearchRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    assertTrue(cause.getMessage().contains("Failed to process search response"), cause.getMessage());
    assertFalse(cause.getMessage().contains("SECRET-SHOULD-NOT-BE-REPORTED"), cause.getMessage());
  }

  @Test
  void errorInSuccessfulResponseFailsTheRows() throws Exception {
    server.enqueue("{" +
      "\"status\":{\"total\":1,\"failed\":0,\"successful\":1}," +
      "\"hits\":[{\"id\":\"a\"}]," +
      "\"error\":\"something went wrong\"" +
      "}");

    SearchResponse response = send(newSearchRequest());

    List<String> received = response.rows()
      .map(row -> new String(row.data(), UTF_8))
      .onErrorResume(e -> {
        // Like the Netty implementation, the message has the raw JSON value of the error field.
        assertTrue(e.getMessage().startsWith("Unknown search error: \"something went wrong\""), e.getMessage());
        return Flux.just("<error>");
      })
      .collectList()
      .block(Duration.ofSeconds(30));

    assertEquals(asList("{\"id\":\"a\"}", "<error>"), received);

    // Like the Netty implementation, the trailer still succeeds.
    assertNotNull(response.trailer().block(Duration.ofSeconds(30)));
  }

  @Test
  void successStatusWithoutStatusFieldFailsPromptly() {
    // The status completes a search response's header. Without it, fail right away instead of waiting for the timeout.
    server.enqueue("{\"hits\":[{\"id\":\"a\"}],\"total_hits\":1}");

    Throwable cause = sendExpectingFailure(service, newSearchRequest());
    assertInstanceOf(DecodingFailureException.class, cause);
    assertTrue(cause.getMessage().contains("Failed to process search response"), cause.getMessage());
  }

  private static String repeat(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}
