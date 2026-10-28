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
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.TrafficCaptureFormat;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Traffic capture for the OkHttp-based services (see {@link TrafficCaptureInterceptor}).
 */
class TrafficCaptureInterceptorTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String STATEMENT_JSON = "{\"statement\":\"SELECT 1\"}";

  private final SimpleEventBus eventBus = new SimpleEventBus(true);
  private CoreEnvironment env;
  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;

  @AfterEach
  void teardown() {
    try {
      if (okHttpClient != null) okHttpClient.close();
      if (server != null) server.close();
    } finally {
      if (env != null) env.shutdown();
    }
  }

  /**
   * Sends a query to a test server, with traffic capture enabled for the given services, and reads its rows.
   */
  private void sendQuery(ServiceType... servicesToCapture) throws Exception {
    sendQuery(singletonMap("Content-Type", "application/json"), servicesToCapture); // like the query service
  }

  private void sendQuery(Map<String, String> responseHeaders, ServiceType... servicesToCapture) throws Exception {
    env = CoreEnvironment.builder()
      .eventBus(eventBus)
      .ioConfig(io -> io.captureTraffic(servicesToCapture))
      .build();
    server = TestHttpServer.startHttp();
    okHttpClient = newTestOkHttpClient(env);
    Core core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);
    OkHttpQueryService service = new OkHttpQueryService(
      QueryServiceConfig.maxEndpoints(4).build(),
      core.context(),
      new HostAndPort(TestHttpServer.HOST_NAME, server.port())
    );

    server.enqueue(200, "{\"requestID\":\"abc-123\",\"results\":[{\"greeting\":\"hello\"}],\"status\":\"success\"}", responseHeaders);
    QueryRequest request = new QueryRequest(
      TIMEOUT, core.context(), BestEffortRetryStrategy.INSTANCE, core.context().authenticator(),
      "SELECT 1", STATEMENT_JSON.getBytes(UTF_8), true, null, null, null, null, null, false
    );
    service.send(request);
    QueryResponse response = request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    List<String> rows = response.rows()
      .map(QueryChunkRow::data)
      .map(it -> new String(it, UTF_8))
      .collectList()
      .block(TIMEOUT);
    assertEquals(1, rows.size());
    response.trailer().block(TIMEOUT);
  }

  private <T extends Event> String captured(Class<T> type) {
    return eventBus.publishedEvents().stream()
      .filter(type::isInstance)
      .map(Event::description)
      .collect(Collectors.joining());
  }

  @Test
  void capturesRequestAndResponseOfCapturedService() throws Exception {
    sendQuery(ServiceType.QUERY);

    String written = captured(WriteTrafficCapturedEvent.class);
    assertTrue(written.contains("POST /query/service HTTP/1.1"), written);
    assertTrue(written.contains(STATEMENT_JSON), written);
    assertTrue(written.contains("Authorization: Basic <masked>"), written);
    assertTrue(written.contains("Content-Length: <masked>"), written);
    assertFalse(written.contains("username"), "credentials should be masked: " + written);
    assertFalse(written.contains("dXNlcm5hbWU6cGFzc3dvcmQ="), "credentials should be masked: " + written); // Base64

    String read = captured(ReadTrafficCapturedEvent.class);
    assertTrue(read.contains("HTTP/1.1 200"), read);
    assertTrue(read.contains("\"greeting\":\"hello\""), read);
  }

  @Test
  void masksSecretFormFields() throws Exception {
    Request request = new Request.Builder()
      .url("http://localhost:8091/settings/rbac/users/local/alice")
      .header("Authorization", "Bearer some.jwt.token")
      .header("Content-Type", "application/x-www-form-urlencoded") // added by OkHttp before network interceptors
      .put(new FormBody.Builder().add("name", "Alice").add("password", "s3cret").add("clientKey", "AKIA/xyz").build())
      .build();

    String captured = TrafficCaptureInterceptor.describe(request);

    assertTrue(captured.contains("Authorization: Bearer <masked>\n"), captured);
    assertTrue(captured.contains("name=Alice&password=<masked>&clientKey=<masked>"), captured);
    assertFalse(captured.contains("s3cret"), captured);
    assertFalse(captured.contains("some.jwt.token"), captured);
  }

  @Test
  void masksSecretJsonFieldsAndTheLengthOfTheOriginalBody() throws Exception {
    String body = "{\"name\":\"myLink\",\"password\" : \"s3cret\",\"other\":\"visible\"}";
    Request request = new Request.Builder()
      .url("http://localhost:8095/analytics/link")
      .header("Content-Type", "application/json") // like Content-Length, added by OkHttp before network interceptors
      .header("Content-Length", String.valueOf(body.length()))
      .post(RequestBody.create(body, MediaType.get("application/json")))
      .build();

    String captured = TrafficCaptureInterceptor.describe(request);

    String masked = "{\"name\":\"myLink\",\"password\" : \"<masked>\",\"other\":\"visible\"}";
    assertTrue(captured.contains("Content-Length: <masked>\n"), captured);
    // The frame gives the masked text's length, not the original's (which would reveal the password's length).
    assertTrue(captured.endsWith(TrafficCaptureInterceptor.framedText(masked, 0, masked.length())), captured);
  }

  @Test
  void masksSecretFieldsInBodiesThatAreHexDumped() throws Exception {
    Request request = new Request.Builder()
      .url("http://localhost:8091/settings/rbac/users/local/alice")
      .put(RequestBody.create("password=s3cret&name=Alice".getBytes(UTF_8), null))
      .build();

    String captured = TrafficCaptureInterceptor.describe(request);

    assertTrue(captured.endsWith(TrafficCaptureFormat.hexDump("password=<masked>&name=Alice".getBytes(UTF_8), 0)), captured);
  }

  @Test
  void masksAuthorizationWithoutScheme() throws Exception {
    Request request = new Request.Builder()
      .url("http://localhost:8091/pools")
      .header("Authorization", "opaque-credential")
      .build();

    assertTrue(TrafficCaptureInterceptor.describe(request).contains("Authorization: <masked>\n"));
  }

  @Test
  void bodyWithoutTextualContentTypeIsHexDumped() throws Exception {
    sendQuery(emptyMap(), ServiceType.QUERY);

    String read = captured(ReadTrafficCapturedEvent.class);
    assertTrue(read.contains("HTTP/1.1 200"), read); // the status line and headers are still text
    assertTrue(read.contains("|00000000| 7b 22 72 65"), read); // {"re...
    assertFalse(read.contains("\"greeting\":\"hello\""), read);
  }

  @Test
  void doesNotCaptureOtherServices() throws Exception {
    sendQuery(ServiceType.SEARCH);

    assertEquals("", captured(WriteTrafficCapturedEvent.class));
    assertEquals("", captured(ReadTrafficCapturedEvent.class));
  }

  @Test
  void decodesCharactersSplitBetweenChunks() {
    // 1-, 2-, 3-, and 4-byte characters.
    String text = "a\u00e9\u20ac\ud834\udd1e!";
    byte[] bytes = text.getBytes(UTF_8);

    // Every way of splitting it into two chunks, and one byte at a time.
    for (int split = 0; split <= bytes.length; split++) {
      TrafficCaptureInterceptor.Utf8ChunkDecoder decoder = new TrafficCaptureInterceptor.Utf8ChunkDecoder();
      String decoded = decoder.decode(Arrays.copyOfRange(bytes, 0, split))
        + decoder.decode(Arrays.copyOfRange(bytes, split, bytes.length))
        + decoder.flush();
      assertEquals(text, decoded, "split at " + split);
    }
    TrafficCaptureInterceptor.Utf8ChunkDecoder decoder = new TrafficCaptureInterceptor.Utf8ChunkDecoder();
    StringBuilder oneByteAtATime = new StringBuilder();
    for (byte b : bytes) {
      oneByteAtATime.append(decoder.decode(new byte[]{b}));
    }
    assertEquals(text, oneByteAtATime.append(decoder.flush()).toString());
  }

  @Test
  void incompleteCharacterAtTheEndIsFlushedAsIs() {
    TrafficCaptureInterceptor.Utf8ChunkDecoder decoder = new TrafficCaptureInterceptor.Utf8ChunkDecoder();
    byte[] euro = "\u20ac".getBytes(UTF_8);
    assertEquals("x", decoder.decode(new byte[]{'x', euro[0], euro[1]})); // the euro sign isn't complete yet
    assertEquals("\ufffd", decoder.flush()); // the body ended: malformed, like a plain decode
  }

  @Test
  void textualContentTypes() {
    for (String type : new String[]{
      "application/json", "application/json; charset=utf-8", "text/plain", "text/html; charset=UTF-8",
      "application/problem+json", "application/xml", "application/x-www-form-urlencoded"
    }) {
      assertTrue(TrafficCaptureInterceptor.isTextual(type), type);
    }
    for (String type : new String[]{
      "application/octet-stream", "image/png", "text/plain; charset=iso-8859-1", "not a media type"
    }) {
      assertFalse(TrafficCaptureInterceptor.isTextual(type), type);
    }
    assertFalse(TrafficCaptureInterceptor.isTextual(null), "no content type");
  }

  @Test
  void textIsFramedWithItsOffsetAndLength() {
    String framed = TrafficCaptureInterceptor.framedText(" hi ", 0x2000, 4);
    String nl = System.lineSeparator();
    assertEquals("+---- text: offset 0x00002000, 4 bytes ----+" + nl + " hi " + nl + "+---- end of text ----+", framed);
  }

  @Test
  void decoderCountsOnlyTheBytesItDecoded() {
    TrafficCaptureInterceptor.Utf8ChunkDecoder decoder = new TrafficCaptureInterceptor.Utf8ChunkDecoder();
    byte[] euro = "\u20ac".getBytes(UTF_8); // 3 bytes

    decoder.decode(new byte[]{'a', 'b', euro[0]}); // the euro sign's first byte is held back
    assertEquals(2, decoder.decodedBytes());
    decoder.decode(new byte[]{euro[1], euro[2], 'c'});
    assertEquals(6, decoder.decodedBytes());
    decoder.flush();
    assertEquals(6, decoder.decodedBytes());
  }
}
