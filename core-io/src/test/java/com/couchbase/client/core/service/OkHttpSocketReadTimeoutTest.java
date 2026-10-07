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
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Checks that read timeouts are enforced with the socket's SO_TIMEOUT, instead of OkHttp's
 * own read timeout (which uses Okio's watchdog thread).
 */
class OkHttpSocketReadTimeoutTest {

  /**
   * See CouchbaseOkHttpClient.READ_TIMEOUT_GRACE_PERIOD.
   */
  private static final Duration GRACE_PERIOD = Duration.ofSeconds(15);

  private static CoreEnvironment env;

  private TestHttpServer server;
  private CouchbaseOkHttpClient okHttpClient;

  /**
   * Like the client's own, but also records the read timeout of each request's socket
   * (as set by the client's network interceptor, which runs first).
   */
  private OkHttpClient recordingClient;
  private final List<Socket> sockets = new ArrayList<>();
  private final List<Integer> socketReadTimeouts = new ArrayList<>();

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
    recordingClient = okHttpClient.getClientForTest().newBuilder()
      .addNetworkInterceptor(chain -> {
        Socket socket = chain.connection().socket();
        synchronized (sockets) {
          sockets.add(socket);
          socketReadTimeouts.add(socket.getSoTimeout());
        }
        return chain.proceed(chain.request());
      })
      .build();
  }

  @AfterEach
  void teardown() {
    try {
      okHttpClient.close();
    } finally {
      server.close();
    }
  }

  private Response get(Duration requestTimeout) throws IOException {
    return recordingClient.newCall(
      CouchbaseOkHttpClient.newRequest(
        new okhttp3.Request.Builder().url("http://" + TestHttpServer.HOST_NAME + ":" + server.port() + "/"),
        mock(RequestContext.class),
        requestTimeout
      )
    ).execute();
  }

  private static int millis(Duration d) {
    return (int) d.toMillis();
  }

  @Test
  void socketReadTimeoutIsRequestTimeoutPlusGracePeriod() throws Exception {
    server.enqueue("hello");

    try (Response response = get(Duration.ofSeconds(3))) {
      assertEquals("hello", response.body().string());
    }

    assertEquals(millis(Duration.ofSeconds(3).plus(GRACE_PERIOD)), socketReadTimeouts.get(0));
  }

  @Test
  void requestNotFromNewCallIsRejected() {
    // Its socket would have no read timeout.
    server.enqueue("hello");

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> okHttpClient.getClientForTest().newCall(
      new okhttp3.Request.Builder()
        .url("http://" + TestHttpServer.HOST_NAME + ":" + server.port() + "/")
        .build()
    ).execute().close());
    assertTrue(e.getMessage().contains("DispatchState"), e.getMessage());
  }

  @Test
  void doesNotUseOkioTimeouts() throws Exception {
    server.enqueue("hello");

    try (Response response = get(Duration.ofSeconds(3))) {
      // The body's timeout is backed by the socket's Okio timeout, which would wake Okio's watchdog if it were set.
      assertEquals(0, response.body().source().timeout().timeoutNanos());
      assertEquals("hello", response.body().string());
    }
  }

  @Test
  void reusedConnectionGetsTheNextRequestsTimeout() throws Exception {
    server.enqueue("first");
    server.enqueue("second");

    try (Response response = get(Duration.ofSeconds(3))) {
      // Like a streamed response, once it has started.
      CouchbaseOkHttpClient.setSocketReadTimeout(response.request(), Duration.ofMillis(500));
      assertEquals(500, sockets.get(0).getSoTimeout());
      assertEquals("first", response.body().string());
    }

    try (Response response = get(Duration.ofSeconds(4))) {
      assertEquals("second", response.body().string());
    }

    assertSame(sockets.get(0), sockets.get(1), "should have reused the connection");
    assertEquals(millis(Duration.ofSeconds(4).plus(GRACE_PERIOD)), socketReadTimeouts.get(1));
  }

  @Test
  void streamThatGoesQuietTimesOut() throws Exception {
    Core core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);

    Duration streamingReadTimeout = Duration.ofMillis(500);
    OkHttpQueryService service = new OkHttpQueryService(
      QueryServiceConfig.maxEndpoints(4).build(),
      core.context(),
      new HostAndPort(TestHttpServer.HOST_NAME, server.port()),
      streamingReadTimeout
    );

    // The header and first row arrive, then nothing.
    server.enqueueStreaming("{\"requestID\":\"abc-123\",\"results\":[{\"a\":1},", Duration.ofSeconds(30));

    QueryRequest request = new QueryRequest(
      Duration.ofSeconds(10),
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      "SELECT 1",
      "{\"statement\":\"SELECT 1\"}".getBytes(UTF_8),
      true,
      null,
      null,
      null,
      null,
      null,
      false
    );
    service.send(request);
    QueryResponse response = request.response().get(10, TimeUnit.SECONDS);

    long start = System.nanoTime();
    Exception e = assertThrows(Exception.class, () -> response.rows().blockLast(Duration.ofSeconds(10)));
    Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

    assertInstanceOf(DecodingFailureException.class, e);
    assertTrue(hasCause(e, SocketTimeoutException.class), "expected a socket timeout, but got: " + e);
    assertTrue(elapsed.compareTo(Duration.ofSeconds(5)) < 0, "should have timed out after about " + streamingReadTimeout + ", but took " + elapsed);
  }

  @Test
  // Without a handshake timeout, the read would block forever. A blocked socket read ignores interrupts,
  // so the timeout must run the test on a separate thread to be able to fail it.
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void tlsHandshakeTimesOutAfterConnectTimeout() throws Exception {
    // The OS accepts the TCP connection (into the backlog), but nothing ever answers the TLS handshake.
    try (ServerSocket silentServer = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
      CouchbaseOkHttpClient tlsClient = new CouchbaseOkHttpClient(
        Duration.ofMillis(500), // connect timeout, which the TLS handshake also gets
        IoConfig.create(),
        true, // native IO enabled, like the default environment
        SecurityConfig.builder().enableTls(true).build(),
        PasswordAuthenticator.create("username", "password"),
        "test-user-agent"
      );
      try {
        okhttp3.Request.Builder request = new okhttp3.Request.Builder()
          .url("https://127.0.0.1:" + silentServer.getLocalPort() + "/");

        long start = System.nanoTime();
        IOException e = assertThrows(IOException.class, () ->
          tlsClient.newCall(request, mock(RequestContext.class), Duration.ofSeconds(30)).execute().close());
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertTrue(hasCause(e, SocketTimeoutException.class), "expected a socket timeout, but got: " + e);
        assertTrue(elapsed.compareTo(Duration.ofSeconds(5)) < 0, "handshake should have timed out after about 500 ms, but took " + elapsed);
      } finally {
        tlsClient.close();
      }
    }
  }

  private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (type.isInstance(c)) {
        return true;
      }
    }
    return false;
  }
}
