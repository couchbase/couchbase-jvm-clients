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

package com.couchbase.client.core.cnc.apptelemetry.reporter;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.service.CouchbaseOkHttpClient;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Connects {@link OkHttpAppTelemetryWebSocketClient} to a {@link TestWebSocketServer},
 * which plays the part of the app telemetry server.
 */
class OkHttpAppTelemetryWebSocketClientTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String PATH = "/_appTelemetry";
  private static final String REPORT = "sdk_http_requests{agent=\"test\"} 1\n";

  private @Nullable CoreEnvironment env;
  private @Nullable CouchbaseOkHttpClient okHttpClient;
  private @Nullable TestWebSocketServer server;

  /**
   * The server's end of the client's connection, once the handshake is complete.
   */
  private final CompletableFuture<TestWebSocketServer.Connection> serverConnection = new CompletableFuture<>();

  @AfterEach
  void teardown() throws Exception {
    try {
      if (okHttpClient != null) okHttpClient.close();
    } finally {
      try {
        if (server != null) server.close();
      } finally {
        if (env != null) env.shutdown();
      }
    }
  }

  @Test
  void answersRequestsUntilServerCloses() throws Exception {
    URI uri = startServer(connection -> {
      connection.sendBinary(new byte[]{0}); // GET_TELEMETRY
      connection.sendBinary(new byte[]{42}); // unrecognized
      // The test closes the connection once both answers have arrived.
    });
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    AtomicBoolean connected = new AtomicBoolean();
    CompletableFuture<Void> result = connectInBackground(client, uri, () -> connected.set(true));

    assertEquals((char) 0 + REPORT, new String(nextMessage(), UTF_8));
    assertEquals("\u0001", new String(nextMessage(), UTF_8));
    assertTrue(connected.get());
    assertTrue(serverConnection().authorizationHeader().startsWith("Basic "));

    closeFromServer();
    result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // returns normally
  }

  @Test
  void sendsNoCredentialsToUserSpecifiedEndpoint() throws Exception {
    URI uri = startServer(connection -> connection.sendBinary(new byte[]{0}));
    OkHttpAppTelemetryWebSocketClient client = newClient(uri);

    CompletableFuture<Void> result = connectInBackground(client, uri, () -> {
    });

    assertEquals((char) 0 + REPORT, new String(nextMessage(), UTF_8));
    assertNull(serverConnection().authorizationHeader());

    closeFromServer();
    result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  void returnsWhenServerDropsConnectionWithoutCloseFrame() throws Exception {
    URI uri = startServer(connection -> connection.sendBinary(new byte[]{0}));
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    CompletableFuture<Void> result = connectInBackground(client, uri, () -> {
    });
    nextMessage(); // connected

    serverConnection().drop();
    result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // returns normally
  }

  @Test
  void interruptClosesConnection() throws Exception {
    URI uri = startServer(connection -> connection.sendBinary(new byte[]{0}));
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    CompletableFuture<Void> result = connectInBackground(client, uri, () -> {
    });
    nextMessage(); // connected

    workerThread.get().interrupt();

    Exception e = assertThrows(Exception.class, () -> result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    assertInstanceOf(InterruptedException.class, e.getCause());
    serverConnection().closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  void connectionFailureThrows() throws Exception {
    int unusedPort;
    try (ServerSocket s = new ServerSocket(0)) {
      unusedPort = s.getLocalPort();
    }
    URI uri = URI.create("ws://127.0.0.1:" + unusedPort + PATH);
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    AtomicBoolean connected = new AtomicBoolean();
    CompletableFuture<Void> result = connectInBackground(client, uri, () -> connected.set(true));

    Exception e = assertThrows(Exception.class, () -> result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    assertInstanceOf(RuntimeException.class, e.getCause());
    assertFalse(connected.get());
  }

  private final AtomicReference<Thread> workerThread = new AtomicReference<>();

  private CompletableFuture<Void> connectInBackground(AppTelemetryClient client, URI uri, Runnable onConnected) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    Thread t = new Thread(() -> {
      try {
        client.connectAndWaitForClose(uri, onConnected);
        result.complete(null);
      } catch (Throwable e) {
        result.completeExceptionally(e);
      }
    });
    workerThread.set(t);
    t.start();
    return result;
  }

  private TestWebSocketServer.Connection serverConnection() throws Exception {
    return serverConnection.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  private byte[] nextMessage() throws Exception {
    byte[] message = serverConnection().receivedMessages.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertNotNull(message, "timed out waiting for message from client");
    return message;
  }

  private void closeFromServer() throws Exception {
    serverConnection().sendClose(1000, "bye");
  }

  /**
   * @param userSpecifiedEndpoint if non-null, the environment's app telemetry endpoint
   */
  private OkHttpAppTelemetryWebSocketClient newClient(@Nullable URI userSpecifiedEndpoint) {
    env = CoreEnvironment.builder()
      .appTelemetryEndpoint(userSpecifiedEndpoint == null ? null : userSpecifiedEndpoint.toString())
      .build();

    okHttpClient = new CouchbaseOkHttpClient(
      Duration.ofSeconds(5),
      env.ioConfig(),
      env.ioEnvironment().nativeIoEnabled(),
      env.securityConfig(),
      PasswordAuthenticator.create("username", "password"),
      "test-user-agent"
    );

    Core core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);

    AppTelemetryCollector collector = mock(AppTelemetryCollector.class);
    doAnswer(invocation -> {
      Consumer<? super CharSequence> sink = invocation.getArgument(0);
      sink.accept(REPORT);
      return null;
    }).when(collector).reportTo(any());

    return new OkHttpAppTelemetryWebSocketClient(core.context(), collector);
  }

  /**
   * Starts a WebSocket server, which calls the given callback once the handshake is complete.
   */
  private URI startServer(Consumer<TestWebSocketServer.Connection> onHandshakeComplete) throws Exception {
    server = new TestWebSocketServer(PATH, connection -> {
      serverConnection.complete(connection);
      onHandshakeComplete.accept(connection);
    });
    return server.uri();
  }
}
