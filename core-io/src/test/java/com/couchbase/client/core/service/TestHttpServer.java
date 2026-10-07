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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.jspecify.annotations.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * A minimal HTTP(S) server for tests, built on the JDK's {@code com.sun.net.httpserver}.
 * <p>
 * Used instead of OkHttp's MockWebServer, which depends on OkHttp itself. Once OkHttp is shaded,
 * MockWebServer's (unshaded) OkHttp types would not match the shaded ones our code uses.
 * This server talks to clients only over sockets, so it works with any client.
 * <p>
 * Responses are served in the order they were enqueued. A request that arrives
 * when the queue is empty gets a 404, so a test that forgets to enqueue a response fails loudly.
 */
final class TestHttpServer implements Closeable {

  /**
   * The host name clients should use. It resolves to the loopback address the server listens on,
   * and possibly to other loopback addresses too (for example, both 127.0.0.1 and ::1).
   * That's intentional: it exercises the client's handling of hosts with several addresses.
   */
  static final String HOST_NAME = "localhost";

  private static final class Canned {
    final int code;
    final byte[] body;
    final Duration headersDelay;
    final Map<String, String> headers;
    final Duration holdOpen;

    Canned(int code, String body, Duration headersDelay) {
      this(code, body, headersDelay, emptyMap(), Duration.ZERO);
    }

    Canned(int code, String body, Duration headersDelay, Map<String, String> headers, Duration holdOpen) {
      this.code = code;
      this.body = body.getBytes(UTF_8);
      this.headersDelay = requireNonNull(headersDelay);
      this.headers = requireNonNull(headers);
      this.holdOpen = requireNonNull(holdOpen);
    }
  }

  private final Queue<Canned> responses = new ConcurrentLinkedQueue<>();
  private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
    Thread t = new Thread(r, "test-http-server");
    t.setDaemon(true);
    return t;
  });
  private final HttpServer server;

  private TestHttpServer(HttpServer server) {
    this.server = server;
    server.setExecutor(executor); // so concurrent requests are served concurrently
    server.createContext("/", this::handle);
    server.start();
  }

  static TestHttpServer startHttp() throws IOException {
    return new TestHttpServer(HttpServer.create(loopback(0), 0));
  }

  static TestHttpServer startHttps(SSLContext sslContext) throws IOException {
    return startHttps(sslContext, false);
  }

  /**
   * @param requireClientCertificate if true, the TLS handshake fails unless the client presents
   * a certificate the server's SSLContext trusts.
   */
  static TestHttpServer startHttps(SSLContext sslContext, boolean requireClientCertificate) throws IOException {
    HttpsServer server = HttpsServer.create(loopback(0), 0);
    server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
      @Override
      public void configure(HttpsParameters params) {
        SSLParameters sslParameters = getSSLContext().getDefaultSSLParameters();
        sslParameters.setNeedClientAuth(requireClientCertificate);
        params.setSSLParameters(sslParameters);
      }
    });
    return new TestHttpServer(server);
  }

  /**
   * TLS details of a request the server received.
   */
  static final class TlsInfo {
    /**
     * The certificate the client presented, or null if none.
     */
    final @Nullable X509Certificate clientCertificate;

    /**
     * Identifies the TLS session. A connection that resumed a session has the same ID as the
     * connection that established it. Requests on the same connection share the session too.
     */
    final String sessionId;

    TlsInfo(@Nullable X509Certificate clientCertificate, String sessionId) {
      this.clientCertificate = clientCertificate;
      this.sessionId = requireNonNull(sessionId);
    }

    @Override
    public String toString() {
      return "TlsInfo{" +
        "clientCertificate=" + (clientCertificate == null ? null : clientCertificate.getSubjectX500Principal()) +
        ", sessionId=" + sessionId +
        '}';
    }
  }

  private final Queue<TlsInfo> tlsInfo = new ConcurrentLinkedQueue<>();

  /**
   * A request the server received.
   */
  static final class RecordedRequest {
    final String method;
    final String path; // including the query string, if any
    final Map<String, List<String>> headers; // keys in lower case
    final byte[] body;

    RecordedRequest(String method, String path, Map<String, List<String>> headers, byte[] body) {
      this.method = requireNonNull(method);
      this.path = requireNonNull(path);
      this.headers = requireNonNull(headers);
      this.body = requireNonNull(body);
    }

    /**
     * Returns the first value of the header, or null if absent.
     */
    @Nullable String header(String name) {
      List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
      return values == null || values.isEmpty() ? null : values.get(0);
    }

    String bodyAsString() {
      return new String(body, UTF_8);
    }

    @Override
    public String toString() {
      return method + " " + path;
    }
  }

  private final Queue<RecordedRequest> requests = new ConcurrentLinkedQueue<>();

  /**
   * Returns the requests received so far, in the order they arrived.
   */
  List<RecordedRequest> requests() {
    return new ArrayList<>(requests);
  }

  /**
   * Returns TLS details for each HTTPS request received so far, in the order they arrived.
   */
  List<TlsInfo> tlsInfo() {
    return new ArrayList<>(tlsInfo);
  }

  private void recordTlsInfo(HttpExchange ex) {
    if (!(ex instanceof HttpsExchange)) {
      return;
    }
    SSLSession session = ((HttpsExchange) ex).getSSLSession();
    X509Certificate clientCert = null;
    try {
      Certificate[] peerCerts = session.getPeerCertificates();
      if (peerCerts.length > 0 && peerCerts[0] instanceof X509Certificate) {
        clientCert = (X509Certificate) peerCerts[0];
      }
    } catch (SSLPeerUnverifiedException e) {
      // client didn't present a certificate
    }
    StringBuilder id = new StringBuilder();
    for (byte b : session.getId()) {
      id.append(String.format("%02x", b));
    }
    tlsInfo.add(new TlsInfo(clientCert, id.toString()));
  }

  /**
   * Returns a port nothing is listening on (at the moment, anyway).
   */
  static int unusedPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }

  int port() {
    return server.getAddress().getPort();
  }

  void enqueue(String body) {
    enqueue(200, body, Duration.ZERO);
  }

  void enqueue(String body, Duration headersDelay) {
    enqueue(200, body, headersDelay);
  }

  void enqueue(int code, String body, Duration headersDelay) {
    responses.add(new Canned(code, body, headersDelay));
  }

  void enqueue(int code, String body, Map<String, String> headers) {
    responses.add(new Canned(code, body, Duration.ZERO, headers, Duration.ZERO));
  }

  /**
   * Enqueues a 200 response with a chunked body that stays open (without sending anything more)
   * for the given time after the body is sent, like a stream that has gone quiet.
   */
  void enqueueStreaming(String body, Duration holdOpen) {
    responses.add(new Canned(200, body, Duration.ZERO, emptyMap(), holdOpen));
  }

  private void handle(HttpExchange ex) throws IOException {
    // Not try-with-resources: HttpExchange isn't AutoCloseable until Java 11.
    try {
      recordRequest(ex, readFully(ex.getRequestBody()));
      recordTlsInfo(ex);

      Canned response = responses.poll();
      if (response == null) {
        response = new Canned(404, "No response enqueued", Duration.ZERO);
      }

      if (!response.headersDelay.isZero()) {
        try {
          Thread.sleep(response.headersDelay.toMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return; // server is shutting down
        }
      }

      response.headers.forEach((name, value) -> ex.getResponseHeaders().set(name, value));

      if (!response.holdOpen.isZero()) {
        ex.sendResponseHeaders(response.code, 0); // chunked
        try (OutputStream os = ex.getResponseBody()) {
          os.write(response.body);
          os.flush();
          try {
            Thread.sleep(response.holdOpen.toMillis());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // server is shutting down
          }
        }
        return;
      }

      // A fixed content length lets the client reuse the connection (HTTP/1.1 keep-alive).
      ex.sendResponseHeaders(response.code, response.body.length);
      try (OutputStream os = ex.getResponseBody()) {
        os.write(response.body);
      }
    } finally {
      ex.close();
    }
  }

  private void recordRequest(HttpExchange ex, byte[] body) {
    Map<String, List<String>> headers = new HashMap<>();
    ex.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(Locale.ROOT), new ArrayList<>(values)));
    requests.add(new RecordedRequest(ex.getRequestMethod(), ex.getRequestURI().toString(), headers, body));
  }

  private static byte[] readFully(@Nullable InputStream is) throws IOException {
    if (is == null) {
      return new byte[0];
    }
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int n;
    while ((n = is.read(buffer)) != -1) {
      result.write(buffer, 0, n);
    }
    return result.toByteArray();
  }

  private static InetSocketAddress loopback(int port) {
    return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow(); // interrupts handlers sleeping in a headers delay
  }
}
