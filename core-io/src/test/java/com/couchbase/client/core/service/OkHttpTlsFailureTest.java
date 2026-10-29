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
import com.couchbase.client.core.deps.okhttp3.tls.HandshakeCertificates;
import com.couchbase.client.core.deps.okhttp3.tls.HeldCertificate;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpMethod;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.service.OkHttpTlsHelper.CERTIFICATE_HINT;
import static com.couchbase.client.core.service.OkHttpTlsHelper.CLIENT_CERTIFICATE_HINT;
import static com.couchbase.client.core.service.OkHttpTlsHelper.NOT_TLS_HINT;
import static com.couchbase.client.core.service.OkHttpTlsHelper.NO_COMMON_PARAMETERS_HINT;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * How the OkHttp-based services handle TLS failures: failures that retrying won't fix (like an untrusted
 * certificate) fail the request, and other TLS failures are retried.
 */
class OkHttpTlsFailureTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String LOOPBACK_IP = "127.0.0.1";

  private CoreEnvironment env;
  private CouchbaseOkHttpClient okHttpClient;
  private Core core;

  /**
   * Uses TLS, trusting only the JDK's and Capella's CA certificates, with the given TLS settings.
   */
  private void start(Consumer<SecurityConfig.Builder> securityCustomizer) {
    env = CoreEnvironment.builder()
      .securityConfig(it -> securityCustomizer.accept(it.enableTls(true)))
      .build();
    okHttpClient = newTestOkHttpClient(env);
    core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);
  }

  private void start() {
    start(security -> {
    });
  }

  @AfterEach
  void tearDown() {
    try {
      if (okHttpClient != null) okHttpClient.close();
    } finally {
      if (env != null) env.shutdown();
    }
  }

  private CoreHttpRequest newRequest() {
    CoreCommonOptions options = CoreCommonOptions.of(TIMEOUT, null, null);
    return CoreHttpRequest.builder(options, core.context(), CoreHttpMethod.GET, CoreHttpPath.path("/pools"), RequestTarget.manager())
      .build();
  }

  private CoreHttpRequest send(int port) {
    CoreHttpRequest request = newRequest();
    new OkHttpManagerService(core.context(), new HostAndPort(LOOPBACK_IP, port)).send(request);
    return request;
  }

  /**
   * Checks the request failed without being retried, and returns the exception it failed with.
   */
  private static AuthenticationFailureException assertFailedWithoutRetry(CoreHttpRequest request) {
    ExecutionException e = assertThrows(ExecutionException.class, () -> request.response().get(TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS));
    assertNull(request.context().retryReasons());
    return assertInstanceOf(AuthenticationFailureException.class, e.getCause());
  }

  private static void assertHint(String expectedHint, Exception e) {
    assertTrue(e.getMessage().contains(expectedHint), "expected the hint \"" + expectedHint + "\" in: " + e.getMessage());
  }

  @Test
  void untrustedServerCertificateFailsTheRequest() throws Exception {
    start();
    HeldCertificate selfSigned = new HeldCertificate.Builder().addSubjectAlternativeName(LOOPBACK_IP).build();
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(selfSigned).build();

    try (TestHttpServer server = TestHttpServer.startHttps(serverCerts.sslContext())) {
      CoreHttpRequest request = send(server.port());
      assertHint(CERTIFICATE_HINT, assertFailedWithoutRetry(request));
    }
  }

  @Test
  void portThatDoesNotUseTlsFailsTheRequest() throws Exception {
    start();
    // Answers the ClientHello as an HTTP server answers a malformed request.
    try (ServerSocket server = serve(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()), socket -> {
      int ignoreLength = socket.getInputStream().read(new byte[16 * 1024]); // the ClientHello
      socket.getOutputStream().write("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".getBytes(UTF_8));
    })) {
      CoreHttpRequest request = send(server.getLocalPort());
      assertHint(NOT_TLS_HINT, assertFailedWithoutRetry(request));
    }
  }

  @Test
  void noCipherSuiteInCommonFailsTheRequest() throws Exception {
    start(security -> security.ciphers(singletonList("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")));

    HeldCertificate serverCert = new HeldCertificate.Builder().rsa2048().addSubjectAlternativeName(LOOPBACK_IP).build();
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(serverCert).build();
    SSLServerSocket serverSocket = (SSLServerSocket) serverCerts.sslContext().getServerSocketFactory()
      .createServerSocket(0, 50, InetAddress.getLoopbackAddress());
    serverSocket.setEnabledProtocols(new String[]{"TLSv1.2"});
    serverSocket.setEnabledCipherSuites(new String[]{"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"});

    // (The JDK's HTTPS server closes the connection without sending the alert, so it can't be used here.)
    try (ServerSocket server = serve(serverSocket, socket -> ((SSLSocket) socket).startHandshake())) {
      CoreHttpRequest request = send(server.getLocalPort());
      assertHint(NO_COMMON_PARAMETERS_HINT, assertFailedWithoutRetry(request));
    }
  }

  @Test
  void rejectedClientCertificateFailsTheRequest() throws Exception {
    start();
    // Like a server that rejects the client's certificate. This one sends the alert in reply to the ClientHello
    // (with TLS 1.3, a real server sends it after the client finishes the handshake, so the client sees it
    // while sending the request or reading the response), then reads everything the client sends, so the client
    // sees the alert, however the timing works out.
    try (ServerSocket server = serve(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()), socket -> {
      InputStream in = socket.getInputStream();
      int ignoreLength = in.read(new byte[16 * 1024]); // the ClientHello
      socket.getOutputStream().write(BAD_CERTIFICATE_ALERT);
      while (in.read() != -1) {
        // until the client closes the connection
      }
    })) {
      CoreHttpRequest request = send(server.getLocalPort());
      assertHint(CLIENT_CERTIFICATE_HINT, assertFailedWithoutRetry(request));
    }
  }

  /**
   * A TLS record with a fatal "bad_certificate" alert.
   */
  private static final byte[] BAD_CERTIFICATE_ALERT = {
    21, // content type: alert
    3, 3, // TLS 1.2 record version
    0, 2, // length
    2, // level: fatal
    42 // description: bad_certificate
  };

  @Test
  void handshakeTheServerAbandonsIsRetried() throws Exception {
    start();
    // Like a server that's restarting: it accepts the connection, then closes it during the handshake.
    try (ServerSocket server = serve(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()), socket -> {
      int ignoreLength = socket.getInputStream().read(new byte[16 * 1024]); // the ClientHello. Read it, so closing doesn't reset the connection.
    })) {
      CoreHttpRequest request = send(server.getLocalPort());

      // Retried (by the real core; the mock doesn't, so the request is left waiting).
      waitUntilCondition(() -> request.context().retryReasons() != null || request.completed());
      assertFalse(request.completed(), "should be retried, not " + request.cancellationReason());
      assertEquals(singleton(RetryReason.ENDPOINT_NOT_AVAILABLE), request.context().retryReasons());
      request.cancel(CancellationReason.STOPPED_LISTENING);
    }
  }

  private interface SocketHandler {
    void handle(Socket socket) throws IOException;
  }

  /**
   * Accepts connections until the server socket is closed, handling each with the handler, then closing it.
   */
  private static ServerSocket serve(ServerSocket serverSocket, SocketHandler handler) {
    Thread acceptor = new Thread(() -> {
      while (!serverSocket.isClosed()) {
        try (Socket socket = serverSocket.accept()) {
          handler.handle(socket);
        } catch (IOException e) {
          // closed
        }
      }
    }, "tls-failure-test-server");
    acceptor.setDaemon(true);
    acceptor.start();
    return serverSocket;
  }
}
