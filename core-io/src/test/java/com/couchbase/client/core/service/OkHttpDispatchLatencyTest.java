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
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.core.service.OkHttpTestSupport.newTestOkHttpClient;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * The dispatch latency of a request sent by an OkHttp-based service.
 */
class OkHttpDispatchLatencyTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final Duration HANDSHAKE_DELAY = Duration.ofMillis(200);
  private static final String LOOPBACK_IP = "127.0.0.1";

  private CoreEnvironment env;
  private CouchbaseOkHttpClient okHttpClient;

  @AfterEach
  void tearDown() {
    try {
      if (okHttpClient != null) okHttpClient.close();
    } finally {
      if (env != null) env.shutdown();
    }
  }

  @Test
  void dispatchLatencyExcludesConnecting() throws Exception {
    HeldCertificate serverCert = new HeldCertificate.Builder().addSubjectAlternativeName(LOOPBACK_IP).build();
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(serverCert).build();

    env = CoreEnvironment.builder()
      .securityConfig(it -> it.enableTls(true).trustCertificates(listOf(serverCert.certificate())))
      .build();
    okHttpClient = newTestOkHttpClient(env);
    Core core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);

    // A server that's slow to do the TLS handshake, then answers the request right away.
    ServerSocket serverSocket = serverCerts.sslContext().getServerSocketFactory()
      .createServerSocket(0, 50, InetAddress.getLoopbackAddress());
    AtomicLong handshakeDelayNanos = new AtomicLong();

    try (ServerSocket server = serve((SSLServerSocket) serverSocket, socket -> {
      long delayStart = System.nanoTime();
      try {
        Thread.sleep(HANDSHAKE_DELAY.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      handshakeDelayNanos.set(System.nanoTime() - delayStart);
      ((SSLSocket) socket).startHandshake();

      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), UTF_8));
      String line;
      do {
        line = in.readLine(); // the request line and headers; a GET has no body
      } while (line != null && !line.isEmpty());

      socket.getOutputStream().write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".getBytes(UTF_8));
    })) {
      CoreCommonOptions options = CoreCommonOptions.of(TIMEOUT, null, null);
      CoreHttpRequest request = CoreHttpRequest.builder(options, core.context(), HttpMethod.GET, CoreHttpPath.path("/pools"), RequestTarget.manager())
        .build();
      long requestStart = System.nanoTime();
      new OkHttpManagerService(core.context(), new HostAndPort(LOOPBACK_IP, server.getLocalPort())).send(request);
      request.response().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      long requestNanos = System.nanoTime() - requestStart;

      long dispatchLatency = request.context().dispatchLatency();
      assertTrue(dispatchLatency > 0, "dispatch latency should be recorded");

      // Compares durations measured with the same clock, instead of comparing with a fixed limit,
      // so a slow machine doesn't make the test fail. The handshake delay and the dispatch are both
      // part of the request, and don't overlap: the client can't start writing the request until the
      // handshake is done, and the server doesn't start the handshake until its delay is over.
      // So if the dispatch latency excludes the handshake, the two add up to no more than the whole request.
      assertTrue(
        dispatchLatency + handshakeDelayNanos.get() <= requestNanos,
        "dispatch latency should exclude the TLS handshake, but dispatch latency was " + Duration.ofNanos(dispatchLatency)
          + ", handshake delay was " + Duration.ofNanos(handshakeDelayNanos.get())
          + ", and the whole request took " + Duration.ofNanos(requestNanos)
      );
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
    }, "dispatch-latency-test-server");
    acceptor.setDaemon(true);
    acceptor.start();
    return serverSocket;
  }
}
