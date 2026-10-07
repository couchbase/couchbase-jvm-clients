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

import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Checks that {@link NodeConnectionTracker} makes sense of the event sequences
 * a real OkHttp client produces, talking to a real server.
 * <p>
 * Uses {@link TestHttpServer} instead of OkHttp's MockWebServer,
 * so this test keeps working once OkHttp is shaded.
 */
class NodeConnectionTrackerIntegrationTest {

  private static final Duration KEEP_ALIVE = Duration.ofMillis(500);
  private static final Duration WAIT = Duration.ofSeconds(10);

  private final List<OkHttpClient> clients = new ArrayList<>();

  // Every server a test starts, so all of them get closed, even if a test starts more than one.
  private final List<TestHttpServer> servers = new ArrayList<>();

  // Tracks connections to the most recently started server.
  private NodeConnectionTracker tracker;

  private TestHttpServer startHttp() throws IOException {
    return started(TestHttpServer.startHttp());
  }

  private TestHttpServer startHttps(HeldCertificate serverCert) throws IOException {
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(serverCert).build();
    return started(TestHttpServer.startHttps(serverCerts.sslContext()));
  }

  private TestHttpServer started(TestHttpServer s) {
    servers.add(s);
    tracker = newTracker(TestHttpServer.HOST_NAME, s.port());
    return s;
  }

  @AfterEach
  void cleanup() {
    try {
      for (OkHttpClient client : clients) {
        client.dispatcher().cancelAll();
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
      }
    } finally {
      // Close the servers even if client cleanup fails.
      servers.forEach(TestHttpServer::close);
    }
  }

  private static HttpUrl url(String scheme, int port) {
    return new HttpUrl.Builder()
      .scheme(scheme)
      .host(TestHttpServer.HOST_NAME)
      .port(port)
      .build();
  }

  private static HeldCertificate serverCertificate() {
    return new HeldCertificate.Builder()
      .addSubjectAlternativeName(TestHttpServer.HOST_NAME)
      .build();
  }

  private static NodeConnectionTracker newTracker(String host, int port) {
    return new NodeConnectionTracker(
      ServiceType.QUERY,
      new HostAndPort(host, port),
      null,
      () -> CircuitBreaker.State.CLOSED,
      null // no connection events
    );
  }

  private OkHttpClient newClient() {
    return newClient(new OkHttpClient.Builder());
  }

  private OkHttpClient newClient(OkHttpClient.Builder builder) {
    OkHttpClient client = builder
      .connectTimeout(Duration.ofSeconds(5))
      .readTimeout(Duration.ofSeconds(5))
      // Short keep-alive, so tests can watch idle connections get evicted.
      .connectionPool(new ConnectionPool(5, KEEP_ALIVE.toMillis(), MILLISECONDS))
      .build();
    clients.add(client);
    return client;
  }

  private Call newCall(OkHttpClient client, HttpUrl url, NodeConnectionTracker tracker) {
    Call call = client.newCall(new Request.Builder().url(url).build());
    call.addEventListener(tracker);
    return call;
  }

  private Call newCall(OkHttpClient client, HttpUrl url, NodeConnectionTracker tracker, RequestContext requestContext) {
    Call call = client.newCall(new Request.Builder()
      .url(url)
      .tag(RequestContext.class, requestContext)
      .build());
    call.addEventListener(tracker);
    return call;
  }

  private static void execute(Call call) throws IOException {
    try (Response response = call.execute()) {
      response.body().string();
    }
  }

  private static List<EndpointState> states(NodeConnectionTracker tracker) {
    return tracker.diagnostics().stream()
      .map(EndpointDiagnostics::state)
      .collect(Collectors.toList());
  }

  private static EndpointDiagnostics only(List<EndpointDiagnostics> diagnostics) {
    assertEquals(1, diagnostics.size(), "expected exactly one entry, but got: " + diagnostics);
    return diagnostics.get(0);
  }

  @Test
  void connectionIsReportedThenEvictedWhenIdle() throws Exception {
    TestHttpServer server = startHttp();
    server.enqueue("ok");
    OkHttpClient client = newClient();

    execute(newCall(client, url("http", server.port()), tracker));

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.CONNECTED, d.state());
    assertTrue(d.remote().endsWith(":" + server.port()), d.remote());
    assertNotNull(d.local());
    assertTrue(d.lastActivity().isPresent(), "connection was released, so there should be activity");
    assertTrue(d.id().isPresent());

    // The pool closes the idle connection after the keep-alive period.
    // An idle node with a closed circuit reports nothing, like an idle Netty pool.
    waitUntilCondition(() -> tracker.diagnostics().isEmpty(), WAIT);
  }

  @Test
  void sequentialRequestsReuseOneConnection() throws Exception {
    TestHttpServer server = startHttp();
    server.enqueue("one");
    server.enqueue("two");
    OkHttpClient client = newClient();

    execute(newCall(client, url("http", server.port()), tracker));
    execute(newCall(client, url("http", server.port()), tracker));

    assertEquals(1, tracker.diagnostics().size());
  }

  @Test
  void concurrentRequestsUseSeparateConnections() throws Exception {
    TestHttpServer server = startHttp();
    server.enqueue("one", Duration.ofMillis(500));
    server.enqueue("two", Duration.ofMillis(500));
    OkHttpClient client = newClient();

    CountDownLatch done = new CountDownLatch(2);
    Callback callback = new Callback() {
      @Override
      public void onResponse(Call call, Response response) throws IOException {
        try (Response r = response) {
          r.body().string();
        } finally {
          done.countDown();
        }
      }

      @Override
      public void onFailure(Call call, IOException e) {
        done.countDown();
      }
    };
    newCall(client, url("http", server.port()), tracker).enqueue(callback);
    newCall(client, url("http", server.port()), tracker).enqueue(callback);

    waitUntilCondition(() -> {
      List<EndpointState> states = states(tracker);
      return states.size() == 2 && states.stream().allMatch(s -> s == EndpointState.CONNECTED);
    }, WAIT);

    assertTrue(done.await(WAIT.toMillis(), MILLISECONDS));
    assertEquals(2, tracker.diagnostics().size());
  }

  @Test
  void refusedConnectionShowsPlaceholder() throws Exception {
    HttpUrl url = url("http", TestHttpServer.unusedPort());

    NodeConnectionTracker deadNodeTracker = newTracker(url.host(), url.port());
    Call call = newCall(newClient(), url, deadNodeTracker);

    IOException e = assertThrows(IOException.class, () -> execute(call));
    assertTrue(hasCause(e, ConnectException.class), "expected connection refused, but got " + e);

    EndpointDiagnostics d = only(deadNodeTracker.diagnostics());
    assertEquals(EndpointState.DISCONNECTED, d.state());
    assertTrue(d.lastConnectAttemptFailure().isPresent());
  }

  @Test
  void untrustedCertificateShowsPlaceholderWithTlsFailure() throws Exception {
    TestHttpServer server = startHttps(serverCertificate());

    // The client uses the default trust store, which doesn't trust the self-signed server certificate.
    Call call = newCall(newClient(), url("https", server.port()), tracker);

    // Don't check the exception type. The server listens on 127.0.0.1, but "localhost" may also
    // resolve to ::1. Then OkHttp tries both addresses, and throws the exception from the last one
    // ("connection refused"), which doesn't include the TLS failure from the first one.
    assertThrows(IOException.class, () -> execute(call));

    // The tracker should still report the TLS failure, since it's the useful one.
    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.DISCONNECTED, d.state());
    Throwable reported = d.lastConnectAttemptFailure().orElseThrow(AssertionError::new);
    assertTrue(hasCause(reported, SSLException.class), "expected TLS failure, but got " + reported);

    InternalEndpointDiagnostics internal = tracker.internalDiagnostics().get(0);
    assertNotNull(internal.tlsHandshakeFailure);
    assertTrue(hasCause(internal.tlsHandshakeFailure, SSLException.class));
  }

  @Test
  void trustedCertificateReportsConnected() throws Exception {
    HeldCertificate serverCert = serverCertificate();
    TestHttpServer server = startHttps(serverCert);
    server.enqueue("ok");

    HandshakeCertificates clientCerts = new HandshakeCertificates.Builder()
      .addTrustedCertificate(serverCert.certificate())
      .build();
    OkHttpClient client = newClient(new OkHttpClient.Builder()
      .sslSocketFactory(clientCerts.sslSocketFactory(), clientCerts.trustManager()));

    execute(newCall(client, url("https", server.port()), tracker));

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.CONNECTED, d.state());
    assertEquals(Optional.empty(), d.lastConnectAttemptFailure());
    assertEquals(null, tracker.internalDiagnostics().get(0).tlsHandshakeFailure);
  }

  @Test
  void recordsDispatchDetailsMatchingDiagnostics() throws Exception {
    TestHttpServer server = startHttp();
    server.enqueue("one");
    server.enqueue("two");
    OkHttpClient client = newClient();

    RequestContext first = mock(RequestContext.class);
    RequestContext second = mock(RequestContext.class);
    execute(newCall(client, url("http", server.port()), tracker, first));
    execute(newCall(client, url("http", server.port()), tracker, second)); // reuses the connection

    EndpointDiagnostics d = only(tracker.diagnostics());
    String id = d.id().orElseThrow(AssertionError::new);

    for (RequestContext ctx : Arrays.asList(first, second)) {
      // Ping reports and tracing read these.
      verify(ctx).lastChannelId(id);

      ArgumentCaptor<HostAndPort> local = ArgumentCaptor.forClass(HostAndPort.class);
      verify(ctx).lastDispatchedFrom(local.capture());
      assertTrue(d.local().endsWith(":" + local.getValue().port()),
        "diagnostics local " + d.local() + " should match dispatched-from " + local.getValue());
    }
  }

  @Test
  void cancelledCallIsNotAConnectFailure() throws Exception {
    // The server never responds in time, so the call is still waiting when cancelled.
    TestHttpServer server = startHttp();
    server.enqueue("late", Duration.ofSeconds(30));
    Call call = newCall(newClient(), url("http", server.port()), tracker);

    CountDownLatch failed = new CountDownLatch(1);
    call.enqueue(new Callback() {
      @Override
      public void onResponse(Call call, Response response) {
        response.close();
      }

      @Override
      public void onFailure(Call call, IOException e) {
        failed.countDown();
      }
    });

    waitUntilCondition(() -> states(tracker).contains(EndpointState.CONNECTED), WAIT);
    call.cancel();
    assertTrue(failed.await(WAIT.toMillis(), MILLISECONDS));

    // Cancelling closes the connection. No placeholder, because the node did nothing wrong.
    waitUntilCondition(() -> tracker.diagnostics().isEmpty(), WAIT);
  }

  @Test
  void noConnectAttemptsLeftPendingAfterCalls() throws Exception {
    TestHttpServer server = startHttp();
    server.enqueue("ok");
    OkHttpClient client = newClient();
    execute(newCall(client, url("http", server.port()), tracker));

    assertTrue(tracker.diagnostics().stream().noneMatch(d -> d.state() == EndpointState.CONNECTING));

    // And after a failed call.
    HttpUrl deadUrl = url("http", TestHttpServer.unusedPort());
    NodeConnectionTracker deadNodeTracker = newTracker(deadUrl.host(), deadUrl.port());
    assertThrows(IOException.class, () -> execute(newCall(client, deadUrl, deadNodeTracker)));

    assertTrue(deadNodeTracker.diagnostics().stream().noneMatch(d -> d.state() == EndpointState.CONNECTING));
  }
}
