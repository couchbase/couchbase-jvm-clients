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
import com.couchbase.client.core.cnc.events.core.WatchdogInvalidStateIdentifiedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointDisconnectedEvent;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.MockUtil;
import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.Request;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class NodeConnectionTrackerTest {

  private static final Duration FAILURE_VISIBILITY = Duration.ofMinutes(1);
  private static final InetSocketAddress NODE_ADDR = InetSocketAddress.createUnresolved("node1", 8093);

  private final AtomicLong clock = new AtomicLong(1_000_000);
  private final AtomicReference<CircuitBreaker.State> cbState = new AtomicReference<>(CircuitBreaker.State.CLOSED);

  private final NodeConnectionTracker tracker = new NodeConnectionTracker(
    ServiceType.QUERY,
    new HostAndPort("node1", 8093),
    null,
    cbState::get,
    FAILURE_VISIBILITY,
    clock::get,
    null // no connection events (see publishingTracker)
  );

  private static Call newCall() {
    return newCall(new Request.Builder().url("http://node1:8093/").build());
  }

  private static Call newCall(RequestContext requestContext) {
    return newCall(new Request.Builder()
      .url("http://node1:8093/")
      .tag(RequestContext.class, requestContext)
      .build());
  }

  private static Call newCall(Request request) {
    Call call = mock(Call.class);
    when(call.isCanceled()).thenReturn(false);
    when(call.request()).thenReturn(request);
    return call;
  }

  private static Connection newConnection(Socket socket) {
    Connection connection = mock(Connection.class);
    when(connection.socket()).thenReturn(socket);
    return connection;
  }

  private static Socket newSocket(int localPort) {
    Socket socket = mock(Socket.class);
    when(socket.isClosed()).thenReturn(false);
    when(socket.getLocalSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.1", localPort));
    when(socket.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("127.0.0.2", 8093));
    return socket;
  }

  /**
   * Simulates a call whose only connection attempt fails.
   */
  private void failToConnect(IOException e) {
    Call call = newCall();
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, e);
    tracker.callFailed(call, e);
  }

  private void advance(Duration d) {
    clock.addAndGet(d.toNanos());
  }

  private EndpointDiagnostics only(List<EndpointDiagnostics> diagnostics) {
    assertEquals(1, diagnostics.size(), "expected exactly one entry, but got: " + diagnostics);
    return diagnostics.get(0);
  }

  @Test
  void idleNodeReportsNothing() {
    assertTrue(tracker.diagnostics().isEmpty());
    assertTrue(tracker.internalDiagnostics().isEmpty());
  }

  @Test
  void reportsConnectingWhileConnectInProgress() {
    Call call = newCall();
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.CONNECTING, d.state());
    assertEquals(ServiceType.QUERY, d.type());
    assertEquals(CircuitBreaker.State.CLOSED, d.circuitBreakerState());
    assertNull(d.local());
  }

  @Test
  void reportsConnectedConnection() {
    Call call = newCall();
    Connection connection = newConnection(newSocket(50000));

    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectEnd(call, NODE_ADDR, Proxy.NO_PROXY, null);
    tracker.connectionAcquired(call, connection);

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.CONNECTED, d.state());
    assertTrue(d.local().endsWith(":50000"), d.local());
    assertTrue(d.remote().endsWith(":8093"), d.remote());
    assertTrue(d.id().isPresent());
    assertFalse(d.lastActivity().isPresent(), "no activity until the connection is released");
    assertFalse(d.lastConnectAttemptFailure().isPresent());
  }

  @Test
  void reportsLastActivity() {
    Call call = newCall();
    Connection connection = newConnection(newSocket(50000));
    tracker.connectionAcquired(call, connection);
    tracker.connectionReleased(call, connection);
    advance(Duration.ofMillis(5));

    Duration lastActivity = only(tracker.diagnostics()).lastActivity().orElseThrow(AssertionError::new);
    assertEquals(TimeUnit.MILLISECONDS.toMicros(5), TimeUnit.NANOSECONDS.toMicros(lastActivity.toNanos()));
  }

  @Test
  void reusedConnectionReportedOnce() {
    Connection connection = newConnection(newSocket(50000));
    tracker.connectionAcquired(newCall(), connection);
    tracker.connectionAcquired(newCall(), connection);

    only(tracker.diagnostics());
  }

  @Test
  void reportsEachConnection() {
    tracker.connectionAcquired(newCall(), newConnection(newSocket(50000)));
    tracker.connectionAcquired(newCall(), newConnection(newSocket(50001)));

    assertEquals(2, tracker.diagnostics().size());
  }

  @Test
  void closedConnectionIsDropped() {
    Socket socket = newSocket(50000);
    tracker.connectionAcquired(newCall(), newConnection(socket));
    only(tracker.diagnostics());

    when(socket.isClosed()).thenReturn(true); // evicted from the pool
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void connectFailureShowsPlaceholder() {
    Call call = newCall();
    ConnectException refused = new ConnectException("Connection refused");
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, refused);
    tracker.callFailed(call, refused);

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.DISCONNECTED, d.state());
    assertTrue(d.lastConnectAttemptFailure().orElseThrow(AssertionError::new).getMessage().startsWith("Connection refused - Check server ports"));
    assertTrue(d.remote().contains("node1"), d.remote());
  }

  @Test
  void connectFailurePlaceholderExpires() {
    failToConnect(new ConnectException("Connection refused"));
    only(tracker.diagnostics());

    advance(FAILURE_VISIBILITY);
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void successfulConnectClearsFailure() {
    failToConnect(new ConnectException("Connection refused"));

    Call call = newCall();
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectEnd(call, NODE_ADDR, Proxy.NO_PROXY, null);
    tracker.callEnd(call);

    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void cancelledConnectIsNotAFailure() {
    Call call = newCall();
    when(call.isCanceled()).thenReturn(true);
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, new IOException("Canceled"));
    tracker.callFailed(call, new IOException("Canceled"));

    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void failedAttemptIsNotRecordedUntilCallFails() {
    Call call = newCall();
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, new ConnectException("Connection refused"));

    // OkHttp might still try another address.
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void failedAttemptFollowedBySuccessIsNotAFailure() {
    // OkHttp tries the next address after the first one fails.
    Call call = newCall();
    InetSocketAddress other = InetSocketAddress.createUnresolved("node1-alt", 8093);
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, new ConnectException("Connection refused"));
    tracker.connectStart(call, other, Proxy.NO_PROXY);
    tracker.connectEnd(call, other, Proxy.NO_PROXY, null);
    Socket socket = newSocket(50000);
    tracker.connectionAcquired(call, newConnection(socket));
    tracker.callEnd(call);

    when(socket.isClosed()).thenReturn(true); // connection later idles out
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void fastFallbackLoserFailingAfterWinnerIsNotAFailure() {
    Call call = newCall();
    InetSocketAddress other = InetSocketAddress.createUnresolved("node1-ipv6", 8093);
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectStart(call, other, Proxy.NO_PROXY);
    tracker.connectEnd(call, NODE_ADDR, Proxy.NO_PROXY, null);
    Socket socket = newSocket(50000);
    tracker.connectionAcquired(call, newConnection(socket));
    tracker.connectFailed(call, other, Proxy.NO_PROXY, null, new IOException("Socket closed")); // the loser
    tracker.callEnd(call);

    when(socket.isClosed()).thenReturn(true); // connection later idles out
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void reusingPooledConnectionClearsFailure() {
    Connection pooled = newConnection(newSocket(50000));
    tracker.connectionAcquired(newCall(), pooled);

    failToConnect(new ConnectException("Connection refused")); // e.g. a blip while the node restarted

    // A later call reuses the pooled connection; no new connection is established.
    Call call = newCall();
    tracker.connectionAcquired(call, pooled);
    tracker.callEnd(call);

    when(pooled.socket().isClosed()).thenReturn(true); // connection later idles out
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void failureAfterAcquiringConnectionIsNotAConnectFailure() {
    Call call = newCall();
    Socket socket = newSocket(50000);
    tracker.connectionAcquired(call, newConnection(socket));
    tracker.callFailed(call, new IOException("unexpected end of stream"));

    when(socket.isClosed()).thenReturn(true);
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void connectFailureAfterConnectionsIdleOut() {
    Socket socket = newSocket(50000);
    tracker.connectionAcquired(newCall(), newConnection(socket));
    when(socket.isClosed()).thenReturn(true);

    ConnectException refused = new ConnectException("Connection refused");
    failToConnect(refused);

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.DISCONNECTED, d.state());
    assertTrue(d.lastConnectAttemptFailure().orElseThrow(AssertionError::new).getMessage().startsWith("Connection refused - Check server ports"));
  }

  @Test
  void tlsFailureOnOneAddressIsReportedEvenIfLastAttemptFailedDifferently() {
    // The node's hostname resolves to two addresses. The TLS handshake fails on the first,
    // and the node isn't listening on the second. OkHttp throws the last exception.
    Call call = newCall();
    InetSocketAddress other = InetSocketAddress.createUnresolved("node1-ipv6", 8093);
    SSLHandshakeException tlsFailure = new SSLHandshakeException("PKIX path building failed");
    ConnectException refused = new ConnectException("Connection refused");

    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, tlsFailure);
    tracker.connectStart(call, other, Proxy.NO_PROXY);
    tracker.connectFailed(call, other, Proxy.NO_PROXY, null, refused);
    tracker.callFailed(call, refused);

    assertSame(tlsFailure, only(tracker.diagnostics()).lastConnectAttemptFailure().orElseThrow(AssertionError::new));
    assertSame(tlsFailure, tracker.internalDiagnostics().get(0).tlsHandshakeFailure);
  }

  @Test
  void tlsFailureOnOneAddressIsIgnoredIfAnotherSucceeds() {
    Call call = newCall();
    InetSocketAddress other = InetSocketAddress.createUnresolved("node1-ipv6", 8093);
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, new SSLHandshakeException("nope"));
    tracker.connectStart(call, other, Proxy.NO_PROXY);
    tracker.connectEnd(call, other, Proxy.NO_PROXY, null);
    Socket socket = newSocket(50000);
    tracker.connectionAcquired(call, newConnection(socket));
    tracker.callEnd(call);

    when(socket.isClosed()).thenReturn(true);
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void placeholderUsesSameAddressFormatAsOtherEntries() {
    cbState.set(CircuitBreaker.State.OPEN);
    assertEquals("node1:8093", only(tracker.diagnostics()).remote());
  }

  @Test
  void recordsDispatchDetailsOnRequestContext() {
    RequestContext requestContext = mock(RequestContext.class);
    tracker.connectionAcquired(newCall(requestContext), newConnection(newSocket(50000)));

    EndpointDiagnostics d = only(tracker.diagnostics());
    verify(requestContext).lastChannelId(d.id().orElseThrow(AssertionError::new));
    verify(requestContext).lastDispatchedFrom(new HostAndPort("127.0.0.1", 50000));
    // The node's configured address, not the socket's resolved remote address (127.0.0.2), as with Netty.
    verify(requestContext).lastDispatchedTo(new HostAndPort("node1", 8093));
  }

  @Test
  void reusedConnectionRecordsSameChannelId() {
    Connection connection = newConnection(newSocket(50000));
    RequestContext first = mock(RequestContext.class);
    RequestContext second = mock(RequestContext.class);
    tracker.connectionAcquired(newCall(first), connection);
    tracker.connectionAcquired(newCall(second), connection);

    String id = only(tracker.diagnostics()).id().orElseThrow(AssertionError::new);
    verify(first).lastChannelId(id);
    verify(second).lastChannelId(id);
  }

  @Test
  void failedConnectDoesNotRecordDispatchDetails() {
    RequestContext requestContext = mock(RequestContext.class);
    Call call = newCall(requestContext);
    ConnectException refused = new ConnectException("Connection refused");
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, refused);
    tracker.callFailed(call, refused);

    // As with Netty, a request that never got a connection was never dispatched.
    verifyNoInteractions(requestContext);
  }

  @Test
  void requestWithoutContextIsFine() {
    tracker.connectionAcquired(newCall(), newConnection(newSocket(50000)));
    only(tracker.diagnostics());
  }

  @Test
  void dnsFailureShowsPlaceholder() {
    Call call = newCall();
    UnknownHostException e = new UnknownHostException("node1");
    tracker.callFailed(call, e);

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.DISCONNECTED, d.state());
    assertSame(e, d.lastConnectAttemptFailure().orElseThrow(AssertionError::new));
  }

  @Test
  void callEndClearsAbandonedConnectAttempts() {
    // Fast fallback: two attempts race, the loser may not report connectFailed.
    Call call = newCall();
    InetSocketAddress other = InetSocketAddress.createUnresolved("node1-ipv6", 8093);
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectStart(call, other, Proxy.NO_PROXY);
    assertEquals(2, tracker.diagnostics().size());

    tracker.connectEnd(call, NODE_ADDR, Proxy.NO_PROXY, null);
    tracker.callEnd(call);
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void openCircuitShowsPlaceholderWhenIdle() {
    cbState.set(CircuitBreaker.State.OPEN);

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.DISCONNECTED, d.state());
    assertEquals(CircuitBreaker.State.OPEN, d.circuitBreakerState());
  }

  @Test
  void connectionsReportCircuitBreakerState() {
    tracker.connectionAcquired(newCall(), newConnection(newSocket(50000)));
    cbState.set(CircuitBreaker.State.HALF_OPEN);

    EndpointDiagnostics d = only(tracker.diagnostics());
    assertEquals(EndpointState.CONNECTED, d.state());
    assertEquals(CircuitBreaker.State.HALF_OPEN, d.circuitBreakerState());
  }

  @Test
  void disabledCircuitBreakerShowsNothingWhenIdle() {
    cbState.set(CircuitBreaker.State.DISABLED);
    assertTrue(tracker.diagnostics().isEmpty());
  }

  @Test
  void internalDiagnosticsReportTlsFailure() {
    SSLHandshakeException tlsFailure = new SSLHandshakeException("PKIX path building failed");
    failToConnect(tlsFailure);

    List<InternalEndpointDiagnostics> internal = tracker.internalDiagnostics();
    assertEquals(1, internal.size());
    assertEquals(AuthenticationStatus.UNKNOWN, internal.get(0).authenticationStatus);
    assertSame(tlsFailure, internal.get(0).tlsHandshakeFailure);
  }

  @Test
  void internalDiagnosticsWithoutTlsFailure() {
    tracker.connectionAcquired(newCall(), newConnection(newSocket(50000)));

    List<InternalEndpointDiagnostics> internal = tracker.internalDiagnostics();
    assertEquals(1, internal.size());
    assertNull(internal.get(0).tlsHandshakeFailure);
  }

  // ---- Connection events ----

  private final SimpleEventBus eventBus = new SimpleEventBus(true);

  /**
   * A tracker that publishes connection events to {@link #eventBus}.
   */
  private final ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);

  private NodeConnectionTracker publishingTracker() {
    return publishingTracker(null);
  }

  /**
   * A tracker that publishes connection events to {@link #eventBus}, whose core has the given cluster config.
   */
  private NodeConnectionTracker publishingTracker(@Nullable ClusterConfig clusterConfig) {
    CoreEnvironment env = mock(CoreEnvironment.class);
    when(env.eventBus()).thenReturn(eventBus);
    Core core = MockUtil.mockCore(env);
    when(core.clusterConfig()).thenReturn(clusterConfig);
    when(core.configurationProvider()).thenReturn(configurationProvider);
    return new NodeConnectionTracker(
      ServiceType.QUERY,
      new HostAndPort("node1", 8093),
      null,
      cbState::get,
      FAILURE_VISIBILITY,
      clock::get,
      core.context()
    );
  }

  private <T extends Event> List<T> events(Class<T> type) {
    return eventBus.publishedEvents().stream()
      .filter(type::isInstance)
      .map(type::cast)
      .collect(Collectors.toList());
  }

  private static void failToConnect(NodeConnectionTracker tracker, Call call, IOException e) {
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    tracker.connectFailed(call, NODE_ADDR, Proxy.NO_PROXY, null, e);
    tracker.callFailed(call, e);
  }

  @Test
  void publishesConnectedEventOncePerNewConnection() {
    NodeConnectionTracker tracker = publishingTracker();
    Call call = newCall();
    tracker.connectStart(call, NODE_ADDR, Proxy.NO_PROXY);
    advance(Duration.ofMillis(5));
    Connection connection = newConnection(newSocket(50000));
    tracker.connectionAcquired(call, connection);
    tracker.connectionAcquired(newCall(), connection); // reused from the pool: not a new connection

    List<EndpointConnectedEvent> events = events(EndpointConnectedEvent.class);
    assertEquals(1, events.size());
    assertEquals(Duration.ofMillis(5), events.get(0).duration());
    EndpointContext context = (EndpointContext) events.get(0).context();
    assertEquals(ServiceType.QUERY, context.serviceType());
    assertTrue(context.channelId().isPresent());
    assertEquals(new HostAndPort("127.0.0.1", 50000), context.localSocket().orElse(null));
  }

  @Test
  void publishesConnectionFailedEventsAtWarnLevelWithAttemptNumbers() {
    NodeConnectionTracker tracker = publishingTracker();
    failToConnect(tracker, newCall(), new ConnectException("Connection refused"));
    failToConnect(tracker, newCall(), new ConnectException("Connection refused"));

    List<EndpointConnectionFailedEvent> events = events(EndpointConnectionFailedEvent.class);
    assertEquals(2, events.size());
    assertEquals(Event.Severity.WARN, events.get(0).severity());
    assertEquals(1, events.get(0).attempt());
    assertEquals(2, events.get(1).attempt());
    assertTrue(events.get(0).description().contains("Connection refused"), events.get(0).description());

    // A connection resets the count, like the Netty endpoints' connect attempts.
    tracker.connectionAcquired(newCall(), newConnection(newSocket(50000)));
    failToConnect(tracker, newCall(), new ConnectException("Connection refused"));
    assertEquals(1, events(EndpointConnectionFailedEvent.class).get(2).attempt());
  }

  private static final String TLS_HINT = " - Check server ports, and whether the cluster requires TLS (for example, use couchbases://).";
  private static final String PORTS_HINT = " - Check server ports.";

  /**
   * Returns a connect failure like OkHttp's, whose message only names the address: the reason is in the cause.
   */
  private static ConnectException okHttpConnectFailure(IOException reason) {
    ConnectException e = new ConnectException("Failed to connect to /127.0.0.1:8093");
    e.initCause(reason);
    return e;
  }

  @Test
  void refusedConnectionWithoutTlsMentionsTls() {
    ConnectException okHttpFailure = okHttpConnectFailure(new ConnectException("Connection refused"));

    IOException annotated = NodeConnectionTracker.annotateConnectException(okHttpFailure, false);
    assertEquals(ConnectException.class, annotated.getClass());
    assertEquals("Failed to connect to /127.0.0.1:8093: Connection refused" + TLS_HINT, annotated.getMessage());
    assertNull(annotated.getCause()); // like the Netty endpoints': one line in the WARN log
    assertEquals(0, annotated.getStackTrace().length);

    assertEquals("Connection refused: connect" + TLS_HINT, // Windows
      NodeConnectionTracker.annotateConnectException(new ConnectException("Connection refused: connect"), false).getMessage());
  }

  @Test
  void refusedConnectionWithTlsDoesNotMentionTls() {
    // The TLS ports are always open: wrong host or port, or the node is down.
    IOException annotated = NodeConnectionTracker.annotateConnectException(
      okHttpConnectFailure(new ConnectException("Connection refused")), true);
    assertEquals("Failed to connect to /127.0.0.1:8093: Connection refused" + PORTS_HINT, annotated.getMessage());
  }

  @Test
  void connectTimeoutDoesNotMentionTls() {
    // A cluster that requires TLS refuses connections; a timeout is more likely a firewall, or the wrong host.
    IOException annotated = NodeConnectionTracker.annotateConnectException(
      okHttpConnectFailure(new SocketTimeoutException("Connect timed out")), false);
    assertEquals("Failed to connect to /127.0.0.1:8093: Connect timed out" + PORTS_HINT, annotated.getMessage());
  }

  @Test
  void otherFailuresAreNotAnnotated() {
    SSLHandshakeException tlsFailure = new SSLHandshakeException("PKIX path building failed");
    assertSame(tlsFailure, NodeConnectionTracker.annotateConnectException(tlsFailure, true));
  }

  @Test
  void connectionFailedEventHasTheHint() {
    NodeConnectionTracker tracker = publishingTracker();
    failToConnect(tracker, newCall(), new ConnectException("Connection refused")); // http://
    String description = events(EndpointConnectionFailedEvent.class).get(0).description();
    assertTrue(description.endsWith("because of ConnectException: Connection refused" + TLS_HINT), description);

    failToConnect(tracker, newCall(new Request.Builder().url("https://node1:18093/").build()), new ConnectException("Connection refused"));
    description = events(EndpointConnectionFailedEvent.class).get(1).description();
    assertTrue(description.endsWith("because of ConnectException: Connection refused" + PORTS_HINT), description);
  }

  @Test
  void connectionFailuresWarnAtMostOncePerInterval() {
    NodeConnectionTracker tracker = publishingTracker();
    Duration interval = NodeConnectionTracker.CONNECT_FAILURE_WARN_INTERVAL;
    Runnable fail = () -> failToConnect(tracker, newCall(), new ConnectException("Connection refused"));

    fail.run(); // the first since the last success
    fail.run();
    advance(interval.minusMillis(1));
    fail.run();
    advance(Duration.ofMillis(1));
    fail.run(); // an interval after the last WARN
    fail.run();

    // Reconnecting starts over.
    tracker.connectionAcquired(newCall(), newConnection(newSocket(50000)));
    fail.run();

    assertEquals(
      Arrays.asList("1 WARN", "2 DEBUG", "3 DEBUG", "4 WARN", "5 DEBUG", "1 WARN"),
      events(EndpointConnectionFailedEvent.class).stream()
        .map(e -> e.attempt() + " " + e.severity())
        .collect(Collectors.toList()));
  }

  @Test
  void connectionFailedEventSaysWhetherTheTargetIsInTheTopology() {
    for (boolean inTopology : new boolean[]{true, false}) {
      eventBus.clear();
      failToConnect(publishingTracker(configWhere(inTopology)), newCall(), new ConnectException("Connection refused"));
      String description = events(EndpointConnectionFailedEvent.class).get(0).description();
      assertTrue(description.endsWith("(targetInCurrentTopology=" + inTopology + ")"), description);
    }
  }

  @Test
  void connectionFailedEventDoesNotSayWhetherTheTargetIsInTheTopologyWithoutAConfig() {
    ClusterConfig noConfigYet = mock(ClusterConfig.class);
    when(noConfigYet.hasClusterOrBucketConfig()).thenReturn(false);
    failToConnect(publishingTracker(noConfigYet), newCall(), new ConnectException("Connection refused"));
    String description = events(EndpointConnectionFailedEvent.class).get(0).description();
    assertFalse(description.contains("targetInCurrentTopology"), description);
  }

  private static ClusterConfig configWhere(boolean targetInTopology) {
    ClusterConfig config = mock(ClusterConfig.class);
    when(config.hasClusterOrBucketConfig()).thenReturn(true);
    when(config.contains(ServiceType.QUERY, new HostAndPort("node1", 8093))).thenReturn(targetInTopology);
    return config;
  }

  @Test
  void connectFailureToTargetNotInTopologyTriggersReconfiguration() {
    NodeConnectionTracker tracker = publishingTracker(configWhere(false));
    Runnable fail = () -> failToConnect(tracker, newCall(), new ConnectException("Connection refused"));

    fail.run();
    verify(configurationProvider, times(1)).republishCurrentConfig();
    assertEquals(1, events(WatchdogInvalidStateIdentifiedEvent.class).size());

    fail.run(); // too soon to ask again
    advance(NodeConnectionTracker.RECONFIGURATION_REQUEST_INTERVAL.minusMillis(1));
    fail.run();
    verify(configurationProvider, times(1)).republishCurrentConfig();

    advance(Duration.ofMillis(1));
    fail.run();
    verify(configurationProvider, times(2)).republishCurrentConfig();
  }

  @Test
  void connectFailureToTargetInTopologyDoesNotTriggerReconfiguration() {
    failToConnect(publishingTracker(configWhere(true)), newCall(), new ConnectException("Connection refused"));
    failToConnect(publishingTracker(null), newCall(), new ConnectException("Connection refused")); // no config yet
    verify(configurationProvider, never()).republishCurrentConfig();
    assertTrue(events(WatchdogInvalidStateIdentifiedEvent.class).isEmpty());
  }

  @Test
  void cancelledCallPublishesNoConnectionFailure() {
    NodeConnectionTracker tracker = publishingTracker();
    Call call = newCall();
    when(call.isCanceled()).thenReturn(true); // by the SDK (timeout, shutdown): says nothing about the node
    failToConnect(tracker, call, new IOException("Canceled"));
    assertTrue(eventBus.publishedEvents().isEmpty(), eventBus.publishedEvents().toString());
  }

  @Test
  void failureOnAConnectionPublishesUnexpectedDisconnectUnlessItTimedOut() {
    NodeConnectionTracker tracker = publishingTracker();
    Connection connection = newConnection(newSocket(50000));

    Call timedOut = newCall();
    tracker.connectionAcquired(timedOut, connection);
    tracker.callFailed(timedOut, new SocketTimeoutException("Read timed out"));
    assertTrue(events(UnexpectedEndpointDisconnectedEvent.class).isEmpty());

    Call broken = newCall();
    tracker.connectionAcquired(broken, connection);
    advance(Duration.ofSeconds(3));
    tracker.callFailed(broken, new IOException("unexpected end of stream"));

    List<UnexpectedEndpointDisconnectedEvent> events = events(UnexpectedEndpointDisconnectedEvent.class);
    assertEquals(1, events.size());
    assertEquals(Event.Severity.WARN, events.get(0).severity());
  }

  @Test
  void closedConnectionPublishesDisconnectedEventOnce() {
    NodeConnectionTracker tracker = publishingTracker();
    Socket socket = newSocket(50000);
    tracker.connectionAcquired(newCall(), newConnection(socket));

    when(socket.isClosed()).thenReturn(true);
    tracker.diagnostics(); // notices it's closed
    tracker.diagnostics();

    assertEquals(1, events(EndpointDisconnectedEvent.class).size());
  }
}
