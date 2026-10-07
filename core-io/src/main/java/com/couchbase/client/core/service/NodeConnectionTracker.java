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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.core.WatchdogInvalidStateIdentifiedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointDisconnectedEvent;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.EventListener;
import okhttp3.Protocol;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static java.util.Objects.requireNonNull;

/**
 * Tracks the HTTP connections to one node, so a service can report them as endpoint diagnostics.
 * <p>
 * Added to each OkHttp call dispatched to the node, using {@link Call#addEventListener}.
 * <p>
 * Reports:
 * <ul>
 *   <li>One {@link EndpointState#CONNECTED} entry per open connection. Connections the pool has
 *       closed (for example, because they were idle) are dropped, so an idle node reports no
 *       entries, like an idle Netty pool whose endpoints were all removed.
 *   <li>One {@link EndpointState#CONNECTING} entry per connection attempt in progress.
 *   <li>If neither of those exist, one {@link EndpointState#DISCONNECTED} placeholder entry when
 *       there is something worth reporting: the circuit breaker is not closed, or a connection
 *       attempt failed recently. Without it, an idle node with an open circuit would be invisible.
 * </ul>
 * OkHttp's public API has no event for a pooled connection closing, so closed connections
 * are detected by checking the socket whenever diagnostics are requested or a new
 * connection is acquired.
 * <p>
 * A connection failure is recorded only when a call fails without ever acquiring a connection,
 * meaning every attempt to connect failed (or DNS resolution failed). A single failed attempt
 * is not enough: OkHttp may retry another address, and when several addresses are raced
 * ("fast fallback") the losers fail even though the call succeeds. The failure is cleared
 * whenever a connection is established or acquired, since either shows the node is reachable.
 * <p>
 * When a call tries several addresses, OkHttp throws the exception from the <em>last</em>
 * attempt, and earlier failures are not in its cause chain. If any attempt failed during the
 * TLS handshake, that failure is recorded instead, because it is more useful than (say) a
 * "connection refused" from an address the node isn't listening on.
 * <p>
 * If the OkHttp request carries a {@link RequestContext} tag, the tracker also records which
 * connection the request was dispatched on ({@link RequestContext#lastChannelId()},
 * {@link RequestContext#lastDispatchedFrom()} and {@link RequestContext#lastDispatchedTo()}).
 * Ping reports, tracing, and error contexts read these. As with the Netty endpoints, they are set
 * only once the request has a connection, and {@code lastDispatchedTo} is the node's configured
 * address (for example, a host name), not the socket's resolved remote address.
 * <p>
 * If given a {@link CoreContext}, it also publishes the Netty endpoints' connection events:
 * {@link EndpointConnectedEvent} when a new connection is first used, {@link EndpointConnectionFailedEvent}
 * (at warning level) when a call fails to get a connection, {@link UnexpectedEndpointDisconnectedEvent}
 * (at warning level) when a call fails on a connection for a reason other than a timeout, and
 * {@link EndpointDisconnectedEvent} when a closed connection is noticed (by the same checks as above,
 * so not necessarily when it closed).
 * <p>
 * Thread-safe.
 */
@NullMarked
final class NodeConnectionTracker extends EventListener {
  private static final Logger log = LoggerFactory.getLogger(NodeConnectionTracker.class);

  /**
   * How long a failed connection attempt keeps the placeholder entry visible,
   * if no later attempt succeeds and no traffic is sent to the node.
   */
  static final Duration DEFAULT_FAILURE_VISIBILITY = Duration.ofMinutes(5);

  /**
   * While connecting keeps failing, at most one connection failure per interval is published at WARN level;
   * the rest are published at DEBUG level.
   * <p>
   * Every request attempt that can't get a connection fails to connect, and requests are retried, so the
   * failures are as frequent as the retries of all the requests waiting for this node. (The Netty endpoints
   * have one connect attempt at a time, with backoff, however many requests are waiting.)
   */
  static final Duration CONNECT_FAILURE_WARN_INTERVAL = Duration.ofSeconds(10);

  /**
   * How often, at most, a connection failure to a target that isn't in the current cluster config asks the core
   * to reconfigure itself from that config (see {@link #maybeRequestReconfiguration}).
   */
  static final Duration RECONFIGURATION_REQUEST_INTERVAL = Duration.ofSeconds(10);

  private final ServiceType serviceType;
  private final HostAndPort address;
  private final @Nullable String namespace;
  private final Supplier<CircuitBreaker.State> circuitBreakerState;
  private final long failureVisibilityNanos;
  private final LongSupplier nanoClock;

  private final Map<Connection, ConnectionInfo> connections = new ConcurrentHashMap<>();
  private final Map<PendingConnect, Boolean> pendingConnects = new ConcurrentHashMap<>();

  /**
   * State of calls in progress. An entry is removed when the call ends or fails.
   */
  private final Map<Call, CallState> calls = new ConcurrentHashMap<>();

  // Most recent connection failure. Cleared when a connection is established or acquired.
  private volatile @Nullable Failure lastConnectFailure;

  /**
   * For publishing connection events, or null to publish none.
   */
  private final @Nullable CoreContext coreContext;

  /**
   * Calls that failed to get a connection since the last success, like the Netty endpoints' connect attempt number.
   */
  private final AtomicLong consecutiveConnectFailures = new AtomicLong();

  /**
   * When the last connection failure published at WARN level was, if there have been any since the last success.
   */
  private final AtomicLong lastConnectFailureWarnNanos = new AtomicLong();

  /**
   * When this tracker last asked the core to reconfigure itself.
   */
  private final AtomicLong lastReconfigurationRequestNanos;

  /**
   * @param coreContext for publishing connection events, or null to publish none
   */
  NodeConnectionTracker(
    ServiceType serviceType,
    HostAndPort address,
    @Nullable String namespace,
    Supplier<CircuitBreaker.State> circuitBreakerState,
    @Nullable CoreContext coreContext
  ) {
    this(serviceType, address, namespace, circuitBreakerState, DEFAULT_FAILURE_VISIBILITY, System::nanoTime, coreContext);
  }

  /**
   * For tests, which control the clock and how long a connection failure stays visible.
   */
  NodeConnectionTracker(
    ServiceType serviceType,
    HostAndPort address,
    @Nullable String namespace,
    Supplier<CircuitBreaker.State> circuitBreakerState,
    Duration failureVisibility,
    LongSupplier nanoClock,
    @Nullable CoreContext coreContext
  ) {
    this.serviceType = requireNonNull(serviceType);
    this.address = requireNonNull(address);
    this.namespace = namespace;
    this.circuitBreakerState = requireNonNull(circuitBreakerState);
    this.failureVisibilityNanos = failureVisibility.toNanos();
    this.nanoClock = requireNonNull(nanoClock);
    this.coreContext = coreContext;
    this.lastReconfigurationRequestNanos = new AtomicLong(nanoClock.getAsLong() - RECONFIGURATION_REQUEST_INTERVAL.toNanos());
  }

  // ---- EventListener callbacks ----

  @Override
  public void connectStart(Call call, InetSocketAddress inetSocketAddress, Proxy proxy) {
    pendingConnects.put(new PendingConnect(call, inetSocketAddress), Boolean.TRUE);
    CallState state = callState(call);
    if (state.connectStartNanos == 0) {
      state.connectStartNanos = nanoClock.getAsLong(); // the first attempt, if there are several
    }
  }

  @Override
  public void connectEnd(Call call, InetSocketAddress inetSocketAddress, Proxy proxy, @Nullable Protocol protocol) {
    pendingConnects.remove(new PendingConnect(call, inetSocketAddress));
    lastConnectFailure = null;
  }

  @Override
  public void connectFailed(
    Call call,
    InetSocketAddress inetSocketAddress,
    Proxy proxy,
    @Nullable Protocol protocol,
    IOException ioe
  ) {
    // Don't record the failure yet. OkHttp might try another address, or this might be
    // the loser of a fast fallback race. See callFailed.
    pendingConnects.remove(new PendingConnect(call, inetSocketAddress));

    if (hasCause(ioe, SSLException.class)) {
      // Remember it, because OkHttp won't include it in the exception it throws
      // if a later attempt (to another address) fails differently.
      callState(call).tlsFailure = ioe;
    }
  }

  @Override
  public void connectionAcquired(Call call, Connection connection) {
    CallState state = callState(call);
    state.acquiredConnection = true;
    lastConnectFailure = null;
    consecutiveConnectFailures.set(0);

    pruneClosedConnections();
    boolean[] isNew = {false};
    ConnectionInfo info = connections.computeIfAbsent(connection, c -> {
      isNew[0] = true;
      return new ConnectionInfo(c, nanoClock.getAsLong());
    });
    state.connection = info;
    if (isNew[0]) {
      long start = state.connectStartNanos;
      Duration duration = start == 0 ? Duration.ZERO : Duration.ofNanos(nanoClock.getAsLong() - start);
      publish(context -> new EndpointConnectedEvent(duration, endpointContext(context, info), new TreeMap<>()));
    }

    RequestContext requestContext = call.request().tag(RequestContext.class);
    if (requestContext != null) {
      requestContext.lastChannelId(info.id);
      requestContext.lastDispatchedFrom(info.localAddress);
      requestContext.lastDispatchedTo(address);
    }
  }

  @Override
  public void connectionReleased(Call call, Connection connection) {
    ConnectionInfo info = connections.get(connection);
    if (info != null) {
      info.lastActivityNanos = nanoClock.getAsLong();
    }
  }

  @Override
  public void callEnd(Call call) {
    calls.remove(call);
    removePendingConnects(call);
  }

  @Override
  public void callFailed(Call call, IOException ioe) {
    CallState state = calls.remove(call);
    removePendingConnects(call);

    // If the call never got a connection, it failed while resolving the address,
    // connecting, or doing the TLS handshake.
    if (state == null || !state.acquiredConnection) {
      IOException tlsFailure = state == null ? null : state.tlsFailure;
      IOException cause = annotateConnectException(tlsFailure != null ? tlsFailure : ioe, call.request().isHttps());
      recordConnectFailure(call, cause);

      if (!call.isCanceled()) {
        long attempt = consecutiveConnectFailures.incrementAndGet();
        long now = nanoClock.getAsLong();
        long start = state == null ? 0 : state.connectStartNanos;
        Duration duration = start == 0 ? Duration.ZERO : Duration.ofNanos(now - start);
        // The attempt number says how many failures there have been since the last WARN, too.
        Event.Severity severity = shouldWarnOfConnectFailure(attempt, now) ? Event.Severity.WARN : Event.Severity.DEBUG;
        CoreContext coreContext = this.coreContext;
        Boolean targetInTopology = coreContext == null ? null : isTargetInTopology(coreContext);
        publish(context -> new EndpointConnectionFailedEvent(
          severity, duration, endpointContext(context, null), attempt, cause, targetInTopology));
        if (coreContext != null && Boolean.FALSE.equals(targetInTopology)) {
          maybeRequestReconfiguration(coreContext, now);
        }
      }
      return;
    }

    // It failed on a connection: the connection broke (for example, the server closed it), unless the SDK
    // cancelled the call (timeout, shutdown) or the read timed out, which say nothing about the connection.
    ConnectionInfo info = state.connection;
    if (info != null && !call.isCanceled() && !(ioe instanceof InterruptedIOException)) {
      long connectedForNanos = nanoClock.getAsLong() - info.connectedAtNanos;
      publish(context -> new UnexpectedEndpointDisconnectedEvent(endpointContext(context, info), 1, connectedForNanos));
    }
  }

  // ---- Diagnostics ----

  List<EndpointDiagnostics> diagnostics() {
    return diagnostics(recentConnectFailure());
  }

  List<InternalEndpointDiagnostics> internalDiagnostics() {
    Failure failure = recentConnectFailure();
    Throwable tlsFailure = failure != null && hasCause(failure.cause, SSLException.class) ? failure.cause : null;

    List<InternalEndpointDiagnostics> result = new ArrayList<>();
    for (EndpointDiagnostics d : diagnostics(failure)) {
      // Same as Netty's HTTP endpoints, which never reported authentication status.
      result.add(new InternalEndpointDiagnostics(d, AuthenticationStatus.UNKNOWN, tlsFailure));
    }
    return result;
  }

  private List<EndpointDiagnostics> diagnostics(@Nullable Failure failure) {
    pruneClosedConnections();

    CircuitBreaker.State cbState = circuitBreakerState.get();
    Optional<Throwable> failureCause = failure == null ? Optional.empty() : Optional.of(failure.cause);

    List<EndpointDiagnostics> result = new ArrayList<>();

    long now = nanoClock.getAsLong();
    connections.forEach((connection, info) -> result.add(new EndpointDiagnostics(
      serviceType,
      EndpointState.CONNECTED,
      cbState,
      info.local,
      info.remote,
      Optional.ofNullable(namespace),
      info.lastActivity(now),
      Optional.of(info.id),
      Optional.empty()
    )));

    pendingConnects.keySet().forEach(pending -> result.add(new EndpointDiagnostics(
      serviceType,
      EndpointState.CONNECTING,
      cbState,
      null,
      format(pending.address),
      Optional.ofNullable(namespace),
      Optional.empty(),
      Optional.empty(),
      failureCause
    )));

    boolean circuitNotClosed = cbState == CircuitBreaker.State.OPEN || cbState == CircuitBreaker.State.HALF_OPEN;
    if (result.isEmpty() && (circuitNotClosed || failure != null)) {
      result.add(new EndpointDiagnostics(
        serviceType,
        EndpointState.DISCONNECTED,
        cbState,
        null,
        address.toString(),
        Optional.ofNullable(namespace),
        Optional.empty(),
        Optional.empty(),
        failureCause
      ));
    }

    return result;
  }

  // ---- Internals ----

  /**
   * Returns whether to publish this connection failure at WARN level: the first since the last success,
   * or the first since {@link #CONNECT_FAILURE_WARN_INTERVAL} after the last WARN.
   */
  private boolean shouldWarnOfConnectFailure(long attempt, long nowNanos) {
    if (attempt == 1) {
      lastConnectFailureWarnNanos.set(nowNanos);
      return true;
    }
    long last = lastConnectFailureWarnNanos.get();
    return nowNanos - last >= CONNECT_FAILURE_WARN_INTERVAL.toNanos()
      && lastConnectFailureWarnNanos.compareAndSet(last, nowNanos); // only one of the calls failing at once
  }

  /**
   * Returns a connect failure with a more helpful message, like the Netty endpoints' (see
   * {@code BaseEndpoint.annotateConnectException}), or the failure as is if it isn't a {@link ConnectException}.
   * <p>
   * OkHttp's message only says which address it failed to connect to ("Failed to connect to /127.0.0.1:8093"),
   * leaving the reason ("Connection refused") to its cause, so this adds the reason, and a hint.
   * The result has no stack trace or cause.
   * A cluster that requires TLS refuses connections to its non-TLS ports from other hosts, so if a connection
   * without TLS was refused, the hint mentions that. (With TLS, it can't be the reason: the TLS ports are always
   * open. And a cluster that requires TLS refuses connections; it doesn't let them time out.)
   */
  static IOException annotateConnectException(IOException e, boolean tls) {
    if (!(e instanceof ConnectException)) {
      return e;
    }
    String message = String.valueOf(e.getMessage());
    Throwable cause = e.getCause();
    if (cause != null && cause.getMessage() != null && !message.contains(cause.getMessage())) {
      message += ": " + cause.getMessage();
    }
    boolean refused = isRefused(e) || (cause instanceof ConnectException && isRefused(cause));
    String hint = !tls && refused
      ? " - Check server ports, and whether the cluster requires TLS (for example, use couchbases://)."
      : " - Check server ports.";
    // A plain ConnectException, not a subclass: the connection failed event shows the exception's class name.
    ConnectException annotated = new ConnectException(message + hint);
    // Like the Netty endpoints', no stack trace or cause: the message says it all, and the connection failed
    // event logs it at WARN level. AbstractOkHttpService logs the original at DEBUG level.
    annotated.setStackTrace(new StackTraceElement[0]);
    return annotated;
  }

  /**
   * Returns whether the exception says the connection was refused: "Connection refused", or on Windows,
   * "Connection refused: connect".
   */
  private static boolean isRefused(Throwable e) {
    return e.getMessage() != null && e.getMessage().startsWith("Connection refused");
  }

  private void recordConnectFailure(Call call, IOException ioe) {
    if (call.isCanceled()) {
      return; // Cancelled by the SDK (timeout, shutdown) or the user; says nothing about the node.
    }
    lastConnectFailure = new Failure(ioe, nanoClock.getAsLong());
  }

  private @Nullable Failure recentConnectFailure() {
    Failure f = lastConnectFailure;
    return f != null && nanoClock.getAsLong() - f.atNanos < failureVisibilityNanos ? f : null;
  }

  private CallState callState(Call call) {
    return calls.computeIfAbsent(call, c -> new CallState());
  }

  private void removePendingConnects(Call call) {
    // Fast fallback can start several connection attempts for one call;
    // the losers don't always report connectFailed.
    pendingConnects.keySet().removeIf(pending -> pending.call == call);
  }

  private void pruneClosedConnections() {
    connections.forEach((connection, info) -> {
      if (isClosed(connection) && connections.remove(connection, info)) {
        publish(context -> new EndpointDisconnectedEvent(Duration.ZERO, endpointContext(context, info)));
      }
    });
  }

  private void publish(Function<CoreContext, Event> event) {
    CoreContext context = coreContext;
    if (context == null) {
      return;
    }
    try {
      context.environment().eventBus().publish(event.apply(context));
    } catch (RuntimeException e) {
      // This runs in OkHttp's event callbacks, so an exception would fail the call.
      log.debug("Failed to publish a connection event", e);
    }
  }

  /**
   * Returns whether the current cluster config has this service on this node, or null if unknown (no config yet),
   * like the Netty endpoints' (see {@code BaseEndpoint.maybeDisconnectIfEndpointNoLongerInTopology}).
   * <p>
   * If a connection fails because the node is no longer in the cluster, and the SDK hasn't caught up (it still
   * sends requests there), this says whether that's because the SDK's config is out of date (true),
   * or because the SDK hasn't acted on its config (false).
   */
  private @Nullable Boolean isTargetInTopology(CoreContext context) {
    try {
      ClusterConfig config = context.core().clusterConfig();
      return config != null && config.hasClusterOrBucketConfig() ? config.contains(serviceType, address) : null;
    } catch (RuntimeException e) {
      log.debug("Failed to check whether {} on {} is in the cluster topology", serviceType, redactMeta(address), e);
      return null;
    }
  }

  /**
   * Asks the core to reconfigure itself from the current cluster config, unless this tracker did so recently.
   * <p>
   * The SDK only sends requests to nodes and services it manages, so if the current config doesn't have this
   * service on this node, the SDK hasn't caught up with its own config: it should have removed the service, or
   * the node. Reconfiguring removes them, like the core's {@code InvalidStateWatchdog} does when it notices
   * the number of nodes is off (which it doesn't if, for example, a node was replaced). Like the Netty endpoints'
   * check (see {@code BaseEndpoint.maybeDisconnectIfEndpointNoLongerInTopology}), this is a safety net:
   * the core should have reconfigured itself when it got the config.
   * <p>
   * The config might have changed a moment ago, with the core about to reconfigure itself anyway.
   * Asking again does no harm: it only reconfigures from the same config again.
   */
  private void maybeRequestReconfiguration(CoreContext context, long nowNanos) {
    long last = lastReconfigurationRequestNanos.get();
    if (nowNanos - last < RECONFIGURATION_REQUEST_INTERVAL.toNanos()
      || !lastReconfigurationRequestNanos.compareAndSet(last, nowNanos)) {
      return;
    }
    publish(c -> new WatchdogInvalidStateIdentifiedEvent(c, "Failed to connect to the " + serviceType
      + " service on " + redactMeta(address) + ", which isn't in the current cluster config; triggering reconfiguration."));
    try {
      context.core().configurationProvider().republishCurrentConfig();
    } catch (RuntimeException e) {
      // This runs in OkHttp's event callbacks, so an exception would fail the call.
      log.warn("Failed to trigger reconfiguration", e);
    }
  }

  /**
   * Returns an endpoint context for an event about the given connection (or none: a failed connection attempt),
   * like the Netty endpoints'.
   */
  private EndpointContext endpointContext(CoreContext context, @Nullable ConnectionInfo info) {
    return new EndpointContext(
      context,
      address,
      circuitBreakerView,
      serviceType,
      Optional.ofNullable(info == null ? null : info.localAddress),
      Optional.ofNullable(namespace),
      Optional.ofNullable(info == null ? null : info.id)
    );
  }

  /**
   * Reports the service's circuit breaker state, for the endpoint contexts. (The service's own circuit breaker
   * is an {@link AttemptCircuitBreaker}, not the endpoints' {@link CircuitBreaker}.)
   */
  private final CircuitBreaker circuitBreakerView = new CircuitBreaker() {
    @Override
    public void track() {
    }

    @Override
    public void markSuccess() {
    }

    @Override
    public void markFailure() {
    }

    @Override
    public void reset() {
    }

    @Override
    public boolean allowsRequest() {
      return circuitBreakerState.get() != State.OPEN;
    }

    @Override
    public State state() {
      return circuitBreakerState.get();
    }
  };

  private static boolean isClosed(Connection connection) {
    try {
      return connection.socket().isClosed();
    } catch (RuntimeException e) {
      return true;
    }
  }

  private static @Nullable HostAndPort toHostAndPort(@Nullable SocketAddress address) {
    if (!(address instanceof InetSocketAddress)) {
      return null;
    }
    InetSocketAddress a = (InetSocketAddress) address;
    return new HostAndPort(a.getHostString(), a.getPort());
  }

  private static @Nullable String format(@Nullable SocketAddress address) {
    if (!(address instanceof InetSocketAddress)) {
      return null;
    }
    InetSocketAddress a = (InetSocketAddress) address;
    return redactMeta(a.getHostString()) + ":" + a.getPort();
  }

  @Override
  public String toString() {
    return "NodeConnectionTracker{" +
      "serviceType=" + serviceType +
      ", address=" + address +
      ", connections=" + connections.size() +
      ", pendingConnects=" + pendingConnects.size() +
      '}';
  }

  private static final class ConnectionInfo {
    final String id; // "0x" followed by hex digits, like Netty's channel IDs
    final @Nullable HostAndPort localAddress;
    final @Nullable String local;
    final @Nullable String remote;
    final long connectedAtNanos; // when first acquired
    volatile long lastActivityNanos; // 0 means none yet

    ConnectionInfo(Connection connection, long connectedAtNanos) {
      this.connectedAtNanos = connectedAtNanos;
      Socket socket = connection.socket();
      this.id = String.format("0x%08x", System.identityHashCode(connection));
      this.localAddress = toHostAndPort(socket.getLocalSocketAddress());
      this.local = format(socket.getLocalSocketAddress());
      this.remote = format(socket.getRemoteSocketAddress());
    }

    Optional<Long> lastActivity(long now) {
      long last = lastActivityNanos;
      return last == 0
        ? Optional.empty()
        : Optional.of(TimeUnit.NANOSECONDS.toMicros(now - last));
    }
  }

  private static final class PendingConnect {
    final Call call;
    final InetSocketAddress address;

    PendingConnect(Call call, InetSocketAddress address) {
      this.call = requireNonNull(call);
      this.address = requireNonNull(address);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (!(o instanceof PendingConnect)) return false;
      PendingConnect that = (PendingConnect) o;
      return call == that.call && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(System.identityHashCode(call), address);
    }
  }

  private static final class CallState {
    volatile boolean acquiredConnection;
    volatile @Nullable IOException tlsFailure;
    volatile long connectStartNanos; // 0 means not connecting (yet)
    volatile @Nullable ConnectionInfo connection;
  }

  private static final class Failure {
    final Throwable cause;
    final long atNanos;

    Failure(Throwable cause, long atNanos) {
      this.cause = requireNonNull(cause);
      this.atNanos = atNanos;
    }
  }
}
