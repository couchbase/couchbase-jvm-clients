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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionAbortedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionIgnoredEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointStateChangedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointWriteFailedEvent;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointDisconnectedEvent;
import com.couchbase.client.core.cnc.events.io.InvalidRequestDetectedEvent;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.endpoint.KeyValueEndpoint;
import com.couchbase.client.core.endpoint.LazyCircuitBreaker;
import com.couchbase.client.core.endpoint.NoopCircuitBreaker;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.SingleStateful;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

/**
 * Like {@link KeyValueEndpoint}, but without Netty: each connection is a plain socket (with TLS, if enabled),
 * read and written by threads of its own (see {@link KeyValueConnection}), or by a shared NIO event loop
 * (see {@link NioKeyValueConnection}).
 * <p>
 * Does what {@link BaseEndpoint} does for a KV endpoint, with the same states, events, circuit breaker,
 * and reconnect backoff.
 */
@NullMarked
@Stability.Internal
public class SocketKeyValueEndpoint implements Endpoint, KeyValueEndpointConnection.Listener {
  private static final Logger log = LoggerFactory.getLogger(SocketKeyValueEndpoint.class);

  private static final Duration TARGET_ABSENT_BEFORE_DISCONNECT = Duration.ofSeconds(
    Long.parseLong(System.getProperty("com.couchbase.endpoint.targetAbsentBeforeDisconnectSeconds", "60")));

  // Like BaseEndpoint's reconnect backoff.
  private static final Duration FIRST_RECONNECT_DELAY = Duration.ofMillis(32);
  private static final Duration MAX_RECONNECT_DELAY = Duration.ofMillis(4096);

  private final SingleStateful<EndpointState> state;
  private final AtomicReference<EndpointContext> endpointContext;
  private final CircuitBreaker circuitBreaker;
  private final boolean circuitBreakerEnabled;
  private final CircuitBreaker.CompletionCallback circuitBreakerCallback;

  private final String hostname;
  private final int port;
  private final Optional<String> bucketName;
  private final Authenticator authenticator;
  private final KeyValueSocketConnector connector;
  private final Set<ServerFeature> features;

  /**
   * Whether connections use NIO on a shared event loop ({@link NioKeyValueConnection}), or a reader thread and
   * a writer thread each ({@link KeyValueConnection}).
   */
  private final boolean nio;

  private final AtomicBoolean disconnect = new AtomicBoolean();
  private final CountDownLatch disconnectSignal = new CountDownLatch(1);

  /**
   * Orders a connect attempt's outcome against {@link #disconnect()}.
   */
  private final Object lock = new Object();

  /**
   * The connect attempt in progress, if any. Guarded by {@link #lock}.
   */
  private KeyValueEndpointConnection.@Nullable Opener connecting;

  /**
   * The current connection, once connected. Like BaseEndpoint's channel, it's kept after it closes
   * (until the next one connects), for diagnostics.
   */
  private volatile @Nullable KeyValueEndpointConnection connection;

  /**
   * Whether {@link #closed} was called for {@link #connection}. Guarded by {@link #lock}.
   */
  private boolean connectionClosed;

  private volatile long disconnectStartNanos;
  private volatile long lastResponseTimestamp;
  private volatile long lastConnectedAt;
  private volatile @Nullable Throwable lastConnectAttemptFailure;

  // Only accessed by the connect loop.
  private long firstSawTargetAbsentNanos = 0;

  public SocketKeyValueEndpoint(
    ServiceContext serviceContext,
    String hostname,
    int port,
    Optional<String> bucketName,
    Authenticator authenticator,
    KeyValueSocketConnector connector
  ) {
    // The com.couchbase.kvNio system property (true by default), read when the endpoint is created.
    this(serviceContext, hostname, port, bucketName, authenticator, connector,
      Boolean.parseBoolean(System.getProperty("com.couchbase.kvNio", "true")));
  }

  /**
   * @param nio whether connections use NIO (see {@link NioKeyValueConnection}).
   */
  SocketKeyValueEndpoint(
    ServiceContext serviceContext,
    String hostname,
    int port,
    Optional<String> bucketName,
    Authenticator authenticator,
    KeyValueSocketConnector connector,
    boolean nio
  ) {
    this.nio = nio;
    this.hostname = requireNonNull(hostname);
    this.port = port;
    this.bucketName = requireNonNull(bucketName);
    this.authenticator = requireNonNull(authenticator);
    this.connector = requireNonNull(connector);
    this.features = KeyValueEndpoint.KeyValuePipelineInitializer.serverFeatures(serviceContext.environment());

    CircuitBreakerConfig circuitBreakerConfig = serviceContext.environment().ioConfig().kvCircuitBreakerConfig();
    if (circuitBreakerConfig.enabled()) {
      this.circuitBreaker = new LazyCircuitBreaker(circuitBreakerConfig);
      this.circuitBreakerEnabled = true;
    } else {
      this.circuitBreaker = NoopCircuitBreaker.INSTANCE;
      this.circuitBreakerEnabled = false;
    }
    this.circuitBreakerCallback = circuitBreakerConfig.completionCallback();

    this.endpointContext = new AtomicReference<>(
      new EndpointContext(serviceContext, new HostAndPort(hostname, port), circuitBreaker, ServiceType.KV,
        Optional.empty(), serviceContext.bucket(), Optional.empty())
    );
    this.state = SingleStateful.fromInitial(
      EndpointState.DISCONNECTED,
      (from, to) -> serviceContext.environment().eventBus().publish(new EndpointStateChangedEvent(endpointContext.get(), from, to))
    );
  }

  @Override
  public EndpointContext context() {
    return endpointContext.get();
  }

  // ---- connecting

  @Override
  public void connect() {
    // Under the lock, and not once told to disconnect: otherwise this could leave the endpoint CONNECTING
    // after disconnect() set it to DISCONNECTED (the connect loop would see the disconnect and exit).
    synchronized (lock) {
      if (!disconnect.get() && state.compareAndTransition(EndpointState.DISCONNECTED, EndpointState.CONNECTING)) {
        KeyValueConnection.ioThreads.execute(this::connectUntilConnectedOrDisconnected);
      }
    }
  }

  /**
   * Like BaseEndpoint's reconnect(): tries to connect until it succeeds or the endpoint is told to disconnect,
   * waiting longer after each failed attempt.
   */
  private void connectUntilConnectedOrDisconnected() {
    for (long attempt = 1; ; attempt++) {
      // Under the lock, so if disconnect() comes first, the endpoint stays DISCONNECTED.
      synchronized (lock) {
        if (disconnect.get()) {
          return;
        }
        state.transition(EndpointState.CONNECTING);
      }

      EndpointContext ctx = endpointContext.get();
      Duration connectTimeout = ctx.environment().timeoutConfig().connectTimeout();
      long start = System.nanoTime();
      KeyValueEndpointConnection.Opener opener = null;
      KeyValueEndpointConnection conn = null;

      try {
        opener = nio
          ? new NioKeyValueConnectionOpener(ctx, hostname, port, bucketName, authenticator.keyValueSaslCredentials(ctx), features, connector)
          : new KeyValueConnectionOpener(ctx, hostname, port, bucketName, authenticator.keyValueSaslCredentials(ctx), features, connector);
        synchronized (lock) {
          if (disconnect.get()) {
            throw new IllegalStateException("Endpoint was told to disconnect.");
          }
          connecting = opener;
        }
        conn = opener.open(start + connectTimeout.toNanos(), this);
        connected(ctx, conn, start);
        return;

      } catch (Throwable t) {
        // Anything that goes wrong is a failed attempt (rather than ending the loop, which would leave the endpoint
        // CONNECTING, with nothing reconnecting). Including connected() failing: it only throws before it hands
        // the connection over.
        synchronized (lock) {
          connecting = null;
        }
        if (conn != null) {
          conn.close();
        } else if (opener != null) {
          opener.close();
        }
        try {
          state.transition(EndpointState.DISCONNECTED);
          Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
          if (disconnect.get()) {
            ctx.environment().eventBus().publish(new EndpointConnectionAbortedEvent(elapsed, ctx, timings(opener)));
            return;
          }
          reportConnectFailure(ctx, t, t instanceof TimeoutException ? connectTimeout : elapsed, attempt);
        } catch (Throwable reportingFailure) {
          log.warn("Failed to report a failed attempt to connect; trying again.", reportingFailure);
        }
        if (!waitBeforeReconnecting(attempt)) {
          return; // told to disconnect while waiting
        }
      }
    }
  }

  private void reportConnectFailure(EndpointContext ctx, Throwable t, Duration duration, long attempt) {
    // We drop the severity for the BucketNotFoundException because it shows up when
    // bootstrapping against MDS clusters and nodes with no kv service enabled on it
    // that is bucket aware. If a bucket really does not exist we'll get an auth
    // exception instead.
    Event.Severity severity = t instanceof BucketNotFoundException
      ? Event.Severity.DEBUG
      : Event.Severity.WARN;

    Throwable ex = BaseEndpoint.annotateConnectException(t);
    lastConnectAttemptFailure = ex;

    Boolean targetInTopology = maybeDisconnectIfEndpointNoLongerInTopology(ctx, attempt);

    ctx.environment().eventBus().publish(new EndpointConnectionFailedEvent(
      severity,
      duration,
      ctx,
      attempt,
      ex,
      targetInTopology
    ));
  }

  /**
   * @return false if the endpoint was told to disconnect while waiting.
   */
  private boolean waitBeforeReconnecting(long attempt) {
    long delayMillis = FIRST_RECONNECT_DELAY.toMillis() << Math.min(attempt - 1, 30);
    delayMillis = Math.min(delayMillis, MAX_RECONNECT_DELAY.toMillis());
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMillis);
    while (true) {
      try {
        return !disconnectSignal.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        // Only disconnect() stops reconnecting (otherwise the endpoint would stay DISCONNECTED). Nothing in the SDK
        // interrupts this thread. Doesn't restore the interrupt, which would make the next attempt's I/O fail.
        log.debug("Ignoring an interrupt while waiting to reconnect; {}", endpointContext.get());
      }
    }
  }

  private void connected(EndpointContext ctx, KeyValueEndpointConnection conn, long start) {
    long now = System.nanoTime();
    Duration elapsed = Duration.ofNanos(now - start);

    synchronized (lock) {
      connecting = null;

      if (disconnect.get()) {
        // Unlike the Netty implementation, doesn't publish EndpointDisconnectedEvent too (when the connection closes):
        // the endpoint never reported it as connected.
        conn.close();
        ctx.environment().eventBus().publish(new EndpointConnectionIgnoredEvent(elapsed, ctx, conn.timings()));
        return;
      }

      lastConnectAttemptFailure = null;

      Optional<HostAndPort> localSocket = Optional.empty();
      SocketAddress localAddress = conn.localAddress();
      if (localAddress instanceof InetSocketAddress) {
        InetSocketAddress so = (InetSocketAddress) localAddress;
        localSocket = Optional.of(new HostAndPort(so.getHostString(), so.getPort()));
      }

      EndpointContext newContext = new EndpointContext(
        ctx,
        ctx.remoteSocket(),
        ctx.circuitBreaker(),
        ctx.serviceType(),
        localSocket,
        ctx.bucket(),
        Optional.of(conn.channelId())
      );
      ctx.environment().eventBus().publish(new EndpointConnectedEvent(elapsed, newContext, conn.timings()));
      endpointContext.set(newContext);
      circuitBreaker.reset();
      lastConnectedAt = now;

      // Connected before starting, so if the connection closes right away, closed() sees it was connected.
      // (Requests sent in the meantime wait in the connection's queue.)
      // Hands the connection over last, and nothing after that throws: if anything before it does,
      // the connect loop closes the connection, and tries again.
      state.transition(EndpointState.CONNECTED);
      connection = conn;
      connectionClosed = false;
      conn.start(); // doesn't throw if it can't start a thread: the connection closes (see closed())
    }
  }

  private static TreeMap<String, Duration> timings(KeyValueEndpointConnection.@Nullable Opener opener) {
    return opener == null ? new TreeMap<>() : new TreeMap<>(opener.timings());
  }

  /**
   * Like BaseEndpoint's: checks whether this endpoint's target host is still in the current cluster topology.
   * If it persistently isn't, the SDK is reconnect-looping to a node that has been removed - i.e. a stuck
   * reconfiguration upstream.
   */
  private @Nullable Boolean maybeDisconnectIfEndpointNoLongerInTopology(EndpointContext ctx, long attempt) {
    Boolean targetInTopology = null;
    try {
      ClusterConfig config = ctx.core().clusterConfig();
      boolean alreadyDisconnecting = disconnect.get();
      boolean haveConfig = config != null && config.hasClusterOrBucketConfig();
      if (!alreadyDisconnecting && haveConfig) {
        targetInTopology = config.contains(ServiceType.KV, ctx.remoteSocket());
      }

      if (Boolean.FALSE.equals(targetInTopology)) {
        if (firstSawTargetAbsentNanos == 0) {
          firstSawTargetAbsentNanos = System.nanoTime();
        } else if (System.nanoTime() - firstSawTargetAbsentNanos >= TARGET_ABSENT_BEFORE_DISCONNECT.toNanos()) {
          log.warn("Endpoint's target has been absent from the cluster topology for over {}s ({}"
              + " failed connect attempts); abandoning reconnection and disconnecting the endpoint."
              + " This is a safety net for a stuck reconfiguration. Current nodes: {}",
            TARGET_ABSENT_BEFORE_DISCONNECT.getSeconds(), attempt, ctx.core().nodes());
          disconnect();
          // The owning Service and Node should GC themselves once all owned Endpoints disconnect and disappear.
        }
      } else {
        // Target present, no usable config, or already disconnecting: reset the absence timer.
        firstSawTargetAbsentNanos = 0;
      }
    } catch (Throwable t) {
      log.warn("Topology membership check failed; ignoring", t);
      firstSawTargetAbsentNanos = 0;
    }
    return targetInTopology;
  }

  // ---- disconnecting

  @Override
  public void disconnect() {
    KeyValueEndpointConnection conn;
    synchronized (lock) {
      if (!disconnect.compareAndSet(false, true)) {
        return;
      }
      state.transition(EndpointState.DISCONNECTING);
      disconnectSignal.countDown();
      disconnectStartNanos = System.nanoTime();

      if (connecting != null) {
        connecting.close(); // the connect loop sees the disconnect, and reports the attempt as aborted
      }
      conn = connectionClosed ? null : connection;
      if (conn == null) {
        state.transition(EndpointState.DISCONNECTED);
        state.close();
      }
    }

    if (conn != null) {
      // Even if it's already closing, closed() hasn't been called yet; it finishes disconnecting,
      // once the connection's requests are retried.
      conn.close();
    }
  }

  @Override
  public boolean receivedDisconnectSignal() {
    return disconnect.get();
  }

  // ---- KeyValueEndpointConnection.Listener

  @Override
  public void responseReceived() {
    lastResponseTimestamp = System.nanoTime();
  }

  @Override
  public void writeFailed(KeyValueRequest<? extends Response> request, Throwable cause) {
    EndpointContext ctx = endpointContext.get();
    Event.Severity severity = disconnect.get() ? Event.Severity.DEBUG : Event.Severity.WARN;
    ctx.environment().eventBus().publish(new EndpointWriteFailedEvent(severity, ctx, cause));
    RetryOrchestrator.maybeRetry(ctx, request, RetryReason.ENDPOINT_NOT_WRITABLE);
  }

  /**
   * Like BaseEndpoint's notifyChannelInactive() (and its closeChannel(), when told to disconnect).
   */
  @Override
  public void closed(KeyValueEndpointConnection conn, int requestsInFlight) {
    // Under the lock, so disconnect() either happens first (and leaves the rest to us),
    // or after we reconnect (and stops the connect loop).
    synchronized (lock) {
      if (conn != connection) {
        return;
      }
      connectionClosed = true;

      EndpointContext ctx = endpointContext.get();
      if (disconnect.get()) {
        state.transition(EndpointState.DISCONNECTED);
        state.close();
        ctx.environment().eventBus().publish(
          new EndpointDisconnectedEvent(Duration.ofNanos(System.nanoTime() - disconnectStartNanos), ctx)
        );
        return;
      }

      if (state() == EndpointState.CONNECTED) {
        long connectedSince = System.nanoTime() - lastConnectedAt;
        ctx.environment().eventBus().publish(
          new UnexpectedEndpointDisconnectedEvent(ctx, requestsInFlight, connectedSince)
        );

        state.transition(EndpointState.DISCONNECTED);
        connect();
      }
    }
  }

  // ---- sending

  @Override
  public <R extends Request<? extends Response>> void send(final R request) {
    if (request.timeoutElapsed()) {
      request.cancel(CancellationReason.TIMEOUT);
    }
    if (request.completed()) {
      return;
    }

    final EndpointContext ctx = endpointContext.get();
    if (!(request instanceof KeyValueRequest)) {
      // The Netty implementation closes the connection; this one only fails the request.
      ctx.environment().eventBus().publish(new InvalidRequestDetectedEvent(new IoContext(ctx, null, null, ctx.bucket()), ServiceType.KV, request));
      request.fail(InvalidArgumentException.fromMessage("Not a KV request: " + request.getClass().getName()));
      return;
    }

    KeyValueEndpointConnection conn = connection;
    if (conn != null && canWrite(conn)) {
      request.context()
        .lastDispatchedFrom(ctx.localSocket().orElse(null))
        .lastDispatchedTo(ctx.remoteSocket())
        .lastChannelId(ctx.channelId().orElse(null));

      if (circuitBreakerEnabled) {
        circuitBreaker.track();
        request.response().whenComplete((response, throwable) -> {
          if (circuitBreakerCallback.apply(response, throwable)) {
            circuitBreaker.markSuccess();
          } else {
            circuitBreaker.markFailure();
          }
        });
      }

      if (!conn.send((KeyValueRequest<?>) request)) {
        // It closed after we checked.
        writeFailed((KeyValueRequest<?>) request, new IOException("Connection closed"));
      }
    } else {
      RetryReason retryReason = circuitBreaker.allowsRequest()
        ? RetryReason.ENDPOINT_NOT_WRITABLE
        : RetryReason.ENDPOINT_CIRCUIT_OPEN;
      RetryOrchestrator.maybeRetry(ctx, request, retryReason);
    }
  }

  private boolean canWrite(KeyValueEndpointConnection conn) {
    return state.state() == EndpointState.CONNECTED
      && conn.isWritable()
      && circuitBreaker.allowsRequest();
  }

  @Override
  public boolean freeToWrite() {
    return true; // pipelined
  }

  @Override
  public long outstandingRequests() {
    KeyValueEndpointConnection conn = connection;
    return conn == null || conn.isClosed() ? 0 : conn.inFlightCount();
  }

  @Override
  public long lastResponseReceived() {
    return lastResponseTimestamp;
  }

  @Override
  public long lastConnectedAt() {
    return lastConnectedAt;
  }

  // ---- state and diagnostics

  @Override
  public EndpointState state() {
    return state.state();
  }

  @Override
  public Flux<EndpointState> states() {
    return state.states();
  }

  @Override
  public String remoteHostname() {
    return hostname;
  }

  @Override
  public int remotePort() {
    return port;
  }

  @Override
  public @Nullable Throwable lastConnectAttemptFailure() {
    return lastConnectAttemptFailure;
  }

  @Override
  public EndpointDiagnostics diagnostics() {
    KeyValueEndpointConnection conn = connection;
    String remote = conn == null ? null : format(conn.remoteAddress());
    String local = conn == null ? null : format(conn.localAddress());

    final Optional<Long> lastActivity = lastResponseTimestamp == 0
      ? Optional.empty()
      : Optional.of(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - lastResponseTimestamp));

    final Optional<String> id = Optional.ofNullable(conn).map(c -> "0x" + c.shortChannelId());
    return new EndpointDiagnostics(context().serviceType(), state(), circuitBreaker.state(), local, remote, context().bucket(),
      lastActivity, id, Optional.ofNullable(lastConnectAttemptFailure()));
  }

  private static @Nullable String format(@Nullable SocketAddress address) {
    if (!(address instanceof InetSocketAddress)) {
      return null;
    }
    InetSocketAddress a = (InetSocketAddress) address;
    return redactMeta(a.getHostString()) + ":" + a.getPort();
  }

  @Override
  public InternalEndpointDiagnostics internalDiagnostics() {
    EndpointContext ctx = context();
    return new InternalEndpointDiagnostics(diagnostics(), ctx.authenticationStatus(), ctx.tlsHandshakeFailure());
  }
}
