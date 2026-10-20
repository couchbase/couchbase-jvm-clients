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

import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.CollectionOutdatedHandledEvent;
import com.couchbase.client.core.cnc.events.io.CustomTlsCiphersEnabledEvent;
import com.couchbase.client.core.cnc.events.io.GenericFailureDetectedEvent;
import com.couchbase.client.core.cnc.events.io.InvalidPacketDetectedEvent;
import com.couchbase.client.core.cnc.events.io.KeyValueErrorMapCodeHandledEvent;
import com.couchbase.client.core.cnc.events.io.NotMyVbucketReceivedEvent;
import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseReceivedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownResponseStatusReceivedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.RangeScanPartitionFailedException;
import com.couchbase.client.core.error.context.GenericRequestErrorContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.TracingUtils;
import com.couchbase.client.core.io.netty.TrafficCaptureHandler;
import com.couchbase.client.core.io.netty.kv.AuthenticationRefreshHandler;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.KeyValueMessageHandler;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.io.netty.kv.ServerPushHandler;
import com.couchbase.client.core.io.netty.kv.sasl.SaslCredentials;
import com.couchbase.client.core.io.netty.kv.sasl.SingleStepSaslAuthParameters;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueResponse;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.CbThreads;
import com.couchbase.client.core.util.SynchronousEventBus;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.couchbase.client.core.io.netty.TracingUtils.setCommonDispatchSpanAttributes;
import static com.couchbase.client.core.io.netty.TracingUtils.setCommonKVSpanAttributes;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.CONN_STATE_INVALIDATED;
import static com.couchbase.client.core.io.netty.kv.ErrorMap.ErrorAttribute.FETCH_CONFIG;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.bodyAsString;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.service.KeyValueSocketConnector.closeQuietly;
import static com.couchbase.client.core.service.KeyValueSocketConnector.remainingMillis;
import static java.util.Objects.requireNonNull;

/**
 * One KV connection that doesn't use Netty: a socket (with TLS, if enabled) and the threads that use it.
 * Does what the Netty pipeline's KV handlers do (see {@link com.couchbase.client.core.endpoint.KeyValueEndpoint}),
 * with the same events, retries and errors.
 * <p>
 * Lifecycle: {@link #open} connects and does the handshake, blocking the caller. Then {@link #start} starts
 * two threads, which run until the connection closes:
 * <ul>
 *   <li>The writer encodes nothing (requests are encoded by {@link #send}, on the caller's thread); it writes
 *       queued requests to the socket, up to {@value #FLUSH_CONSOLIDATION_LIMIT} per flush, flushing when the queue
 *       is empty. It also sends a NOOP when the server has been quiet for a while, and closes the connection
 *       if the server stays quiet (like the Netty implementation's {@code IdleStateHandler}).
 *   <li>The reader reads packets from the server, and completes (or retries) the requests they answer.
 * </ul>
 * Like the Netty implementation's {@code FlushConsolidationHandler}, requests sent by the reader thread while it's
 * handling a batch of responses (for example, from a callback that sends the next request when a response arrives)
 * are written together: the reader wakes the writer once, when it has handled every response it already read,
 * instead of once per request.
 * When the connection closes (because either thread failed, the server closed it, or {@link #close} was called),
 * the last thread to finish retries the requests that were queued or in flight, then tells the listener.
 * <p>
 * Thread-safe.
 */
@NullMarked
final class KeyValueConnection {
  private static final Logger log = LoggerFactory.getLogger(KeyValueConnection.class);

  /**
   * Heap buffers, since requests are written to (and responses read from) streams. Not pooled, like plain arrays.
   */
  static final ByteBufAllocator ALLOC = new UnpooledByteBufAllocator(false);

  /**
   * Runs the connect attempts, and each connection's reader and writer. Virtual threads, if available,
   * unless the {@code com.couchbase.kvVirtualThreads} system property is false (then platform threads).
   */
  static final ExecutorService ioThreads = Boolean.parseBoolean(System.getProperty("com.couchbase.kvVirtualThreads", "true"))
    ? CbThreads.unboundedExecutorService("cb-kv-io-")
    : Executors.newCachedThreadPool(CbThreads.platformThreadFactory("cb-kv-io-"));

  /**
   * Runs {@link #checkStuckWriter} for every connection. One thread is plenty: the checks are quick.
   */
  private static final ScheduledThreadPoolExecutor stuckWriterChecks = newStuckWriterChecks();

  private static ScheduledThreadPoolExecutor newStuckWriterChecks() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, CbThreads.platformThreadFactory("cb-kv-idle-"));
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  private static final int BUFFER_SIZE = 64 * 1024;

  /**
   * The most requests the writer writes before flushing, like the Netty implementation's
   * {@code FlushConsolidationHandler} (which uses the same default).
   */
  private static final int FLUSH_CONSOLIDATION_LIMIT = 256;

  /**
   * How often the writer checks whether the server has been quiet, at most.
   */
  private static final long MAX_IDLE_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

  /**
   * Like the Netty implementation's IdleStateHandler, which waits at least this long.
   */
  private static final long MIN_READER_IDLE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

  /**
   * Returns how long the server can be quiet before the connection sends a NOOP: half the idle timeout.
   * Or 0 if there's no idle detection: like the Netty implementation's IdleStateHandler, if it's not positive.
   */
  static long readerIdleNanos(Duration configIdleRedialTimeout) {
    long half = configIdleRedialTimeout.toNanos() / 2;
    return half <= 0 ? 0 : Math.max(half, MIN_READER_IDLE_NANOS);
  }

  /**
   * What the connection tells its endpoint.
   */
  interface Listener {
    /**
     * Called for every response the connection reads.
     */
    void responseReceived();

    /**
     * Called for each request that was queued, but never written, when the connection closed.
     */
    void writeFailed(KeyValueRequest<? extends Response> request, Throwable cause);

    /**
     * Called once, after a connection that was {@linkplain #start started} closes, and its requests were retried.
     *
     * @param requestsInFlight how many requests were written, and waiting for their responses, when it closed.
     */
    void closed(KeyValueConnection connection, int requestsInFlight);
  }

  /**
   * A request waiting to be written.
   */
  private static final class Outbound {
    private final @Nullable KeyValueRequest<Response> request; // null only for CLOSE
    final ByteBuf encoded;
    final int size;

    Outbound(@Nullable KeyValueRequest<Response> request, ByteBuf encoded) {
      this.request = request;
      this.encoded = encoded;
      this.size = encoded.readableBytes();
    }

    KeyValueRequest<Response> request() {
      return requireNonNull(request);
    }
  }

  /**
   * Tells the writer to stop.
   */
  private static final Outbound CLOSE = new Outbound(null, Unpooled.EMPTY_BUFFER);

  /**
   * A request that was written, and is waiting for its response.
   */
  private static final class InFlight {
    final KeyValueRequest<Response> request;
    final long dispatchStartNanos;
    final @Nullable RequestSpan dispatchSpan;

    InFlight(KeyValueRequest<Response> request, long dispatchStartNanos, @Nullable RequestSpan dispatchSpan) {
      this.request = request;
      this.dispatchStartNanos = dispatchStartNanos;
      this.dispatchSpan = dispatchSpan;
    }
  }

  private final EndpointContext endpointContext;
  private final String remoteHostname;
  private final int remotePort;
  private final Optional<String> bucketName;
  private final @Nullable SaslCredentials credentials;
  private final Set<ServerFeature> requestedFeatures;
  private final Listener listener;
  private final EventBus eventBus;
  private final IoConfig ioConfig;
  private final boolean isInternalTracer;

  /**
   * Whether to publish every packet written and read (io.servicesToCapture), like the Netty implementation's
   * TrafficCaptureHandler.
   */
  private final boolean captureTraffic;

  private final int highWaterMark;
  private final int lowWaterMark;
  private final long readerIdleNanos;
  private final long idleCheckIntervalNanos;

  /**
   * The connection's ID: short (8 hex digits, like a Netty channel ID's short form), and long
   * (the endpoint's ID and this ID, as sent in the HELLO request, like the Netty implementation's).
   */
  private final String shortChannelId;
  private final String channelId;

  /**
   * How long each connect step took (see {@link #timings()}).
   */
  private final SortedMap<String, Duration> timings = Collections.synchronizedSortedMap(new TreeMap<>());

  // Set while connecting, before the reader and writer start.
  private volatile @Nullable Socket tcpSocket;
  private volatile @Nullable Socket socket;
  private @Nullable InputStream in;

  /**
   * Splits what the connection reads into packets. Used by the connecting thread during the handshake,
   * then by the reader, so anything read ahead during the handshake isn't lost.
   */
  private final KeyValueFrameDecoder frames = new KeyValueFrameDecoder();
  private @Nullable OutputStream out;
  private volatile IoContext ioContext;
  private volatile @Nullable KeyValueChannelContext channelContext;
  private volatile @Nullable ErrorMap errorMap;

  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile @Nullable Throwable closeCause;

  private final ConcurrentLinkedQueue<Outbound> queue = new ConcurrentLinkedQueue<>();

  // Waking the writer (see wakeWriter()).
  private volatile @Nullable Thread readerThread;
  private volatile @Nullable Thread writerThread;
  private volatile boolean writerParked;

  // Only accessed by the reader thread: whether it's handling responses it has already read,
  // and whether it sent a request in the meantime without waking the writer.
  private boolean handlingReadBatch;
  private boolean writerWakeDeferred;
  private final AtomicLong queuedBytes = new AtomicLong();
  private volatile boolean writable = true;

  /**
   * Written requests waiting for their responses, by opaque.
   */
  private final Map<Integer, InFlight> inFlight = new ConcurrentHashMap<>();

  private volatile long lastReadNanos;
  private volatile boolean noopSentSinceLastRead;

  // Only accessed by the writer.
  private long noopSentNanos;

  /**
   * Whether the reader should ignore the responses it already read, once the connection is closed.
   * Only accessed by the reader.
   */
  private boolean discardUnreadResponses;

  /**
   * Whether the writer is writing (so it might be blocked, and can't run {@link #checkIdle}).
   */
  private volatile boolean writing;

  /**
   * The reader and the writer. The last one to finish retries the connection's requests.
   */
  private final AtomicInteger runningThreads = new AtomicInteger(2);
  private volatile boolean requestsRetried;

  // Guarded by "this".
  private SynchronousEventBus.@Nullable Subscription authenticationRefreshSubscription;
  private boolean started;
  private boolean reauthenticateWhenStarted;
  private @Nullable ScheduledFuture<?> stuckWriterCheck;

  /**
   * @param endpointContext the endpoint's context (at the start of this connect attempt).
   * @param credentials null if the connection doesn't authenticate with SASL.
   */
  KeyValueConnection(
    EndpointContext endpointContext,
    String remoteHostname,
    int remotePort,
    Optional<String> bucketName,
    @Nullable SaslCredentials credentials,
    Set<ServerFeature> requestedFeatures,
    Listener listener
  ) {
    this.endpointContext = requireNonNull(endpointContext);
    this.remoteHostname = requireNonNull(remoteHostname);
    this.remotePort = remotePort;
    this.bucketName = requireNonNull(bucketName);
    this.credentials = credentials;
    this.requestedFeatures = requireNonNull(requestedFeatures);
    this.listener = requireNonNull(listener);
    this.eventBus = endpointContext.environment().eventBus();
    this.ioConfig = endpointContext.environment().ioConfig();
    this.isInternalTracer = CbTracing.isInternalTracer(endpointContext.coreResources().requestTracer());
    this.captureTraffic = ioConfig.servicesToCapture().contains(ServiceType.KV);

    this.highWaterMark = ioConfig.highWaterMark().bytesAsInt();
    this.lowWaterMark = ioConfig.lowWaterMark().bytesAsInt();
    // Like the Netty implementation's IdleStateHandler: NOOP after half the idle time, close after all of it.
    this.readerIdleNanos = readerIdleNanos(ioConfig.configIdleRedialTimeout());
    this.idleCheckIntervalNanos = Math.min(readerIdleNanos, MAX_IDLE_CHECK_INTERVAL_NANOS);

    int id = ThreadLocalRandom.current().nextInt();
    this.shortChannelId = String.format("%08x", id);
    this.channelId = String.format("%016X", endpointContext.id()) + "/" + String.format("%016X", Integer.toUnsignedLong(id));

    this.ioContext = new IoContext(endpointContext, null, null, endpointContext.bucket());
  }

  /**
   * The connection's ID, as sent in the HELLO request.
   */
  String channelId() {
    return channelId;
  }

  /**
   * The connection's ID in short form (hex digits, without a "0x" prefix), like a Netty channel ID's short form.
   */
  String shortChannelId() {
    return shortChannelId;
  }

  /**
   * How long each connect step took, keyed like the Netty implementation's connect timings.
   */
  SortedMap<String, Duration> timings() {
    synchronized (timings) {
      return new TreeMap<>(timings);
    }
  }

  @Nullable
  SocketAddress localAddress() {
    Socket s = socket;
    return s == null ? null : s.getLocalSocketAddress();
  }

  @Nullable
  SocketAddress remoteAddress() {
    Socket s = socket;
    return s == null ? null : s.getRemoteSocketAddress();
  }

  boolean isClosed() {
    return closed.get();
  }

  /**
   * Returns false if the connection has too many bytes waiting to be written (like Netty's channel writability,
   * with the same high and low water marks).
   */
  boolean isWritable() {
    return writable && !closed.get();
  }

  /**
   * The number of requests written that are waiting for their responses.
   */
  int inFlightCount() {
    return inFlight.size();
  }

  // ---- connecting

  /**
   * Connects, and does the handshake. Blocks until it's done. Doesn't start the reader and writer.
   *
   * @param deadlineNanos when to give up, in {@link System#nanoTime()} terms.
   * @throws TimeoutException if the deadline passes.
   */
  void open(KeyValueSocketConnector connector, long deadlineNanos) throws IOException, TimeoutException {
    try {
      doOpen(connector, deadlineNanos);
    } catch (Throwable t) {
      close(t);
      throw t;
    }
  }

  private void doOpen(KeyValueSocketConnector connector, long deadlineNanos) throws IOException, TimeoutException {
    synchronized (this) {
      // close() unsubscribes while holding the lock, so if it's already been called, it's too late to subscribe.
      if (closed.get()) {
        throw new IOException("Connection closed before it opened");
      }
      // Handled on another thread (like the Netty implementation, which switches to the channel's event loop),
      // so the publisher isn't held up, and closing the connection can unsubscribe.
      authenticationRefreshSubscription = endpointContext.core().authenticationRefreshTriggers.subscribe(
        signal -> ioThreads.execute(this::onAuthenticationRefreshSignal)
      );
    }

    List<String> ciphers = endpointContext.environment().securityConfig().ciphers();
    if (connector.tlsEnabled() && !ciphers.isEmpty()) {
      eventBus.publish(new CustomTlsCiphersEnabledEvent(ciphers, endpointContext)); // like the Netty implementation's SslHandlerFactory
    }

    long tcpStart = System.nanoTime();
    long[] tlsStart = new long[1];
    Socket s;
    try {
      s = connector.connect(
        remoteHostname,
        remotePort,
        deadlineNanos,
        newSocket -> {
          if (tcpSocket == null) {
            tcpSocket = newSocket;
          }
          socket = newSocket;
          if (closed.get()) {
            closeQuietly(newSocket); // closed while connecting
          }
        },
        () -> {
          timings.put("TcpConnect", Duration.ofNanos(System.nanoTime() - tcpStart));
          tlsStart[0] = System.nanoTime();
        }
      );
    } catch (IOException e) {
      throw connectFailure(connector, e);
    }

    if (connector.tlsEnabled()) {
      timings.put("TlsHandshake", Duration.ofNanos(System.nanoTime() - tlsStart[0]));
      // Cause is null if handshake was successful. Set it unconditionally,
      // so the endpoint can recover from a transient on-path attack
      // (where the "server" temporarily presents an untrusted certificate).
      endpointContext.tlsHandshakeFailure(null);
    }

    ioContext = new IoContext(endpointContext, s.getLocalSocketAddress(), s.getRemoteSocketAddress(), endpointContext.bucket());
    InputStream input = s.getInputStream(); // the frame decoder does the buffering
    OutputStream output = new BufferedOutputStream(s.getOutputStream(), BUFFER_SIZE);
    in = input;
    out = output;

    KeyValueHandshake.Result result;
    try {
      result = new KeyValueHandshake(
        endpointContext,
        ioContext,
        ALLOC,
        new KeyValueHandshake.PacketIo() {
          @Override
          public void write(ByteBuf packet) throws IOException {
            try {
              captureWritten(packet);
              packet.readBytes(output, packet.readableBytes());
            } catch (IOException e) {
              throw withPendingTlsAlert(connector, e, input);
            } finally {
              packet.release();
            }
          }

          @Override
          public void flush() throws IOException {
            try {
              output.flush();
            } catch (IOException e) {
              throw withPendingTlsAlert(connector, e, input);
            }
          }

          @Override
          public ByteBuf read() throws IOException {
            s.setSoTimeout(remainingMillis(deadlineNanos));
            ByteBuf packet = readPacket(input);
            captureRead(packet);
            return packet;
          }
        },
        requestedFeatures,
        credentials,
        bucketName,
        channelId,
        timings
      ).run();
    } catch (IOException e) {
      // With TLS 1.3, the client finishes its side of the TLS handshake before the server checks the client's
      // certificate. So if the server rejects it, its alert arrives now. Reported like any TLS handshake failure.
      throw connectFailure(connector, e);
    }

    errorMap = result.errorMap;
    channelContext = new KeyValueChannelContext(
      result.features.contains(ServerFeature.SNAPPY) ? endpointContext.environment().compressionConfig() : null,
      bucketName,
      endpointContext.core().configurationProvider().collectionMap(),
      shortChannelId,
      result.features
    );

    // From now on, reads wait as long as it takes. The writer closes the connection if the server is quiet too long.
    s.setSoTimeout(0);
  }

  /**
   * If writing failed (for example, with "Broken pipe") because the server sent a TLS alert and closed
   * the connection, adds the alert (if it can be read) to the failure as a suppressed exception,
   * like OkHttp does.
   */
  private static IOException withPendingTlsAlert(KeyValueSocketConnector connector, IOException e, InputStream input) {
    if (connector.tlsEnabled() && OkHttpTlsHelper.describeTlsFailure(e) == null) {
      try {
        int ignored = input.read();
      } catch (IOException readFailure) {
        e.addSuppressed(readFailure);
      }
    }
    return e;
  }

  /**
   * Returns the exception to fail the connect attempt with, and if it's a TLS failure, records it like the
   * Netty implementation does.
   */
  private IOException connectFailure(KeyValueSocketConnector connector, IOException e) {
    if (!connector.tlsEnabled()) {
      return e;
    }
    SSLException tlsFailure = OkHttpTlsHelper.describeTlsFailure(e);
    if (tlsFailure == null) {
      return e;
    }

    Socket s = tcpSocket;
    if (s != null) {
      ioContext = new IoContext(endpointContext, s.getLocalSocketAddress(), s.getRemoteSocketAddress(), endpointContext.bucket());
    }
    endpointContext.tlsHandshakeFailure(e);
    if (OkHttpTlsHelper.connectionFailedDuringTls(e)) {
      return tlsFailure; // might be temporary
    }
    endpointContext.authenticationStatus(AuthenticationStatus.FAILED);
    eventBus.publish(new SecureConnectionFailedEvent(ioContext, tlsFailure));
    // With a hint saying what to check, if retrying won't help.
    return tlsFailure;
  }

  /**
   * Starts the reader and writer. Call once, after {@link #open} succeeds.
   */
  void start() {
    lastReadNanos = System.nanoTime();
    startThread(this::runReader);
    startThread(this::runWriter);

    boolean reauthenticate;
    synchronized (this) {
      started = true;
      reauthenticate = reauthenticateWhenStarted;
      if (!closed.get() && idleDetectionEnabled()) {
        stuckWriterCheck = stuckWriterChecks.scheduleWithFixedDelay(
          this::checkStuckWriter, idleCheckIntervalNanos, idleCheckIntervalNanos, TimeUnit.NANOSECONDS
        );
      }
    }
    if (reauthenticate) {
      log.info("Immediately reauthenticating the channel because a new authenticator was provided during the handshake process. Channel: {}", this);
      reauthenticate();
    }
  }

  /**
   * If the thread can't be started (for example, the JVM can't create another platform thread), closes
   * the connection, and finishes in the thread's place. So once both threads are done, the requests are retried,
   * and the listener hears the connection closed.
   */
  private void startThread(Runnable thread) {
    try {
      ioThreads.execute(thread);
    } catch (Throwable t) {
      if (close(t)) {
        reportFailure(t);
      }
      threadFinished();
    }
  }

  // ---- sending

  /**
   * Encodes the request, and queues it to be written.
   *
   * @return false if the connection is closed, so the request wasn't sent (the caller should retry it).
   */
  @SuppressWarnings("unchecked")
  boolean send(KeyValueRequest<? extends Response> req) {
    if (closed.get()) {
      return false;
    }

    KeyValueRequest<Response> request = (KeyValueRequest<Response>) req;
    ByteBuf encoded;
    try {
      encoded = request.encode(ALLOC, request.opaque(), requireNonNull(channelContext));
    } catch (Throwable err) {
      handleEncodeFailure(request, err);
      return true;
    }

    if (!MemcacheProtocol.verifyRequest(encoded)) {
      // Reported like MemcacheProtocolVerificationHandler, but the request fails instead of the connection closing.
      // Nothing was written, so the connection is still good. A retry would encode the same bad packet.
      byte[] packet = ByteBufUtil.getBytes(encoded);
      encoded.release();
      eventBus.publish(new InvalidPacketDetectedEvent(ioContext, packet));
      request.fail(new EncodingFailureException("Outgoing packet failed structure verification"));
      return true;
    }

    enqueue(new Outbound(request, encoded));
    return true;
  }

  /**
   * Like KeyValueMessageHandler, when encoding fails.
   */
  private void handleEncodeFailure(KeyValueRequest<Response> request, Throwable err) {
    KeyValueChannelContext ctx = requireNonNull(channelContext);
    if (err instanceof CollectionNotFoundException && ctx.collectionsEnabled()) {
      ConfigurationProvider cp = ioContext.core().configurationProvider();
      if (cp.collectionRefreshInProgress(request.collectionIdentifier())) {
        RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.COLLECTION_MAP_REFRESH_IN_PROGRESS);
      } else if (cp.config().bucketConfig(request.bucket()) instanceof MemcachedBucketConfig) {
        request.fail(FeatureNotAvailableException.collectionsForMemcached());
      } else {
        handleOutdatedCollection(request, RetryReason.COLLECTION_NOT_FOUND);
      }
      return;
    }
    request.fail(err);
  }

  private void enqueue(Outbound outbound) {
    if (queuedBytes.addAndGet(outbound.size) > highWaterMark) {
      writable = false;
    }
    queue.add(outbound);

    // If the connection closed and its requests were already retried, nobody else will write or retry this one.
    if (requestsRetried && queue.remove(outbound)) {
      failQueued(outbound);
      return;
    }

    if (Thread.currentThread() == readerThread && handlingReadBatch) {
      writerWakeDeferred = true; // the reader wakes the writer when it finishes the batch
    } else {
      wakeWriter();
    }
  }

  /**
   * Wakes the writer if it's waiting for requests.
   * <p>
   * The writer sets {@link #writerParked} before checking the queue one last time and parking, and senders add
   * to the queue before checking the flag, so either the writer sees the request or the sender sees the flag.
   */
  private void wakeWriter() {
    if (writerParked) {
      LockSupport.unpark(writerThread);
    }
  }

  private void dequeued(Outbound outbound) {
    if (queuedBytes.addAndGet(-outbound.size) < lowWaterMark) {
      writable = true;
    }
  }

  private void failQueued(Outbound outbound) {
    outbound.encoded.release();
    dequeued(outbound);
    Throwable cause = closeCause;
    listener.writeFailed(outbound.request(), cause != null ? cause : new IOException("Connection closed"));
  }

  // ---- the writer

  private void runWriter() {
    Thread.currentThread().setName("cb-kv-writer-" + shortChannelId);
    writerThread = Thread.currentThread();
    Throwable failure = null;
    try {
      OutputStream output = requireNonNull(out);
      while (!closed.get()) {
        int written = 0;
        writing = true;
        Outbound outbound;
        while (written < FLUSH_CONSOLIDATION_LIMIT && (outbound = queue.poll()) != null) {
          if (outbound == CLOSE) {
            output.flush();
            return;
          }
          write(output, outbound);
          written++;
        }
        if (written > 0) {
          output.flush();
        }
        checkIdle(output); // might write a NOOP
        writing = false;

        if (written == 0) {
          writerParked = true;
          if (queue.isEmpty() && !closed.get()) {
            if (idleDetectionEnabled()) {
              LockSupport.parkNanos(this, idleCheckIntervalNanos);
            } else {
              LockSupport.park(this); // until there's something to write, or the connection closes
            }
          }
          writerParked = false;
        }
      }
    } catch (Throwable t) {
      failure = t;
    } finally {
      if (close(failure)) {
        reportFailure(failure);
      }
      threadFinished();
    }
  }

  private void write(OutputStream output, Outbound outbound) throws IOException {
    KeyValueRequest<Response> request = outbound.request();
    try {
      if (request.completed()) {
        // For example, it timed out while it was queued. The server needn't do it.
        return;
      }
      RequestSpan dispatchSpan;
      try {
        dispatchSpan = newDispatchSpan(request);
      } catch (Throwable t) {
        // A broken tracer. Like the Netty implementation, fails the request. (Unlike it, doesn't write the request,
        // since nothing would read its response: the Netty implementation closes the connection when it arrives.)
        request.fail(t);
        return;
      }
      int opaque = request.opaque();
      inFlight.put(opaque, new InFlight(request, System.nanoTime(), dispatchSpan));
      captureWritten(outbound.encoded);
      outbound.encoded.readBytes(output, outbound.encoded.readableBytes());
    } finally {
      outbound.encoded.release();
      dequeued(outbound);
    }
  }

  private @Nullable RequestSpan newDispatchSpan(KeyValueRequest<Response> request) {
    if (request.requestSpan() == null) {
      return null;
    }
    RequestTracer tracer = endpointContext.coreResources().requestTracer();
    RequestSpan dispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, request.requestSpan());

    if (!isInternalTracer) {
      try {
        setDispatchSpanAttributes(dispatchSpan, request);
      } catch (Throwable t) {
        dispatchSpan.end();
        throw t;
      }
    }
    return dispatchSpan;
  }

  private void setDispatchSpanAttributes(RequestSpan dispatchSpan, KeyValueRequest<Response> request) {
    setCommonDispatchSpanAttributes(
      request.context().coreResources().tracingDecorator(),
      dispatchSpan,
      channelId,
      ioContext.localHostname(),
      ioContext.localPort(),
      request.context().lastDispatchedToNode().canonical().host(),
      remotePort,
      remoteHostname,
      remotePort,
      null
    );
    request.context().coreResources().tracingDecorator().provideAttr(TracingAttribute.OPERATION_ID, dispatchSpan, request.opaque());
    setCommonKVSpanAttributes(dispatchSpan, request);
  }

  /**
   * Like KeyValueMessageHandler with its IdleStateHandler: if the server has been quiet for half the idle timeout,
   * sends a NOOP (so a healthy connection answers); if it's quiet for all of it, closes the connection.
   */
  private void checkIdle(OutputStream output) throws IOException {
    if (!idleDetectionEnabled()) {
      return;
    }
    long now = System.nanoTime();
    if (!noopSentSinceLastRead) {
      if (now - lastReadNanos >= readerIdleNanos) {
        noopSentNanos = now;
        noopSentSinceLastRead = true;
        writeNoop(output);
      }

    } else if (now - noopSentNanos >= readerIdleNanos) {
      // Nothing for a whole idle period after the NOOP (measured from when it was sent, like the Netty
      // implementation's second idle event), so nothing for at least io.configIdleRedialTimeout in all.
      closeBecauseIdle();
    }
  }

  private boolean idleDetectionEnabled() {
    return readerIdleNanos > 0;
  }

  /**
   * Does what {@link #checkIdle} would, if the writer is stuck writing (because the server isn't reading):
   * closes the connection if the server has been quiet for io.configIdleRedialTimeout. Like the Netty
   * implementation, whose idle check runs whether or not its writes are stuck.
   */
  private void checkStuckWriter() {
    if (writing && System.nanoTime() - lastReadNanos >= 2 * readerIdleNanos) {
      closeBecauseIdle();
    }
  }

  private void closeBecauseIdle() {
    if (closed.get()) {
      return;
    }
    log.warn(
      "Closing KV connection because there was no incoming traffic for {} (io.configIdleRedialTimeout); {}",
      ioConfig.configIdleRedialTimeout(),
      endpointContext
    );
    close(new IOException("No incoming traffic for " + ioConfig.configIdleRedialTimeout()));
  }

  @SuppressWarnings("unchecked")
  private void writeNoop(OutputStream output) throws IOException {
    log.debug("Writing NOOP; {}", endpointContext);

    // Timeout and retry strategy are handled by higher layers,
    // and have no effect when the request is written directly
    // like this. That's okay, because they're not needed here;
    // we're just trying to get a healthy server to send something,
    // so checkIdle doesn't close the connection as idle.
    NoopRequest req = new NoopRequest(
      ioConfig.configIdleRedialTimeout(), // not actually enforced
      endpointContext.core().context(),
      FailFastRetryStrategy.INSTANCE, // not actually enforced
      null
    );
    req.response().thenAccept(res -> log.debug("Read NOOP response; {}", endpointContext));

    KeyValueRequest<Response> request = (KeyValueRequest<Response>) (KeyValueRequest<?>) req;
    Outbound outbound = new Outbound(request, request.encode(ALLOC, request.opaque(), requireNonNull(channelContext)));
    queuedBytes.addAndGet(outbound.size);
    write(output, outbound);
    output.flush();
    log.debug("Wrote NOOP; {}", endpointContext);
  }

  // ---- the reader

  private void runReader() {
    Thread.currentThread().setName("cb-kv-reader-" + shortChannelId);
    readerThread = Thread.currentThread();
    Throwable failure = null;
    try {
      InputStream input = requireNonNull(in);
      while (!closed.get()) {
        ByteBuf packet = nextPacket();
        if (packet == null) {
          if (!frames.hasUnreadBytes()) {
            // Handled everything already read; the next read may wait for the server.
            // (With part of a packet read, the rest is usually on its way, so the batch goes on: requests sent
            // while handling it are written with the ones sent while handling the rest of the batch.)
            endReadBatch();
          }
          fill(input);
          lastReadNanos = System.nanoTime();
          noopSentSinceLastRead = false;
          continue;
        }

        handlingReadBatch = true;
        handle(packet);
      }

      // Closed while handling what was read (for example, because a response said to close the connection).
      // Like the Netty implementation, handles the rest of the responses already read (but doesn't read more),
      // so their requests complete, instead of being retried (or canceled, if they're not idempotent).
      ByteBuf packet;
      while (!discardUnreadResponses && (packet = nextPacket()) != null) {
        handle(packet);
      }
    } catch (Throwable t) {
      failure = t;
    } finally {
      if (close(failure)) {
        reportFailure(failure);
      }
      threadFinished();
    }
  }

  private void handle(ByteBuf packet) {
    try {
      captureRead(packet);
      handleInbound(packet);
    } catch (RuntimeException e) {
      // Like Netty's pipeline, which reports a handler's exception, and carries on.
      eventBus.publish(new GenericFailureDetectedEvent(ioContext, e));
    } finally {
      packet.release();
    }
  }

  /**
   * Called by the reader when it has handled every response it already read. Like the Netty implementation's
   * {@code FlushConsolidationHandler} on {@code channelReadComplete}: wakes the writer once for all the requests
   * sent while handling them.
   */
  private void endReadBatch() {
    handlingReadBatch = false;
    if (writerWakeDeferred) {
      writerWakeDeferred = false;
      wakeWriter();
    }
  }

  /**
   * Returns the next packet, reading from the socket as needed. Its buffer has exactly the packet's bytes,
   * and is only valid until the next read (see {@link KeyValueFrameDecoder}).
   *
   * @throws InvalidPacketException if the packet failed verification
   * (like the Netty implementation's MemcacheProtocolVerificationHandler, it's reported, and the connection closed).
   */
  private ByteBuf readPacket(InputStream input) throws IOException {
    ByteBuf packet;
    while ((packet = nextPacket()) == null) {
      fill(input);
    }
    return packet;
  }

  /**
   * Returns the next complete packet already read, or null if there isn't one.
   *
   * @throws InvalidPacketException if the packet failed verification.
   */
  private @Nullable ByteBuf nextPacket() throws InvalidPacketException {
    ByteBuf packet;
    try {
      packet = frames.next();
    } catch (KeyValueFrameDecoder.InvalidSizeException e) {
      throw invalidPacket(e.header);
    }
    if (packet != null && !MemcacheProtocol.verifyResponse(packet)) {
      throw invalidPacket(packet);
    }
    return packet;
  }

  /**
   * Reads more from the socket, waiting until there's something to read.
   *
   * @throws EOFException if the server closed the connection.
   */
  private void fill(InputStream input) throws IOException {
    if (!frames.fill(input)) {
      throw new EOFException("The server closed the connection.");
    }
  }

  private InvalidPacketException invalidPacket(ByteBuf packet) {
    eventBus.publish(new InvalidPacketDetectedEvent(ioContext, ByteBufUtil.getBytes(packet)));
    return new InvalidPacketException();
  }

  /**
   * Thrown when a packet from the server fails verification.
   */
  static final class InvalidPacketException extends IOException {
    InvalidPacketException() {
      super("Invalid packet from server");
    }
  }

  private void handleInbound(ByteBuf packet) {
    if (ServerPushHandler.isServerPush(packet)) {
      ServerPushHandler.handleServerPush(endpointContext, packet);
      return;
    }
    try {
      decode(packet);
    } finally {
      listener.responseReceived();
    }
  }

  // ---- handling responses (like KeyValueMessageHandler)

  private void decode(ByteBuf response) {
    int opaque = MemcacheProtocol.opaque(response);
    InFlight written = inFlight.remove(opaque);

    if (written == null) {
      handleUnknownResponseReceived(response);
      return;
    }
    KeyValueRequest<Response> request = written.request;

    completeRequestTimings(written, response);

    short statusCode = MemcacheProtocol.status(response);
    ResponseStatus status = MemcacheProtocol.decodeStatus(statusCode);
    ErrorMap.ErrorCode errorCode = status != ResponseStatus.SUCCESS ? decodeErrorCode(statusCode) : null;

    if (errorCode != null) {
      request.errorCode(errorCode);
    }

    boolean errorUnknown = false;
    if (status == ResponseStatus.UNKNOWN) {
      errorUnknown = true;

      if (errorCode != null) {
        eventBus.publish(new KeyValueErrorMapCodeHandledEvent(ioContext, errorCode));
        if (errorCode.attributes().contains(CONN_STATE_INVALIDATED)) {
          closeWithReason(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_CLOSE_INDICATION);
        } else {
          status = KeyValueMessageHandler.statusForErrorCode(errorCode);
        }
      }

      eventBus.publish(new UnknownResponseStatusReceivedEvent(ioContext, statusCode));
    }

    boolean isRangeScanContinue = (Request<?>) request instanceof RangeScanContinueRequest;

    if (errorUnknown && errorCode != null && errorCode.attributes().contains(FETCH_CONFIG)) {
      log.warn("Refreshing cluster config in response to error code {} ; details={}", statusCode, errorCode);
      endpointContext.core().configurationProvider().signalNewTopologyAvailable(bucketName.orElse(null), null, endpointContext.remoteSocket());
    }

    if (status == ResponseStatus.NOT_MY_VBUCKET && !isRangeScanContinue) {
      handleNotMyVbucket(request, response);
    } else if (status == ResponseStatus.UNKNOWN_COLLECTION) {
      handleOutdatedCollection(request, RetryReason.KV_COLLECTION_OUTDATED);
    } else if (errorUnknown && KeyValueMessageHandler.errorMapIndicatesRetry(errorCode)) {
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.KV_ERROR_MAP_INDICATED);
    } else if (KeyValueMessageHandler.statusIndicatesInvalidChannel(status, bucketName.isPresent())) {
      closeWithReason(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_CLOSE_INDICATION);
      // Unlike the Netty implementation (which has already forgotten the request when it retries
      // the ones in flight, so it times out), retry it like the other requests in flight.
      RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    } else {
      retryOrComplete(written, response, status, isRangeScanContinue);
    }
  }

  private void retryOrComplete(InFlight written, ByteBuf response, ResponseStatus status, boolean isRangeScanContinue) {
    KeyValueRequest<Response> request = written.request;
    RetryReason retryReason = KeyValueMessageHandler.statusCodeIndicatesRetry(status, request);
    if (retryReason != null) {
      RetryOrchestrator.maybeRetry(ioContext, request, retryReason);
    } else if (isRangeScanContinue) {
      decodeAndCompleteRangeScanContinue(written, response);
    } else if (!request.completed()) {
      decodeAndComplete(request, response);
    } else {
      ioContext.environment().orphanReporter().report(request);
    }
  }

  private void completeRequestTimings(InFlight written, ByteBuf response) {
    KeyValueRequest<Response> request = written.request;
    long serverTime = MemcacheProtocol.parseServerDurationFromResponse(response);
    request.context().serverLatency(serverTime);
    request.context().dispatchLatency(System.nanoTime() - written.dispatchStartNanos);

    RequestSpan dispatchSpan = written.dispatchSpan;
    if (dispatchSpan != null) {
      if (!isInternalTracer) {
        TracingUtils.setServerDurationAttribute(request.context().coreResources().tracingDecorator(), dispatchSpan, serverTime);
      }
      dispatchSpan.end();
    }
  }

  private void decodeAndComplete(KeyValueRequest<Response> request, ByteBuf response) {
    try {
      Response decoded = request.decode(response, requireNonNull(channelContext));
      request.succeed(decoded);
    } catch (Throwable t) {
      request.fail(new DecodingFailureException(t.getMessage(), t, new GenericRequestErrorContext(request)));
    }
  }

  private void decodeAndCompleteRangeScanContinue(InFlight written, ByteBuf response) {
    KeyValueRequest<Response> request = written.request;
    RangeScanContinueResponse decoded = (RangeScanContinueResponse) request.decode(response, requireNonNull(channelContext));
    if (!request.completed()) {
      request.succeed(decoded);
    }

    ResponseStatus status = decoded.status();
    boolean expectedStatus = status == ResponseStatus.COMPLETE
      || status == ResponseStatus.CONTINUE
      || status == ResponseStatus.SUCCESS;

    if (!expectedStatus) {
      decoded.failFeed(new RangeScanPartitionFailedException(
        "Stream continue failed with non-successful response status",
        status
      ));
      return;
    }

    boolean hasLastItem = status == ResponseStatus.COMPLETE;
    boolean completeStream = hasLastItem || status == ResponseStatus.CONTINUE;
    decoded.feedItems(MemcacheProtocol.body(response).orElse(Unpooled.EMPTY_BUFFER), hasLastItem, completeStream);

    if (decoded.status() == ResponseStatus.SUCCESS) {
      // More responses are coming for this request.
      inFlight.put(request.opaque(), new InFlight(request, written.dispatchStartNanos, null));
    }
  }

  private void handleUnknownResponseReceived(ByteBuf response) {
    eventBus.publish(new UnknownResponseReceivedEvent(ioContext, ByteBufUtil.getBytes(response)));
    // We got a response with an opaque value that we know nothing about. There is clearly something weird
    // going on so to be sure we close the connection to avoid any further weird situations.
    // Unlike the Netty implementation, doesn't handle the rest of the responses already read: they might
    // complete the wrong requests.
    discardUnreadResponses = true;
    closeWithReason(ChannelClosedProactivelyEvent.Reason.KV_RESPONSE_CONTAINED_UNKNOWN_OPAQUE);
  }

  private ErrorMap.@Nullable ErrorCode decodeErrorCode(short statusCode) {
    ErrorMap map = errorMap;
    return map != null ? map.errors().get(statusCode) : null;
  }

  private void handleNotMyVbucket(KeyValueRequest<? extends Response> request, ByteBuf response) {
    request.indicateRejectedWithNotMyVbucket();

    eventBus.publish(new NotMyVbucketReceivedEvent(ioContext, request.partition()));

    String origin = request.context().lastDispatchedTo() != null ? request.context().lastDispatchedTo().host() : null;
    RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.KV_NOT_MY_VBUCKET);

    String body = bodyAsString(response).trim();
    if (body.startsWith("{")) {
      ioContext.core().configurationProvider().proposeBucketConfig(
        new ProposedBucketConfigContext(request.bucket(), body, origin)
      );
    }
  }

  private void handleOutdatedCollection(KeyValueRequest<? extends Response> request, RetryReason retryReason) {
    eventBus.publish(new CollectionOutdatedHandledEvent(
      request.collectionIdentifier(),
      retryReason,
      new KeyValueMessageHandler.OutdatedCollectionContext(ioContext, ioContext.core().configurationProvider().collectionMap())
    ));

    ioContext.core().configurationProvider().refreshCollectionId(request.collectionIdentifier());
    RetryOrchestrator.maybeRetry(ioContext, request, retryReason);
  }

  // ---- reauthentication (like AuthenticationRefreshHandler)

  private void onAuthenticationRefreshSignal() {
    synchronized (this) {
      if (closed.get()) {
        return;
      }
      if (!started) {
        log.info("Deferring reauthentication until handshake completes. Channel: {}", this);
        reauthenticateWhenStarted = true;
        return;
      }
    }
    reauthenticate();
  }

  private void reauthenticate() {
    SingleStepSaslAuthParameters params = endpointContext.authenticator().getSingleStepSaslAuthParameters();
    if (params == null) {
      // The Core only signals when the new authenticator supports it.
      log.warn("Can't reauthenticate, since the authenticator doesn't support it. Channel: {}", this);
      return;
    }

    AuthenticationRefreshHandler.SingleStepAuthRequest req = new AuthenticationRefreshHandler.SingleStepAuthRequest(endpointContext, params);
    req.response()
      .thenAccept(response -> {
        if (!response.status().success()) {
          log.warn("Authentication refresh request failed with status {}; closing channel {}", response.status(), this);
          close(null);
        } else {
          log.info("Authentication refresh succeeded for channel {}", this);
        }
      })
      .exceptionally(t -> {
        log.error("Authentication refresh threw an exception; closing channel {}", this, t);
        close(null);
        return null;
      });

    // Like the Netty implementation, ignores writability. We're unlikely to be making these requests more than
    // once a minute, so there's little danger of running out of memory due to too many of them piling up.
    if (!send(req)) {
      log.debug("Failed to send authentication refresh request, but that's probably okay because channel is closed. Channel: {}", this);
    }
  }

  // ---- traffic capture

  /**
   * Publishes the packet about to be written, if capturing traffic. Doesn't change the buffer.
   */
  private void captureWritten(ByteBuf packet) {
    if (captureTraffic) {
      eventBus.publish(new WriteTrafficCapturedEvent(ioContext, TrafficCaptureHandler.memcacheToString(packet)));
    }
  }

  /**
   * Publishes the packet just read, if capturing traffic. Doesn't change the buffer.
   */
  private void captureRead(ByteBuf packet) {
    if (captureTraffic) {
      eventBus.publish(new ReadTrafficCapturedEvent(ioContext, TrafficCaptureHandler.memcacheToString(packet)));
    }
  }

  // ---- closing

  /**
   * Closes the connection (if it isn't already). The requests it was sending are retried once its threads finish.
   */
  void close() {
    close(null);
  }

  /**
   * @return false if it was already closed.
   */
  private boolean close(@Nullable Throwable cause) {
    if (!closed.compareAndSet(false, true)) {
      return false;
    }
    if (cause != null && !(cause instanceof InvalidPacketException)) {
      log.debug("Closing KV connection {} because: {}", this, cause.toString());
    }
    closeCause = cause;

    synchronized (this) {
      if (authenticationRefreshSubscription != null) {
        authenticationRefreshSubscription.unsubscribe();
        authenticationRefreshSubscription = null;
      }
      if (stuckWriterCheck != null) {
        stuckWriterCheck.cancel(false);
        stuckWriterCheck = null;
      }
    }

    // Close the TCP socket first: closing a TLS socket might wait to send close_notify while the writer
    // is blocked writing.
    closeQuietly(tcpSocket);
    closeQuietly(socket);
    queue.add(CLOSE);
    LockSupport.unpark(writerThread);
    return true;
  }

  /**
   * Reports why the reader or writer failed, when that's what closed the connection. Like the Netty implementation's
   * PipelineErrorHandler, which doesn't report the server closing the connection, either.
   */
  private void reportFailure(@Nullable Throwable failure) {
    if (failure == null || failure instanceof EOFException || failure instanceof InvalidPacketException) {
      return; // the server closed the connection, or the packet was already reported
    }
    if (failure instanceof SSLException && !OkHttpTlsHelper.connectionFailedDuringTls(failure)) {
      endpointContext.authenticationStatus(AuthenticationStatus.FAILED);
      eventBus.publish(new SecureConnectionFailedEvent(ioContext, (SSLException) failure));
    } else {
      eventBus.publish(new GenericFailureDetectedEvent(ioContext, failure));
    }
  }

  private void closeWithReason(ChannelClosedProactivelyEvent.Reason reason) {
    // Only for the close (the reader might find more reasons, handling the responses it already read).
    if (close(new IOException("Closed proactively: " + reason))) {
      eventBus.publish(new ChannelClosedProactivelyEvent(ioContext, reason));
    }
  }

  private void threadFinished() {
    if (runningThreads.decrementAndGet() != 0) {
      return;
    }

    // Both threads are done, so nothing else writes or reads the requests.
    requestsRetried = true;

    Outbound unwritten;
    while ((unwritten = queue.poll()) != null) {
      if (unwritten != CLOSE) {
        failQueued(unwritten);
      }
    }

    int requestsInFlight = inFlight.size();
    for (InFlight written : inFlight.values()) {
      if (written.dispatchSpan != null) {
        written.dispatchSpan.end();
      }
      RetryOrchestrator.maybeRetry(ioContext, written.request, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    }
    inFlight.clear();

    listener.closed(this, requestsInFlight);
  }

  @Override
  public String toString() {
    Socket s = socket;
    return "KeyValueConnection{" +
      "id=0x" + shortChannelId +
      ", local=" + redactSystem(s == null ? null : s.getLocalSocketAddress()) +
      ", remote=" + redactSystem(s == null ? remoteHostname + ":" + remotePort : s.getRemoteSocketAddress()) +
      ", tls=" + (s instanceof SSLSocket) +
      '}';
  }
}
