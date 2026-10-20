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
import com.couchbase.client.core.cnc.events.io.ChannelClosedProactivelyEvent;
import com.couchbase.client.core.cnc.events.io.GenericFailureDetectedEvent;
import com.couchbase.client.core.cnc.events.io.InvalidPacketDetectedEvent;
import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.TrafficCaptureHandler;
import com.couchbase.client.core.io.netty.kv.AuthenticationRefreshHandler;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.KeyValueMessageHandler;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.ServerPushHandler;
import com.couchbase.client.core.io.netty.kv.sasl.SingleStepSaslAuthParameters;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.KeyValueOutboundQueue.Outbound;
import com.couchbase.client.core.service.KeyValuePacketReader.InvalidPacketException;
import com.couchbase.client.core.util.CbThreads;
import com.couchbase.client.core.util.SynchronousEventBus;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.io.netty.TracingUtils.newKeyValueDispatchSpan;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.service.KeyValueSocketConnector.closeQuietly;
import static java.util.Objects.requireNonNull;

/**
 * One KV connection that doesn't use Netty: a socket (with TLS, if enabled) and the threads that use it.
 * Does what the Netty pipeline's KV handlers do (see {@link com.couchbase.client.core.endpoint.KeyValueEndpoint}),
 * with the same events, retries and errors.
 * <p>
 * Lifecycle: {@link KeyValueConnectionOpener} connects and does the handshake, then creates the connection.
 * {@link #start} starts two threads, which run until the connection closes:
 * <ul>
 *   <li>The writer encodes queued requests (like the Netty implementation's event loop), and writes them to
 *       the socket, up to {@value #FLUSH_CONSOLIDATION_LIMIT} per flush, flushing when the queue is empty.
 *   <li>The reader reads packets from the server, and completes (or retries) the requests they answer
 *       (see {@link KeyValueResponseHandler}).
 * </ul>
 * A periodic check (see {@link #periodicCheck}) sends a NOOP when the server has been quiet for a while
 * (a third of the idle timeout), and closes the connection if the server stays quiet (see {@link KeyValueIdleDetector}).
 * <p>
 * Like the Netty implementation's {@code FlushConsolidationHandler}, requests sent by the reader thread while it's
 * handling a batch of responses (for example, from a callback that sends the next request when a response arrives)
 * are written together: the reader wakes the writer once, when it has handled every response it already read
 * (or every {@value #FLUSH_CONSOLIDATION_LIMIT} requests, if the batch goes on), instead of once per request.
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
   * Runs {@link #periodicCheck} for every connection. One thread is plenty: the checks are quick.
   */
  private static final ScheduledThreadPoolExecutor periodicChecks = newPeriodicChecks();

  private static ScheduledThreadPoolExecutor newPeriodicChecks() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, CbThreads.platformThreadFactory("cb-kv-idle-"));
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  static final int BUFFER_SIZE = 64 * 1024;

  /**
   * The most requests the writer writes before flushing, and the most requests the reader thread sends while
   * handling a batch of responses before it wakes the writer. Like the Netty implementation's
   * {@code FlushConsolidationHandler} (which uses the same default for both).
   */
  private static final int FLUSH_CONSOLIDATION_LIMIT = 256;

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
   * A request that was written, and is waiting for its response.
   */
  static final class InFlight {
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
  private final Listener listener;
  private final EventBus eventBus;
  private final IoConfig ioConfig;
  private final boolean isInternalTracer;

  /**
   * The connection's ID (see {@link KeyValueConnectionOpener}).
   */
  private final String shortChannelId;
  private final String channelId;

  /**
   * How long each connect step took (see {@link #timings()}). Written by the opener.
   */
  private final SortedMap<String, Duration> timings;

  private final Socket tcpSocket;
  private final Socket socket;

  // The host name and port the connection is to (not the resolved address), for tracing.
  private final String remoteHostname;
  private final int remotePort;

  private final OutputStream out;
  private final KeyValuePacketReader packets;
  private final IoContext ioContext;
  private final KeyValueChannelContext channelContext;
  private final KeyValueResponseHandler responses;
  private final TrafficCapture capture;
  private final AuthenticationRefresh authenticationRefresh;

  // For toString(), which is often called once the socket is closed (when it no longer knows its local address).
  private final SocketAddress localAddress;
  private final SocketAddress remoteAddress;

  private final KeyValueOutboundQueue queue;

  /**
   * Written requests waiting for their responses, by opaque.
   */
  private final Map<Integer, InFlight> inFlight = new ConcurrentHashMap<>();

  private final KeyValueIdleDetector idle;

  // Guarded by "this".
  private @Nullable ScheduledFuture<?> periodicCheck;

  // Closing. close() sets "closed" (once), and closes the sockets, which stops the reader and the writer.
  // The last of them to finish fails or retries every request still queued or in flight, then tells the listener
  // (see threadFinished()). So every request is written and answered, retried, or failed, exactly once.

  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Why the connection closed (null if {@link #close()} was called), for the requests it couldn't write.
   */
  private volatile @Nullable Throwable closeCause;

  /**
   * The reader and the writer. The last one to finish retries the connection's requests.
   */
  private final AtomicInteger runningThreads = new AtomicInteger(2);

  /**
   * Whether the reader should ignore the responses it already read, once the connection is closed
   * (because they might complete the wrong requests; see {@link KeyValueResponseHandler}). Only accessed by the reader.
   */
  private boolean discardUnreadResponses;

  /**
   * Called by {@link KeyValueConnectionOpener}, once it has connected and done the handshake.
   *
   * @param endpointContext the endpoint's context (at the start of this connect attempt).
   * @param tcpSocket the TCP socket (the same as {@code socket} unless TLS is enabled).
   * @param socket the socket to read and write.
   * @param timings how long each connect step took.
   * @param authenticationRefresh this connection's subscription to reauthentication signals; it takes it over.
   */
  KeyValueConnection(
    EndpointContext endpointContext,
    Listener listener,
    Socket tcpSocket,
    Socket socket,
    OutputStream out,
    KeyValuePacketReader packets,
    IoContext ioContext,
    KeyValueChannelContext channelContext,
    @Nullable ErrorMap errorMap,
    String shortChannelId,
    String channelId,
    SortedMap<String, Duration> timings,
    AuthenticationRefresh authenticationRefresh,
    TrafficCapture capture
  ) {
    this.endpointContext = requireNonNull(endpointContext);
    this.listener = requireNonNull(listener);
    this.tcpSocket = requireNonNull(tcpSocket);
    this.socket = requireNonNull(socket);
    this.out = requireNonNull(out);
    this.packets = requireNonNull(packets);
    this.ioContext = requireNonNull(ioContext);
    this.channelContext = requireNonNull(channelContext);
    this.shortChannelId = requireNonNull(shortChannelId);
    this.channelId = requireNonNull(channelId);
    this.timings = requireNonNull(timings);
    this.authenticationRefresh = requireNonNull(authenticationRefresh);
    this.capture = requireNonNull(capture);
    this.localAddress = socket.getLocalSocketAddress();
    this.remoteAddress = socket.getRemoteSocketAddress();
    this.remoteHostname = endpointContext.remoteSocket().host();
    this.remotePort = endpointContext.remoteSocket().port();

    this.eventBus = endpointContext.environment().eventBus();
    this.ioConfig = endpointContext.environment().ioConfig();
    this.isInternalTracer = CbTracing.isInternalTracer(endpointContext.coreResources().requestTracer());

    this.queue = new KeyValueOutboundQueue(
      ioConfig.highWaterMark().bytesAsInt(),
      ioConfig.lowWaterMark().bytesAsInt(),
      FLUSH_CONSOLIDATION_LIMIT
    );
    this.idle = new KeyValueIdleDetector(ioConfig.configIdleRedialTimeout(), new KeyValueIdleDetector.Connection() {
      @Override
      public void sendNoop() {
        KeyValueConnection.this.sendNoop();
      }

      @Override
      public void closeBecauseIdle(String reason) {
        KeyValueConnection.this.closeBecauseIdle(reason);
      }
    });

    this.responses = new KeyValueResponseHandler(
      endpointContext,
      ioContext,
      channelContext,
      errorMap,
      inFlight,
      isInternalTracer,
      (reason, discard) -> {
        if (discard) {
          discardUnreadResponses = true;
        }
        closeWithReason(reason);
      }
    );
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

  SocketAddress localAddress() {
    return localAddress;
  }

  SocketAddress remoteAddress() {
    return remoteAddress;
  }

  boolean isClosed() {
    return closed.get();
  }

  /**
   * Returns false if the connection has too many bytes waiting to be written (like Netty's channel writability,
   * with the same high and low water marks).
   */
  boolean isWritable() {
    return queue.isWritable() && !closed.get();
  }

  /**
   * The number of requests written that are waiting for their responses.
   */
  int inFlightCount() {
    return inFlight.size();
  }

  // ---- starting

  /**
   * Starts the reader and writer. Call once.
   */
  void start() {
    idle.start();
    startThread(this::runReader);
    startThread(this::runWriter);

    synchronized (this) {
      // Not once closed: nothing would cancel it.
      if (!closed.get()) {
        periodicCheck = periodicChecks.scheduleWithFixedDelay(
          this::periodicCheck, idle.checkIntervalNanos(), idle.checkIntervalNanos(), TimeUnit.NANOSECONDS
        );
      }
    }

    if (authenticationRefresh.start(this::onAuthenticationRefreshSignal)) {
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
   * Queues the request to be written (the writer encodes it).
   *
   * @return false if the connection is closed, so the request wasn't sent (the caller should retry it).
   */
  @SuppressWarnings("unchecked")
  boolean send(KeyValueRequest<? extends Response> req) {
    if (closed.get()) {
      return false;
    }

    KeyValueRequest<Response> request = (KeyValueRequest<Response>) req;
    enqueue(new Outbound(request, KeyValueOutboundQueue.UNENCODED_REQUEST_SIZE));
    return true;
  }

  /**
   * Called by the writer. Returns the encoded request, or null if encoding failed (and the request was failed
   * or retried).
   */
  private @Nullable ByteBuf encode(KeyValueRequest<Response> request) {
    ByteBuf encoded;
    try {
      encoded = request.encode(ALLOC, request.opaque(), channelContext);
    } catch (Throwable err) {
      handleEncodeFailure(request, err);
      return null;
    }

    if (!MemcacheProtocol.verifyRequest(encoded)) {
      // Reported like MemcacheProtocolVerificationHandler, but the request fails instead of the connection closing.
      // Nothing was written, so the connection is still good. A retry would encode the same bad packet.
      byte[] packet = ByteBufUtil.getBytes(encoded);
      encoded.release();
      eventBus.publish(new InvalidPacketDetectedEvent(ioContext, packet));
      request.fail(new EncodingFailureException("Outgoing packet failed structure verification"));
      return null;
    }
    return encoded;
  }

  /**
   * Like KeyValueMessageHandler, when encoding fails.
   */
  private void handleEncodeFailure(KeyValueRequest<Response> request, Throwable err) {
    KeyValueChannelContext ctx = channelContext;
    if (err instanceof CollectionNotFoundException && ctx.collectionsEnabled()) {
      ConfigurationProvider cp = ioContext.core().configurationProvider();
      if (cp.collectionRefreshInProgress(request.collectionIdentifier())) {
        RetryOrchestrator.maybeRetry(ioContext, request, RetryReason.COLLECTION_MAP_REFRESH_IN_PROGRESS);
      } else if (cp.config().bucketConfig(request.bucket()) instanceof MemcachedBucketConfig) {
        request.fail(FeatureNotAvailableException.collectionsForMemcached());
      } else {
        responses.handleOutdatedCollection(request, RetryReason.COLLECTION_NOT_FOUND);
      }
      return;
    }
    request.fail(err);
  }

  private void enqueue(Outbound outbound) {
    if (!queue.add(outbound)) {
      // The connection closed, and its requests were already retried: nobody else will write or retry this one.
      failQueued(outbound);
    }
  }

  private void failQueued(Outbound outbound) {
    queue.written(outbound);
    Throwable cause = closeCause;
    listener.writeFailed(outbound.request(), cause != null ? cause : new IOException("Connection closed"));
  }

  // ---- the writer

  private void runWriter() {
    Thread.currentThread().setName("cb-kv-writer-" + shortChannelId);
    queue.writerStarted();
    Throwable failure = null;
    try {
      OutputStream output = out;
      while (!closed.get()) {
        int written = 0;
        Outbound outbound;
        while (written < FLUSH_CONSOLIDATION_LIMIT && (outbound = queue.poll()) != null) {
          if (outbound == KeyValueOutboundQueue.CLOSE) {
            output.flush();
            return;
          }
          try {
            write(output, outbound);
          } finally {
            queue.written(outbound);
          }
          written++;
        }
        if (written > 0) {
          output.flush();
        }

        if (written == 0) {
          queue.awaitRequests(); // until there's something to write, or the connection closes
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
    if (request.completed()) {
      // For example, it timed out while it was queued. The server needn't do it (and it needn't be encoded).
      return;
    }
    ByteBuf encoded = encode(request);
    if (encoded == null) {
      return;
    }
    try {
      RequestSpan dispatchSpan;
      try {
        dispatchSpan = newKeyValueDispatchSpan(
          request,
          endpointContext,
          isInternalTracer,
          channelId,
          ioContext,
          remoteHostname,
          remotePort
        );
      } catch (Throwable t) {
        // A broken tracer. Like the Netty implementation, fails the request. (Unlike it, doesn't write the request,
        // since nothing would read its response: the Netty implementation closes the connection when it arrives.)
        request.fail(t);
        return;
      }
      int opaque = request.opaque();
      inFlight.put(opaque, new InFlight(request, System.nanoTime(), dispatchSpan));
      capture.written(encoded);
      encoded.readBytes(output, encoded.readableBytes());
    } finally {
      encoded.release();
    }
  }

  private void closeBecauseIdle(String reason) {
    if (closed.get()) {
      return;
    }
    log.warn("Closing KV connection because the server has gone quiet: {}; {}", reason, endpointContext);
    close(new IOException(reason));
  }

  private void sendNoop() {
    log.debug("Sending NOOP; {}", endpointContext);

    // Its timeout isn't enforced (it's not sent through the Core), and that's okay: we're just trying to get
    // a healthy server to send something, so the idle detector doesn't close the connection. If the connection
    // closes first, the retry strategy cancels it.
    NoopRequest req = new NoopRequest(
      Duration.ofNanos(idle.noopIntervalNanos()),
      endpointContext.core().context(),
      FailFastRetryStrategy.INSTANCE,
      null
    );
    req.response().thenAccept(res -> log.debug("Read NOOP response; {}", endpointContext));
    send(req);
  }

  /**
   * Runs every second or so (see {@link KeyValueIdleDetector#checkIntervalNanos}): checks whether the connection
   * has been idle too long, and wakes the writer if requests are waiting to be written. (They only wait if the reader
   * held back waking the writer, while it waits for the rest of a response; see {@link KeyValueOutboundQueue}.
   * So they wait about a second at most, even if that response never comes.)
   */
  private void periodicCheck() {
    try {
      idle.check();
      queue.wakeWriterIfNotEmpty();
    } catch (Throwable t) {
      // Otherwise the executor would silently stop running it.
      log.warn("Periodic check of KV connection {} failed", this, t);
    }
  }

  // ---- the reader

  private void runReader() {
    Thread.currentThread().setName("cb-kv-reader-" + shortChannelId);
    queue.readerStarted();
    Throwable failure = null;
    try {
      while (!closed.get()) {
        ByteBuf packet = packets.next();
        if (packet == null) {
          if (!packets.hasUnreadBytes()) {
            // Handled everything already read; the next read may wait for the server.
            // (With part of a packet read, the rest is usually on its way, so the batch goes on: requests sent
            // while handling it are written with the ones sent while handling the rest of the batch.)
            queue.endReadBatch();
          }
          packets.fill();
          idle.read();
          continue;
        }

        queue.beginReadBatch();
        handle(packet);
      }

      // Closed while handling what was read (for example, because a response said to close the connection).
      // Like the Netty implementation, handles the rest of the responses already read (but doesn't read more),
      // so their requests complete, instead of being retried (or canceled, if they're not idempotent).
      ByteBuf packet;
      while (!discardUnreadResponses && (packet = packets.next()) != null) {
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
      capture.read(packet);
      handleInbound(packet);
    } catch (RuntimeException e) {
      // Like Netty's pipeline, which reports a handler's exception, and carries on.
      eventBus.publish(new GenericFailureDetectedEvent(ioContext, e));
    } finally {
      packet.release();
    }
  }

  private void handleInbound(ByteBuf packet) {
    if (ServerPushHandler.isServerPush(packet)) {
      ServerPushHandler.handleServerPush(endpointContext, packet);
      return;
    }
    try {
      responses.handle(packet);
    } finally {
      listener.responseReceived();
    }
  }

  // ---- reauthentication (like AuthenticationRefreshHandler)

  private void onAuthenticationRefreshSignal() {
    if (!closed.get()) {
      reauthenticate();
    }
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

  // ---- traffic capture, and reauthentication signals (also used by the opener)

  /**
   * Publishes each packet written and read, if the config asks for KV's (io.servicesToCapture),
   * like the Netty implementation's TrafficCaptureHandler.
   */
  static final class TrafficCapture {
    private final IoContext ioContext;
    private final boolean enabled;

    TrafficCapture(IoContext ioContext) {
      this.ioContext = requireNonNull(ioContext);
      this.enabled = ioContext.environment().ioConfig().servicesToCapture().contains(ServiceType.KV);
    }

    /**
     * Publishes the packet about to be written. Doesn't change the buffer.
     */
    void written(ByteBuf packet) {
      if (enabled) {
        ioContext.environment().eventBus().publish(new WriteTrafficCapturedEvent(ioContext, TrafficCaptureHandler.memcacheToString(packet)));
      }
    }

    /**
     * Publishes the packet just read. Doesn't change the buffer.
     */
    void read(ByteBuf packet) {
      if (enabled) {
        ioContext.environment().eventBus().publish(new ReadTrafficCapturedEvent(ioContext, TrafficCaptureHandler.memcacheToString(packet)));
      }
    }
  }

  /**
   * A connection's subscription to reauthentication signals (see {@code Core.authenticationRefreshTriggers}).
   * The opener subscribes before connecting, so a signal during the handshake isn't missed. Signals before the
   * connection starts are remembered, and acted on once it does (like the Netty implementation's
   * AuthenticationRefreshHandler, which defers them until the handshake completes).
   */
  static final class AuthenticationRefresh {
    private final SynchronousEventBus.Subscription subscription;

    // Guarded by "this".
    private @Nullable Runnable onSignal;
    private boolean signalled;

    AuthenticationRefresh(EndpointContext endpointContext) {
      // Handled on another thread (like the Netty implementation, which switches to the channel's event loop),
      // so the publisher isn't held up, and closing the connection can unsubscribe.
      this.subscription = endpointContext.core().authenticationRefreshTriggers.subscribe(
        signal -> ioThreads.execute(this::signal)
      );
    }

    private void signal() {
      Runnable action;
      synchronized (this) {
        action = onSignal;
        if (action == null) {
          log.info("Deferring reauthentication until handshake completes.");
          signalled = true;
          return;
        }
      }
      action.run();
    }

    /**
     * From now on, runs the action for each signal.
     *
     * @return true if there was a signal before now (which the caller should act on).
     */
    synchronized boolean start(Runnable onSignal) {
      this.onSignal = requireNonNull(onSignal);
      boolean result = signalled;
      signalled = false;
      return result;
    }

    void unsubscribe() {
      subscription.unsubscribe();
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

    authenticationRefresh.unsubscribe();
    synchronized (this) {
      if (periodicCheck != null) {
        periodicCheck.cancel(false);
        periodicCheck = null;
      }
    }

    // Close the TCP socket first: closing a TLS socket might wait to send close_notify while the writer
    // is blocked writing.
    closeQuietly(tcpSocket);
    closeQuietly(socket);
    queue.close();
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
    if (failure instanceof VirtualMachineError) {
      // For example, OutOfMemoryError. So it stands out from ordinary connection failures (the event is a WARN).
      log.error("KV connection {} failed with a JVM error; {}", this, endpointContext, failure);
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
    for (Outbound unwritten : queue.drain()) {
      failQueued(unwritten);
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
    return "KeyValueConnection{" +
      "id=0x" + shortChannelId +
      ", local=" + redactSystem(localAddress) +
      ", remote=" + redactSystem(remoteAddress) +
      ", tls=" + (socket instanceof SSLSocket) +
      '}';
  }
}
