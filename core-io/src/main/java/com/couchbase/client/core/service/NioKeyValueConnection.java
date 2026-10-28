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
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestWriter;
import com.couchbase.client.core.io.netty.kv.AuthenticationRefreshHandler;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.ServerPushHandler;
import com.couchbase.client.core.io.netty.kv.sasl.SingleStepSaslAuthParameters;
import com.couchbase.client.core.io.repackaged.tlschannel.ClientTlsChannel;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsReadException;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsWriteException;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.KeyValueOutboundQueue.Outbound;
import com.couchbase.client.core.service.KeyValuePacketReader.InvalidPacketException;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.core.io.netty.TracingUtils.newKeyValueDispatchSpan;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.service.KeyValueSocketConnector.closeQuietly;
import static java.util.Objects.requireNonNull;

/**
 * One KV connection that doesn't use Netty, read and written by a shared {@link KeyValueEventLoop}: a non-blocking
 * {@link SocketChannel}, with TLS (if enabled) by an {@link javax.net.ssl.SSLEngine} (see {@link ClientTlsChannel}).
 * Does what the Netty pipeline's KV handlers do, with the same events, retries and errors.
 * <p>
 * Lifecycle: {@link NioKeyValueConnectionOpener} connects and does the handshake (on the connecting thread),
 * then creates the connection. {@link #start} registers it with its event loop, which from then on:
 * <ul>
 *   <li>Reads packets when the channel is readable, and completes (or retries) the requests they answer
 *       (see {@link KeyValueResponseHandler}).
 *   <li>Encodes queued requests, and writes them (see {@link #flush}). Requests sent on the loop's thread
 *       (for example, by a response callback) are written at the end of the loop's pass, together; requests sent
 *       by other threads are handed to the loop, which writes them as soon as it can.
 *   <li>When the channel can't take more, keeps what's left (see {@link ChannelOutput}), stops taking requests
 *       off the queue, and waits until the channel is writable again. Meanwhile, the queue fills, and the
 *       connection stops being writable (like Netty's channel writability).
 *   <li>Runs the periodic check: sends a NOOP when the server has been quiet for a while (a third of the idle timeout),
 *       and closes the connection if the server stays quiet (see {@link KeyValueIdleDetector}).
 * </ul>
 * When the connection closes (because reading or writing failed, the server closed it, or {@link #close} was called),
 * the loop handles the responses already read (unless they must be discarded), closes the channel, retries the
 * requests that were queued or in flight, then tells the listener. All of that happens on the loop's thread,
 * so every request is written and answered, retried, or failed, exactly once.
 * <p>
 * Thread-safe. Reading, writing and closing happen on the loop's thread; other threads only queue requests,
 * and ask the loop to close the connection.
 */
@NullMarked
final class NioKeyValueConnection implements KeyValueEndpointConnection {
  private static final Logger log = LoggerFactory.getLogger(NioKeyValueConnection.class);

  /**
   * The size of the request writer's buffer.
   */
  private static final int BUFFER_SIZE = 64 * 1024;

  /**
   * The most requests written before the writer's buffer is written to the channel, like the Netty
   * implementation's {@code FlushConsolidationHandler}.
   */
  private static final int FLUSH_CONSOLIDATION_LIMIT = 256;

  /**
   * The most reads from the channel in one go, so one busy connection can't hold up the loop's others.
   * Like the Netty implementation's (its channels' {@code maxMessagesPerRead}).
   */
  private static final int MAX_READS_PER_PASS = 16;

  private final EndpointContext endpointContext;
  private final Listener listener;
  private final EventBus eventBus;
  private final boolean isInternalTracer;
  private final KeyValueEventLoop loop;

  /**
   * The connection's ID (see {@link NioKeyValueConnectionOpener}).
   */
  private final String shortChannelId;
  private final String channelId;

  /**
   * How long each connect step took (see {@link #timings()}). Written by the opener.
   */
  private final SortedMap<String, Duration> timings;

  private final SocketChannel tcp;
  private final @Nullable ClientTlsChannel tls;

  // The host name and port the connection is to (not the resolved address), for tracing.
  private final String remoteHostname;
  private final int remotePort;

  private final ChannelOutput output = new ChannelOutput();
  private final KeyValueRequestWriter requestWriter;
  private final KeyValuePacketReader packets;
  private final IoContext ioContext;
  private final KeyValueChannelContext channelContext;
  private final KeyValueResponseHandler responses;
  private final KeyValueTrafficCapture capture;
  private final KeyValueAuthenticationRefresh authenticationRefresh;

  // For toString(), which is often called once the channel is closed.
  private final SocketAddress localAddress;
  private final SocketAddress remoteAddress;

  /**
   * Requests waiting to be written, and writability. (The connection doesn't use the queue's thread wake-ups.)
   */
  private final KeyValueOutboundQueue queue;

  /**
   * Written requests waiting for their responses, by opaque.
   */
  private final Map<Integer, KeyValueInFlight> inFlight = new ConcurrentHashMap<>();

  private final KeyValueIdleDetector idle;

  /**
   * Whether the connection is waiting for the loop to write its queued requests (so it's scheduled once at a time).
   */
  private final AtomicBoolean flushScheduled = new AtomicBoolean();

  /**
   * Writes the queued requests; submitted to the loop by other threads (see {@link KeyValueEventLoop#scheduleFlush}).
   */
  final Runnable flushTask = this::flush;

  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Why the connection closed (null if {@link #close()} was called), for the requests it couldn't write.
   */
  private volatile @Nullable Throwable closeCause;

  private volatile boolean started;

  // Only accessed by the loop's thread.

  private @Nullable SelectionKey key;

  /**
   * Whether reading needs the channel to be writable first (a TLS channel answering a key update, for example).
   */
  private boolean readNeedsWrite;

  /**
   * Whether the loop should ignore the responses already read, once the connection is closed
   * (because they might complete the wrong requests; see {@link KeyValueResponseHandler}).
   */
  private boolean discardUnreadResponses;

  /**
   * Whether the connection has finished closing (see {@link #finishClosing}).
   */
  private boolean finished;

  /**
   * When the connection's next periodic check is due. Managed by the loop.
   */
  long nextCheckNanos;

  /**
   * Called by {@link NioKeyValueConnectionOpener}, once it has connected and done the handshake.
   *
   * @param endpointContext the endpoint's context (at the start of this connect attempt).
   * @param tcp the connected, non-blocking channel. Not registered with any selector.
   * @param tls the TLS channel over it, if TLS is enabled.
   * @param packets reads the channel (TLS, if enabled), and has what the handshake read ahead.
   * @param timings how long each connect step took.
   * @param authenticationRefresh this connection's subscription to reauthentication signals; it takes it over.
   */
  NioKeyValueConnection(
    EndpointContext endpointContext,
    Listener listener,
    KeyValueEventLoop loop,
    SocketChannel tcp,
    @Nullable ClientTlsChannel tls,
    KeyValuePacketReader packets,
    IoContext ioContext,
    KeyValueChannelContext channelContext,
    @Nullable ErrorMap errorMap,
    String shortChannelId,
    String channelId,
    SortedMap<String, Duration> timings,
    KeyValueAuthenticationRefresh authenticationRefresh,
    KeyValueTrafficCapture capture
  ) {
    this.endpointContext = requireNonNull(endpointContext);
    this.listener = requireNonNull(listener);
    this.loop = requireNonNull(loop);
    this.tcp = requireNonNull(tcp);
    this.tls = tls;
    this.packets = requireNonNull(packets);
    this.ioContext = requireNonNull(ioContext);
    this.channelContext = requireNonNull(channelContext);
    this.requestWriter = new KeyValueRequestWriter(output, BUFFER_SIZE, channelContext);
    this.shortChannelId = requireNonNull(shortChannelId);
    this.channelId = requireNonNull(channelId);
    this.timings = requireNonNull(timings);
    this.authenticationRefresh = requireNonNull(authenticationRefresh);
    this.capture = requireNonNull(capture);
    this.localAddress = tcp.socket().getLocalSocketAddress();
    this.remoteAddress = tcp.socket().getRemoteSocketAddress();
    this.remoteHostname = endpointContext.remoteSocket().host();
    this.remotePort = endpointContext.remoteSocket().port();

    this.eventBus = endpointContext.environment().eventBus();
    IoConfig ioConfig = endpointContext.environment().ioConfig();
    this.isInternalTracer = CbTracing.isInternalTracer(endpointContext.coreResources().requestTracer());

    this.queue = new KeyValueOutboundQueue(ioConfig.highWaterMark().bytesAsInt(), ioConfig.lowWaterMark().bytesAsInt());
    this.idle = new KeyValueIdleDetector(ioConfig.configIdleRedialTimeout(), new KeyValueIdleDetector.Connection() {
      @Override
      public void sendNoop() {
        NioKeyValueConnection.this.sendNoop();
      }

      @Override
      public void closeBecauseIdle(String reason) {
        NioKeyValueConnection.this.closeBecauseIdle(reason);
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

  @Override
  public String channelId() {
    return channelId;
  }

  @Override
  public String shortChannelId() {
    return shortChannelId;
  }

  @Override
  public SortedMap<String, Duration> timings() {
    synchronized (timings) {
      return new TreeMap<>(timings);
    }
  }

  @Override
  public SocketAddress localAddress() {
    return localAddress;
  }

  @Override
  public SocketAddress remoteAddress() {
    return remoteAddress;
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public boolean isWritable() {
    return queue.isWritable() && !closed.get();
  }

  @Override
  public int inFlightCount() {
    return inFlight.size();
  }

  // ---- starting

  /**
   * Registers the connection with its event loop. Call once.
   */
  @Override
  public void start() {
    started = true;
    idle.start();
    loop.execute(this::register);

    if (authenticationRefresh.start(this::onAuthenticationRefreshSignal)) {
      log.info("Immediately reauthenticating the channel because a new authenticator was provided during the handshake process. Channel: {}", this);
      reauthenticate();
    }
  }

  private void register() {
    if (closed.get()) {
      return; // finishClosing() is on its way
    }
    try {
      key = loop.register(this, tcp);
      // The handshake may have read ahead (and a TLS channel may hold what it read), which the selector doesn't know.
      read();
      flush();
    } catch (Throwable t) {
      fail(t);
    }
  }

  long checkIntervalNanos() {
    return idle.checkIntervalNanos();
  }

  // ---- sending

  @Override
  @SuppressWarnings("unchecked")
  public boolean send(KeyValueRequest<? extends Response> req) {
    if (closed.get()) {
      return false;
    }

    KeyValueRequest<Response> request = (KeyValueRequest<Response>) req;
    Outbound outbound = new Outbound(request, KeyValueOutboundQueue.UNENCODED_REQUEST_SIZE);
    if (!queue.add(outbound)) {
      // The connection closed, and its requests were already retried: nobody else will write or retry this one.
      failQueued(outbound);
      return true;
    }
    if (flushScheduled.compareAndSet(false, true)) {
      loop.scheduleFlush(this);
    }
    return true;
  }

  private void failQueued(Outbound outbound) {
    queue.written(outbound);
    Throwable cause = closeCause;
    listener.writeFailed(outbound.request(), cause != null ? cause : new IOException("Connection closed"));
  }

  // ---- writing (on the loop's thread)

  /**
   * Writes the queued requests, until the queue is empty, or the channel can't take more. Called by the loop.
   */
  void flush() {
    flushScheduled.set(false);
    if (key == null || finished) {
      return; // not registered yet (register() flushes), or done
    }
    try {
      writeQueued();
    } catch (Throwable t) {
      fail(t);
    }
  }

  private void writeQueued() throws IOException {
    if (output.blocked()) {
      return; // the loop resumes once the channel is writable
    }
    int written = 0;
    Outbound outbound;
    while (!closed.get() && (outbound = queue.poll()) != null) {
      try {
        write(outbound);
      } finally {
        queue.written(outbound);
      }
      if (++written == FLUSH_CONSOLIDATION_LIMIT) {
        written = 0;
        requestWriter.flush();
      }
      if (output.blocked()) {
        break;
      }
    }
    requestWriter.flush();
    updateInterest();
  }

  private void write(Outbound outbound) throws IOException {
    KeyValueRequest<Response> request = outbound.request();
    if (request.completed()) {
      // For example, it timed out while it was queued. The server needn't do it (and it needn't be encoded).
      return;
    }
    writeDirectly(request);
  }

  /**
   * Encodes the request straight into the request writer's buffer (see {@link KeyValueRequestWriter}).
   * Nothing of the request reaches the channel until it's in flight, so its response can't arrive first.
   */
  private void writeDirectly(KeyValueRequest<Response> request) throws IOException {
    try {
      requestWriter.makeRoomForRequest();
    } catch (IOException e) {
      // The request is neither queued nor in flight, so closing the connection wouldn't retry it.
      listener.writeFailed(request, e);
      throw e;
    }

    try {
      try {
        request.encodeTo(requestWriter);
      } catch (Throwable err) {
        requestWriter.abortRequest();
        handleEncodeFailure(request, err);
        return;
      }

      try {
        requestWriter.completeRequestHead(request.opaque());
      } catch (IllegalStateException e) {
        // Reported like MemcacheProtocolVerificationHandler, but the request fails instead of the connection closing.
        // Nothing was written, so the connection is still good. A retry would encode the same bad packet.
        byte[] packet = requestWriter.requestBytes();
        requestWriter.abortRequest();
        eventBus.publish(new InvalidPacketDetectedEvent(ioContext, packet));
        request.fail(new EncodingFailureException("Outgoing packet failed structure verification", e));
        return;
      }

      if (!dispatch(request)) {
        return;
      }
      if (capture.enabled()) {
        capture.written(requestWriter.requestBytes());
      }
      requestWriter.finishRequest();
    } finally {
      requestWriter.abortRequest(); // does nothing if the request was finished
    }
  }

  /**
   * Like KeyValueMessageHandler, when encoding fails.
   */
  private void handleEncodeFailure(KeyValueRequest<Response> request, Throwable err) {
    if (err instanceof CollectionNotFoundException && channelContext.collectionsEnabled()) {
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

  /**
   * Starts the request's dispatch span, and puts it in flight. Call just before writing it.
   *
   * @return false if the request failed instead (because the tracer threw), so it mustn't be written.
   */
  private boolean dispatch(KeyValueRequest<Response> request) {
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
      // A broken tracer. Fails the request, and doesn't write it.
      request.fail(t);
      return false;
    }
    inFlight.put(request.opaque(), new KeyValueInFlight(request, System.nanoTime(), dispatchSpan));
    return true;
  }

  private void updateInterest() {
    SelectionKey k = key;
    if (k == null || !k.isValid()) {
      return;
    }
    int ops = SelectionKey.OP_READ | (output.blocked() || readNeedsWrite ? SelectionKey.OP_WRITE : 0);
    if (k.interestOps() != ops) {
      k.interestOps(ops);
    }
  }

  // ---- reading (on the loop's thread)

  /**
   * Called by the loop when the channel is ready.
   */
  void onSelected(SelectionKey selected) {
    try {
      if (!selected.isValid() || finished) {
        return;
      }
      int ready = selected.readyOps();
      if ((ready & SelectionKey.OP_WRITE) != 0) {
        if (output.drain()) {
          if (readNeedsWrite) {
            readNeedsWrite = false;
            read();
          }
          writeQueued();
        }
        updateInterest();
      }
      if ((ready & SelectionKey.OP_READ) != 0 && !readNeedsWrite) {
        read();
      }
    } catch (Throwable t) {
      fail(t);
    }
  }

  /**
   * Handles the packets already read, and reads more. How much more depends on the channel:
   * <ul>
   *   <li>Plain: like the Netty implementation, until a read doesn't fill the room it had (so it's probably all
   *       the channel had), or after {@value #MAX_READS_PER_PASS} reads (so the loop's other connections get a turn).
   *       Then the loop writes the requests sent meanwhile (for example, by the responses' callbacks) before this
   *       connection reads more. (Reading until the channel is empty instead holds those requests back while
   *       responses keep arriving: with 16 KB JSON gets, a few percent less throughput.)
   *   <li>TLS: until the TLS channel has nothing left to decrypt, {@value #MAX_READS_PER_PASS} records at a time.
   *       A TLS channel returns a record at a time (so a read never fills the room), and holds the records it already
   *       read from the socket, which the selector doesn't know about, so this mustn't stop while it has any.
   *       (Faster overall than applying the plain rule to the TLS channel's socket reads, or reading a record per
   *       pass of the loop.)
   * </ul>
   */
  private void read() throws IOException {
    for (int reads = 0; ; ) {
      if (!handlePackets()) {
        return;
      }
      if (reads == MAX_READS_PER_PASS) {
        if (tls != null) {
          loop.execute(this::readMore); // the TLS channel may hold more, which the selector doesn't know about
        }
        return; // otherwise, the selector selects the channel again if it's still readable
      }

      int read;
      try {
        read = packets.fillAvailable();
      } catch (NeedsWriteException e) {
        readNeedsWrite = true;
        updateInterest();
        return;
      }
      if (read == 0) {
        return; // the selector says when there's more
      }
      reads++;
      idle.read();

      if (tls == null && !packets.lastFillFilledRoom()) {
        handlePackets();
        return;
      }
    }
  }

  /**
   * Carries on reading a TLS channel that has read {@value #MAX_READS_PER_PASS} records (see {@link #read}).
   */
  private void readMore() {
    if (finished || readNeedsWrite) {
      return;
    }
    try {
      read();
    } catch (Throwable t) {
      fail(t);
    }
  }

  /**
   * Handles the complete packets already read.
   *
   * @return false if the connection closed (so it mustn't read more).
   * @throws InvalidPacketException if a packet failed verification (it was reported; the connection closes).
   */
  private boolean handlePackets() throws InvalidPacketException {
    KeyValuePacket packet;
    while ((packet = packets.next()) != null) {
      handle(packet);
      if (closed.get()) {
        // Closed while handling what was read (for example, because a response said to close the connection).
        // Like the Netty implementation, handles the rest of the responses already read (but doesn't read more),
        // so their requests complete, instead of being retried (or canceled, if they're not idempotent).
        while (!discardUnreadResponses && (packet = packets.next()) != null) {
          handle(packet);
        }
        return false;
      }
    }
    return true;
  }

  private void handle(KeyValuePacket packet) {
    try {
      capture.read(packet);
      handleInbound(packet);
    } catch (RuntimeException e) {
      // Like Netty's pipeline, which reports a handler's exception, and carries on.
      eventBus.publish(new GenericFailureDetectedEvent(ioContext, e));
    }
  }

  private void handleInbound(KeyValuePacket packet) {
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

  // ---- the periodic check (on the loop's thread)

  /**
   * Called by the loop every second or so (see {@link KeyValueIdleDetector#checkIntervalNanos}): checks whether
   * the connection has been idle too long.
   */
  void periodicCheck() {
    try {
      idle.check();
    } catch (Throwable t) {
      log.warn("Periodic check of KV connection {} failed", this, t);
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

  // ---- closing

  @Override
  public void close() {
    close(null);
  }

  /**
   * Closes the connection because reading or writing failed, and reports the failure.
   */
  private void fail(Throwable failure) {
    if (close(failure)) {
      reportFailure(failure);
    }
  }

  /**
   * Marks the connection closed, and has the loop finish closing it (see {@link #finishClosing}), after whatever
   * it's doing now (for example, handling the responses already read).
   *
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
    loop.execute(this::finishClosing);
    return true;
  }

  /**
   * Closes the channel, then retries the requests that were queued or in flight, and tells the listener.
   * Runs on the loop's thread, so nothing else reads or writes the requests meanwhile.
   */
  private void finishClosing() {
    if (finished) {
      return;
    }
    finished = true;

    if (key != null) {
      key.cancel();
      loop.deregister(this);
    }
    closeQuietly(tls); // tries to send close_notify (without waiting), then closes the TCP channel
    closeQuietly(tcp);
    output.clear();

    for (Outbound unwritten : queue.drain()) {
      failQueued(unwritten);
    }

    int requestsInFlight = inFlight.size();
    for (KeyValueInFlight written : inFlight.values()) {
      if (written.dispatchSpan != null) {
        written.dispatchSpan.end();
      }
      RetryOrchestrator.maybeRetry(ioContext, written.request, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
    }
    inFlight.clear();

    if (started) {
      listener.closed(this, requestsInFlight);
    }
  }

  /**
   * Reports why reading or writing failed, when that's what closed the connection.
   */
  private void reportFailure(Throwable failure) {
    if (failure instanceof EOFException || failure instanceof InvalidPacketException) {
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
    // Only for the close (the loop might find more reasons, handling the responses it already read).
    if (close(new IOException("Closed proactively: " + reason))) {
      eventBus.publish(new ChannelClosedProactivelyEvent(ioContext, reason));
    }
  }

  @Override
  public String toString() {
    return "NioKeyValueConnection{" +
      "id=0x" + shortChannelId +
      ", local=" + redactSystem(localAddress) +
      ", remote=" + redactSystem(remoteAddress) +
      ", tls=" + (tls != null) +
      '}';
  }

  /**
   * Where the request writer writes: the channel (TLS, if enabled), without waiting. What the channel can't take
   * now is kept (copied), and written when the channel is writable again (see {@link #drain}); meanwhile, the
   * connection writes nothing else (see {@link #blocked}). The copies are zeroed once written, since they might
   * hold credentials (a SASL request, when reauthenticating).
   * <p>
   * Only used by the loop's thread.
   */
  private final class ChannelOutput extends OutputStream {
    private final ArrayDeque<ByteBuffer> pending = new ArrayDeque<>();

    /**
     * Whether the TLS channel has encrypted bytes it couldn't write, or needs to read before it can write more.
     * (Either way, it's waiting to be called again.)
     */
    private boolean tlsBlocked;

    /**
     * Whether something is waiting to be written, so nothing else can be written until it is.
     */
    boolean blocked() {
      return tlsBlocked || !pending.isEmpty();
    }

    @Override
    public void write(int b) throws IOException {
      write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      if (length == 0) {
        return;
      }
      ByteBuffer src = ByteBuffer.wrap(bytes, offset, length);
      if (!blocked()) {
        writeNow(src);
      }
      if (src.hasRemaining()) {
        // A copy: the request writer reuses its buffer (and the caller may reuse a value's array).
        ByteBuffer copy = ByteBuffer.allocate(src.remaining());
        copy.put(src).flip();
        pending.add(copy);
      }
    }

    /**
     * Writes what the channel takes now.
     */
    private void writeNow(ByteBuffer src) throws IOException {
      if (tls == null) {
        while (src.hasRemaining() && tcp.write(src) > 0) {
          // the channel took some, so it might take more
        }
        return;
      }
      try {
        tls.write(src); // all of it, or it throws
      } catch (NeedsWriteException | NeedsReadException e) {
        tlsBlocked = true;
      }
    }

    /**
     * Writes what's waiting, as far as the channel takes it.
     *
     * @return true if nothing is waiting anymore.
     */
    boolean drain() throws IOException {
      if (tlsBlocked) {
        tlsBlocked = false;
        writeNow(ByteBuffer.allocate(0)); // writes what the TLS channel already encrypted
        if (tlsBlocked) {
          return false;
        }
      }
      ByteBuffer next;
      while ((next = pending.peek()) != null) {
        writeNow(next);
        if (next.hasRemaining() || tlsBlocked) {
          return false;
        }
        pending.poll();
        Arrays.fill(next.array(), (byte) 0);
      }
      return true;
    }

    /**
     * Forgets what's waiting, once the connection is closed.
     */
    void clear() {
      for (ByteBuffer it : pending) {
        Arrays.fill(it.array(), (byte) 0);
      }
      pending.clear();
    }
  }
}
