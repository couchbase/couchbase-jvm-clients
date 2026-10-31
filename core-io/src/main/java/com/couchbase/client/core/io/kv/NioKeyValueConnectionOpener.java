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

package com.couchbase.client.core.io.kv;

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.CustomTlsCiphersEnabledEvent;
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.kv.NioKeyValueChannelConnector.ConnectedChannel;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.io.netty.kv.sasl.SaslCredentials;
import com.couchbase.client.core.io.repackaged.tlschannel.ClientTlsChannel;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsReadException;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsWriteException;
import com.couchbase.client.core.io.repackaged.tlschannel.WouldBlockException;
import com.couchbase.client.core.io.repackaged.tlschannel.impl.TlsChannelImpl;
import com.couchbase.client.core.service.TlsHelper;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.couchbase.client.core.io.kv.NioKeyValueChannelConnector.closeQuietly;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

/**
 * One attempt to open a {@link NioKeyValueConnection}: connects (TCP, then TLS if enabled) with a non-blocking
 * channel, and does the handshake (see {@link KeyValueHandshake}). The attempt blocks the caller
 * (waiting with a selector of its own, until the connect deadline), then hands the channel to an event loop.
 * {@link #open} blocks the caller; {@link #close} (from another thread) stops the attempt.
 */
@NullMarked
final class NioKeyValueConnectionOpener {
  private static final Logger log = LoggerFactory.getLogger(NioKeyValueConnectionOpener.class);

  /**
   * The handshake's requests are few and small (a SASL payload, at most, besides HELLO's features).
   */
  private static final int HANDSHAKE_BUFFER_SIZE = 4 * KeyValueRequestEncoder.MAX_REQUEST_HEAD_SIZE;

  /**
   * The least room to make for each read from a TLS channel: a whole record, so it's decrypted straight into
   * the read buffer (otherwise, the TLS channel decrypts it into a buffer of its own, and copies it).
   */
  private static final int TLS_MIN_READ = TlsChannelImpl.maxTlsPacketSize;

  private final EndpointContext endpointContext;
  private final String remoteHostname;
  private final int remotePort;
  private final Optional<String> bucketName;
  private final @Nullable SaslCredentials credentials;
  private final Set<ServerFeature> requestedFeatures;
  private final NioKeyValueChannelConnector connector;
  private final EventBus eventBus;

  /**
   * The connection's ID: short (8 hex digits), and long
   * (the endpoint's ID and this ID, as sent in the HELLO request).
   */
  private final String shortChannelId;
  private final String channelId;

  /**
   * How long each connect step took (see {@link #timings()}). Passed on to the connection.
   */
  private final SortedMap<String, Duration> timings = Collections.synchronizedSortedMap(new TreeMap<>());

  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile @Nullable SocketChannel channel;
  private volatile @Nullable KeyValueChannelWaiter waiter;

  /**
   * @param endpointContext the endpoint's context (at the start of this connect attempt).
   * @param credentials null if the connection doesn't authenticate with SASL.
   */
  NioKeyValueConnectionOpener(
    EndpointContext endpointContext,
    String remoteHostname,
    int remotePort,
    Optional<String> bucketName,
    @Nullable SaslCredentials credentials,
    Set<ServerFeature> requestedFeatures,
    NioKeyValueChannelConnector connector
  ) {
    this.endpointContext = requireNonNull(endpointContext);
    this.remoteHostname = requireNonNull(remoteHostname);
    this.remotePort = remotePort;
    this.bucketName = requireNonNull(bucketName);
    this.credentials = credentials;
    this.requestedFeatures = requireNonNull(requestedFeatures);
    this.connector = requireNonNull(connector);
    this.eventBus = endpointContext.environment().eventBus();

    int id = ThreadLocalRandom.current().nextInt();
    this.shortChannelId = String.format("%08x", id);
    this.channelId = String.format("%016X", endpointContext.id()) + "/" + String.format("%016X", Integer.toUnsignedLong(id));
  }

  /**
   * How long each connect step took so far, keyed by step name.
   */
  public SortedMap<String, Duration> timings() {
    synchronized (timings) {
      return new TreeMap<>(timings);
    }
  }

  /**
   * Connects, and does the handshake. Blocks until it's done. Call once.
   *
   * @param deadlineNanos when to give up, in {@link System#nanoTime()} terms.
   * @return the open connection, ready to {@linkplain NioKeyValueConnection#start start}.
   * @throws TimeoutException if the deadline passes.
   */
  public NioKeyValueConnection open(long deadlineNanos, NioKeyValueConnection.Listener listener) throws IOException, TimeoutException {
    if (closed.get()) {
      throw new IOException("Connection closed before it opened");
    }
    // Before connecting, so a reauthentication signal during the handshake isn't missed. The connection acts on it
    // once it starts.
    KeyValueAuthenticationRefresh authenticationRefresh = new KeyValueAuthenticationRefresh(endpointContext);
    try {
      return doOpen(deadlineNanos, listener, authenticationRefresh);
    } catch (Throwable t) {
      log.debug("Failed to open KV connection 0x{} to {}:{} because: {}", shortChannelId, remoteHostname, remotePort, t.toString());
      authenticationRefresh.unsubscribe();
      close();
      // Its selector, if the attempt failed after connecting (once open() returns, the event loop has the channel).
      closeQuietly(waiter);
      throw t;
    }
  }

  private NioKeyValueConnection doOpen(
    long deadlineNanos,
    NioKeyValueConnection.Listener listener,
    KeyValueAuthenticationRefresh authenticationRefresh
  ) throws IOException, TimeoutException {
    List<String> ciphers = endpointContext.environment().securityConfig().ciphers();
    if (connector.tlsEnabled() && !ciphers.isEmpty()) {
      eventBus.publish(new CustomTlsCiphersEnabledEvent(ciphers, endpointContext));
    }

    long tcpStart = System.nanoTime();
    long[] tlsStart = new long[1];
    ConnectedChannel connected;
    try {
      connected = connector.connectChannel(
        remoteHostname,
        remotePort,
        deadlineNanos,
        newChannel -> {
          channel = newChannel;
          if (closed.get()) {
            closeQuietly(newChannel); // closed while connecting
          }
        },
        newWaiter -> waiter = newWaiter,
        () -> {
          timings.put("TcpConnect", Duration.ofNanos(System.nanoTime() - tcpStart));
          tlsStart[0] = System.nanoTime();
        }
      );
    } catch (IOException e) {
      throw connectFailure(e);
    }

    if (connected.tls != null) {
      timings.put("TlsHandshake", Duration.ofNanos(System.nanoTime() - tlsStart[0]));
      // Cause is null if handshake was successful. Set it unconditionally,
      // so the endpoint can recover from a transient on-path attack
      // (where the "server" temporarily presents an untrusted certificate).
      endpointContext.tlsHandshakeFailure(null);
    }

    SocketChannel tcp = connected.tcp;
    ClientTlsChannel tls = connected.tls;
    ByteChannel io = connected.io();
    KeyValueChannelWaiter channelWaiter = connected.waiter;

    Socket s = tcp.socket();
    IoContext ioContext = new IoContext(endpointContext, s.getLocalSocketAddress(), s.getRemoteSocketAddress(), endpointContext.bucket());
    HandshakeOutput rawOutput = new HandshakeOutput(io, channelWaiter, deadlineNanos);
    // Only for the handshake (the connection has a writer of its own), so it's small, and nothing negotiated yet.
    KeyValueRequestWriter output = new KeyValueRequestWriter(
      rawOutput,
      HANDSHAKE_BUFFER_SIZE,
      new KeyValueChannelContext(
        null,
        bucketName,
        endpointContext.core().configurationProvider().collectionMap(),
        shortChannelId,
        emptySet()
      )
    );
    KeyValuePacketReader packets = new KeyValuePacketReader(io, tls != null ? TLS_MIN_READ : KeyValueFrameDecoder.DEFAULT_MIN_READ, ioContext);
    KeyValueTrafficCapture capture = new KeyValueTrafficCapture(ioContext);

    KeyValueHandshake.Result result;
    try {
      result = new KeyValueHandshake(
        endpointContext,
        ioContext,
        new KeyValueHandshake.PacketIo() {
          @Override
          public void write(Opcode opcode, int opaque, Consumer<KeyValueRequestBuilder> build) throws IOException {
            try {
              output.makeRoomForRequest();
              build.accept(output.beginRequest(opcode));
              output.completeRequestHead(opaque);
              if (capture.enabled()) {
                capture.written(output.requestBytes());
              }
              output.finishRequest();
            } catch (IOException e) {
              throw withPendingTlsAlert(e, tls, channelWaiter, deadlineNanos);
            } finally {
              output.abortRequest(); // does nothing if the request was finished
            }
          }

          @Override
          public void flush() throws IOException {
            try {
              output.flush();
            } catch (IOException e) {
              throw withPendingTlsAlert(e, tls, channelWaiter, deadlineNanos);
            }
          }

          @Override
          public KeyValuePacket read() throws IOException {
            KeyValuePacket packet;
            while ((packet = packets.next()) == null) {
              try {
                if (packets.fillAvailable() == 0) {
                  channelWaiter.await(SelectionKey.OP_READ, deadlineNanos);
                }
              } catch (NeedsWriteException e) {
                channelWaiter.await(SelectionKey.OP_WRITE, deadlineNanos);
              }
            }
            capture.read(packet);
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
      throw connectFailure(e);
    }

    KeyValueChannelContext channelContext = new KeyValueChannelContext(
      result.features.contains(ServerFeature.SNAPPY) ? endpointContext.environment().compressionConfig() : null,
      bucketName,
      endpointContext.core().configurationProvider().collectionMap(),
      shortChannelId,
      result.features
    );

    // The event loop takes over the channel.
    channelWaiter.close();
    if (closed.get()) {
      throw new IOException("Connection closed before it opened");
    }

    return new NioKeyValueConnection(
      endpointContext,
      listener,
      endpointContext.environment().ioEnvironment().nioKeyValueEventLoopGroup().next(),
      tcp,
      tls,
      packets,
      ioContext,
      channelContext,
      result.errorMap,
      shortChannelId,
      channelId,
      timings,
      authenticationRefresh,
      capture
    );
  }

  /**
   * Returns the waiter for the attempt's channel, or null if it hasn't been created yet. For tests.
   */
  @Nullable KeyValueChannelWaiter waiter() {
    return waiter;
  }

  /**
   * Stops the attempt (if it's still going), by closing its channel. Once {@link #open} has returned the connection,
   * this closes the connection's channel too, but not the connection; close that instead.
   */
  public void close() {
    if (closed.compareAndSet(false, true)) {
      closeQuietly(channel);
      KeyValueChannelWaiter w = waiter;
      if (w != null) {
        w.wakeup(); // so it sees the channel is closed
      }
    }
  }

  /**
   * Writes everything it's given before returning, waiting until the channel is writable (until the deadline).
   * Unbuffered: the handshake's request writer does the buffering.
   */
  private static final class HandshakeOutput extends OutputStream {
    private final ByteChannel io;
    private final KeyValueChannelWaiter waiter;
    private final long deadlineNanos;

    HandshakeOutput(ByteChannel io, KeyValueChannelWaiter waiter, long deadlineNanos) {
      this.io = requireNonNull(io);
      this.waiter = requireNonNull(waiter);
      this.deadlineNanos = deadlineNanos;
    }

    @Override
    public void write(int b) throws IOException {
      write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      ByteBuffer src = ByteBuffer.wrap(bytes, offset, length);
      // With TLS, also until the TLS channel has written what it encrypted (it may hold some after writing src).
      boolean flushed = false;
      while (src.hasRemaining() || !flushed) {
        try {
          int written = io.write(src);
          flushed = true;
          if (written == 0 && src.hasRemaining()) {
            waiter.await(SelectionKey.OP_WRITE, deadlineNanos);
          }
        } catch (NeedsWriteException e) {
          flushed = false;
          waiter.await(SelectionKey.OP_WRITE, deadlineNanos);
        } catch (NeedsReadException e) {
          flushed = false;
          waiter.await(SelectionKey.OP_READ, deadlineNanos);
        }
      }
    }
  }

  /**
   * If writing failed (for example, with "Broken pipe") because the server sent a TLS alert and closed
   * the connection, adds the alert (if it can be read before the deadline) to the failure as a suppressed exception,
   */
  private IOException withPendingTlsAlert(
    IOException e,
    @Nullable ClientTlsChannel tls,
    KeyValueChannelWaiter waiter,
    long deadlineNanos
  ) {
    if (tls == null || e instanceof SocketTimeoutException || TlsHelper.tlsFailure(e) != null) {
      return e;
    }
    ByteBuffer ignored = ByteBuffer.allocate(TLS_MIN_READ);
    while (true) {
      try {
        if (tls.read(ignored) != 0) {
          return e; // data (or the end of the stream), not an alert
        }
      } catch (NeedsReadException needsRead) {
        try {
          waiter.await(SelectionKey.OP_READ, deadlineNanos);
        } catch (IOException timedOutOrClosed) {
          return e;
        }
      } catch (WouldBlockException wouldBlock) {
        return e;
      } catch (IOException readFailure) {
        e.addSuppressed(readFailure);
        return e;
      }
    }
  }

  /**
   * Returns the exception to fail the connect attempt with, and if it's a TLS failure, records it
   * (on the endpoint context, and with an event if retrying won't help).
   */
  private IOException connectFailure(IOException e) {
    if (!connector.tlsEnabled()) {
      return e;
    }
    SSLException tlsFailure = TlsHelper.tlsFailure(e);
    if (tlsFailure == null) {
      return e;
    }

    SocketChannel c = channel;
    IoContext ioContext = c == null
      ? new IoContext(endpointContext, null, null, endpointContext.bucket())
      : new IoContext(endpointContext, c.socket().getLocalSocketAddress(), c.socket().getRemoteSocketAddress(), endpointContext.bucket());
    endpointContext.tlsHandshakeFailure(e);
    if (TlsHelper.tlsFailureMayBeTemporary(e)) {
      return tlsFailure; // might be temporary
    }
    endpointContext.authenticationStatus(AuthenticationStatus.FAILED);
    eventBus.publish(new SecureConnectionFailedEvent(ioContext, tlsFailure));
    // With a hint saying what to check, if retrying won't help.
    return tlsFailure;
  }
}
