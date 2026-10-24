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

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.CustomTlsCiphersEnabledEvent;
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.KeyValueRequestWriter;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.io.netty.kv.sasl.SaslCredentials;
import com.couchbase.client.core.service.KeyValueConnection.AuthenticationRefresh;
import com.couchbase.client.core.service.KeyValueConnection.TrafficCapture;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
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

import static com.couchbase.client.core.service.KeyValueSocketConnector.closeQuietly;
import static com.couchbase.client.core.service.KeyValueSocketConnector.remainingMillis;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

/**
 * One attempt to open a {@link KeyValueConnection}: connects (TCP, then TLS if enabled), and does the handshake.
 * {@link #open} blocks the caller; {@link #close} (from another thread) stops the attempt.
 */
@NullMarked
final class KeyValueConnectionOpener {
  private static final Logger log = LoggerFactory.getLogger(KeyValueConnectionOpener.class);

  /**
   * The handshake's requests are few and small (a SASL payload, at most, besides HELLO's features).
   */
  private static final int HANDSHAKE_BUFFER_SIZE = 4 * KeyValueRequestEncoder.MAX_REQUEST_HEAD_SIZE;

  private final EndpointContext endpointContext;
  private final String remoteHostname;
  private final int remotePort;
  private final Optional<String> bucketName;
  private final @Nullable SaslCredentials credentials;
  private final Set<ServerFeature> requestedFeatures;
  private final KeyValueSocketConnector connector;
  private final EventBus eventBus;

  /**
   * The connection's ID: short (8 hex digits, like a Netty channel ID's short form), and long
   * (the endpoint's ID and this ID, as sent in the HELLO request, like the Netty implementation's).
   */
  private final String shortChannelId;
  private final String channelId;

  /**
   * How long each connect step took (see {@link #timings()}). Passed on to the connection.
   */
  private final SortedMap<String, Duration> timings = Collections.synchronizedSortedMap(new TreeMap<>());

  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile @Nullable Socket tcpSocket;
  private volatile @Nullable Socket socket;

  /**
   * @param endpointContext the endpoint's context (at the start of this connect attempt).
   * @param credentials null if the connection doesn't authenticate with SASL.
   */
  KeyValueConnectionOpener(
    EndpointContext endpointContext,
    String remoteHostname,
    int remotePort,
    Optional<String> bucketName,
    @Nullable SaslCredentials credentials,
    Set<ServerFeature> requestedFeatures,
    KeyValueSocketConnector connector
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
   * How long each connect step took so far, keyed like the Netty implementation's connect timings.
   */
  SortedMap<String, Duration> timings() {
    synchronized (timings) {
      return new TreeMap<>(timings);
    }
  }

  /**
   * Connects, and does the handshake. Blocks until it's done. Call once.
   *
   * @param deadlineNanos when to give up, in {@link System#nanoTime()} terms.
   * @param listener for the connection.
   * @return the open connection, ready to {@linkplain KeyValueConnection#start start}.
   * @throws TimeoutException if the deadline passes.
   */
  KeyValueConnection open(long deadlineNanos, KeyValueConnection.Listener listener) throws IOException, TimeoutException {
    if (closed.get()) {
      throw new IOException("Connection closed before it opened");
    }
    // Before connecting, so a reauthentication signal during the handshake isn't missed. The connection acts on it
    // once it starts.
    AuthenticationRefresh authenticationRefresh = new AuthenticationRefresh(endpointContext);
    try {
      return doOpen(deadlineNanos, listener, authenticationRefresh);
    } catch (Throwable t) {
      log.debug("Failed to open KV connection 0x{} to {}:{} because: {}", shortChannelId, remoteHostname, remotePort, t.toString());
      authenticationRefresh.unsubscribe();
      close();
      throw t;
    }
  }

  private KeyValueConnection doOpen(
    long deadlineNanos,
    KeyValueConnection.Listener listener,
    AuthenticationRefresh authenticationRefresh
  ) throws IOException, TimeoutException {
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
      throw connectFailure(e);
    }

    if (connector.tlsEnabled()) {
      timings.put("TlsHandshake", Duration.ofNanos(System.nanoTime() - tlsStart[0]));
      // Cause is null if handshake was successful. Set it unconditionally,
      // so the endpoint can recover from a transient on-path attack
      // (where the "server" temporarily presents an untrusted certificate).
      endpointContext.tlsHandshakeFailure(null);
    }

    IoContext ioContext = new IoContext(endpointContext, s.getLocalSocketAddress(), s.getRemoteSocketAddress(), endpointContext.bucket());
    InputStream input = s.getInputStream(); // the packet reader does the buffering
    OutputStream rawOutput = s.getOutputStream();
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
    KeyValuePacketReader packets = new KeyValuePacketReader(input, ioContext);
    TrafficCapture capture = new TrafficCapture(ioContext);

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
              throw withPendingTlsAlert(e, input);
            } finally {
              output.abortRequest(); // does nothing if the request was finished
            }
          }

          @Override
          public void flush() throws IOException {
            try {
              output.flush();
            } catch (IOException e) {
              throw withPendingTlsAlert(e, input);
            }
          }

          @Override
          public KeyValuePacket read() throws IOException {
            s.setSoTimeout(remainingMillis(deadlineNanos));
            KeyValuePacket packet = packets.read();
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

    // From now on, reads wait as long as it takes. The writer closes the connection if the server is quiet too long.
    s.setSoTimeout(0);

    return new KeyValueConnection(
      endpointContext,
      listener,
      requireNonNull(tcpSocket),
      s,
      rawOutput, // the handshake flushed everything it wrote
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
   * Stops the attempt (if it's still going), by closing its sockets. Once {@link #open} has returned the connection,
   * this closes the connection's sockets too, but not the connection; close that instead.
   */
  void close() {
    if (closed.compareAndSet(false, true)) {
      // Close the TCP socket first: closing a TLS socket might wait to send close_notify.
      closeQuietly(tcpSocket);
      closeQuietly(socket);
    }
  }

  /**
   * If writing failed (for example, with "Broken pipe") because the server sent a TLS alert and closed
   * the connection, adds the alert (if it can be read) to the failure as a suppressed exception,
   * like OkHttp does.
   */
  private IOException withPendingTlsAlert(IOException e, InputStream input) {
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
  private IOException connectFailure(IOException e) {
    if (!connector.tlsEnabled()) {
      return e;
    }
    SSLException tlsFailure = OkHttpTlsHelper.describeTlsFailure(e);
    if (tlsFailure == null) {
      return e;
    }

    Socket s = tcpSocket;
    IoContext ioContext = s == null
      ? new IoContext(endpointContext, null, null, endpointContext.bucket())
      : new IoContext(endpointContext, s.getLocalSocketAddress(), s.getRemoteSocketAddress(), endpointContext.bucket());
    endpointContext.tlsHandshakeFailure(e);
    if (OkHttpTlsHelper.connectionFailedDuringTls(e)) {
      return tlsFailure; // might be temporary
    }
    endpointContext.authenticationStatus(AuthenticationStatus.FAILED);
    eventBus.publish(new SecureConnectionFailedEvent(ioContext, tlsFailure));
    // With a hint saying what to check, if retrying won't help.
    return tlsFailure;
  }
}
