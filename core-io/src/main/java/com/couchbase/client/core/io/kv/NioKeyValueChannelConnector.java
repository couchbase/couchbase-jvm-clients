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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.io.repackaged.tlschannel.ClientTlsChannel;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsReadException;
import com.couchbase.client.core.io.repackaged.tlschannel.NeedsWriteException;
import com.couchbase.client.core.service.CouchbaseOkHttpClient;
import com.couchbase.client.core.service.CouchbaseSocketFactory;
import com.couchbase.client.core.service.PerConnectionSslSocketFactory;
import com.couchbase.client.core.service.TlsHelper;
import com.couchbase.client.core.util.CbThreads;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import javax.net.SocketFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Opens the socket channels for KV connections (see {@link NioKeyValueConnection}): TCP, with
 * the same socket options as the OkHttp-based services, and TLS (if enabled) with the same certificates and TLS
 * settings.
 * <p>
 * One per {@link com.couchbase.client.core.Core}. Thread-safe.
 */
@NullMarked
@Stability.Internal
public final class NioKeyValueChannelConnector {
  private final SocketFactory socketFactory;
  private final SecurityConfig securityConfig;
  private final @Nullable PerConnectionSslSocketFactory tlsEngineFactory;
  private final HostResolver hostResolver;

  /**
   * Lookups in progress, by host name. Connections to the same host share a lookup, so a lookup that takes longer
   * than the connect timeout ties up one thread, not one per connect attempt.
   */
  private final ConcurrentMap<String, CompletableFuture<InetAddress>> lookupsInProgress = new ConcurrentHashMap<>();

  /**
   * Host name lookups. Lookups to the same host are shared (see lookup()), so there's about one thread per host
   * being looked up; platform threads are fine. Idle threads end after a minute, so this needn't be shut down.
   */
  private static final ExecutorService hostNameLookups = Executors.newCachedThreadPool(CbThreads.platformThreadFactory("cb-kv-dns-"));

  /**
   * Resolves a host name. Blocks until it's done, or the OS gives up.
   */
  @FunctionalInterface
  interface HostResolver {
    InetAddress resolve(String host) throws UnknownHostException;
  }

  /**
   * @param socketFactory creates unconnected TCP sockets, and sets their options once they connect
   * (see {@link CouchbaseSocketFactory}).
   */
  NioKeyValueChannelConnector(
    SocketFactory socketFactory,
    SecurityConfig securityConfig,
    Authenticator authenticator,
    Duration handshakeTimeout
  ) {
    this(socketFactory, securityConfig, authenticator, handshakeTimeout, InetAddress::getByName);
  }

  NioKeyValueChannelConnector(
    SocketFactory socketFactory,
    SecurityConfig securityConfig,
    Authenticator authenticator,
    Duration handshakeTimeout,
    HostResolver hostResolver
  ) {
    this.socketFactory = requireNonNull(socketFactory);
    this.hostResolver = requireNonNull(hostResolver);
    this.securityConfig = requireNonNull(securityConfig);
    this.tlsEngineFactory = securityConfig.tlsEnabled()
      ? TlsHelper.newEngineFactory(authenticator::getKeyManagerFactory, securityConfig, handshakeTimeout)
      : null;
  }

  /**
   * Creates a connector whose sockets have the same options and TLS settings as the OkHttp client's.
   */
  public static NioKeyValueChannelConnector create(
    CouchbaseOkHttpClient okHttpClient,
    SecurityConfig securityConfig,
    Authenticator authenticator,
    Duration handshakeTimeout
  ) {
    return new NioKeyValueChannelConnector(okHttpClient.socketFactory(), securityConfig, authenticator, handshakeTimeout);
  }

  public boolean tlsEnabled() {
    return tlsEngineFactory != null;
  }

  /**
   * A connected channel for a KV connection that uses NIO (see {@link #connectChannel}).
   */
  static final class ConnectedChannel {
    final SocketChannel tcp;
    final @Nullable ClientTlsChannel tls;

    /**
     * Waits for the channel until the deadline, until the caller hands the channel over to an event loop.
     */
    final KeyValueChannelWaiter waiter;

    ConnectedChannel(SocketChannel tcp, @Nullable ClientTlsChannel tls, KeyValueChannelWaiter waiter) {
      this.tcp = requireNonNull(tcp);
      this.tls = tls;
      this.waiter = requireNonNull(waiter);
    }

    /**
     * The channel to read and write: the TLS channel, if TLS is enabled.
     */
    ByteChannel io() {
      return tls != null ? tls : tcp;
    }
  }

  /**
   * Connects a non-blocking channel to the host, and does the TLS handshake (with an {@link SSLEngine}) if TLS
   * is enabled. Blocks until it's done, or the deadline passes.
   *
   * @param onChannel called with the channel as soon as it's created, so the caller can close it to stop the attempt.
   * @param onWaiter called with the waiter as soon as it's created, so the caller can wake it after closing the channel.
   * @throws UnknownHostException if the host name can't be resolved, or the deadline passes first.
   * @throws ConnectException if the deadline passes before the TCP connection is open.
   * @throws SocketTimeoutException if the deadline passes during the TLS handshake.
   * @throws IOException if connecting fails. If the TLS handshake fails, it's (or it's caused by)
   * an {@link javax.net.ssl.SSLException}.
   */
  ConnectedChannel connectChannel(
    String host,
    int port,
    long deadlineNanos,
    Consumer<SocketChannel> onChannel,
    Consumer<KeyValueChannelWaiter> onWaiter,
    Runnable onTcpConnected
  ) throws IOException {
    CouchbaseSocketFactory couchbaseSocketFactory = socketFactory instanceof CouchbaseSocketFactory ? (CouchbaseSocketFactory) socketFactory : null;
    CouchbaseSocketFactory.OpenedChannel opened = couchbaseSocketFactory != null ? couchbaseSocketFactory.openChannel() : null;
    SocketChannel channel = opened != null ? opened.channel() : openPlainChannel();
    KeyValueChannelWaiter waiter = null;
    ClientTlsChannel tls = null;
    try {
      onChannel.accept(channel);
      waiter = new KeyValueChannelWaiter(channel);
      onWaiter.accept(waiter);
      InetSocketAddress address = resolve(host, port, deadlineNanos);
      connectTcp(channel, waiter, address, deadlineNanos);
      if (couchbaseSocketFactory != null && opened != null) {
        couchbaseSocketFactory.configure(opened);
      }
      onTcpConnected.run();

      if (tlsEngineFactory == null) {
        return new ConnectedChannel(channel, null, waiter);
      }

      // Uses the host name we were given (not the resolved address) for SNI and host name verification.
      SSLEngine engine = tlsEngineFactory.newEngine(host, port);
      TlsHelper.configureTlsEngine(engine, securityConfig);
      tls = ClientTlsChannel.newBuilder(channel, engine)
        .withReleaseBuffers(false) // kept for the connection's life, instead of allocated for each read and write
        .build();
      while (true) {
        try {
          tls.handshake();
          return new ConnectedChannel(channel, tls, waiter);
        } catch (NeedsReadException e) {
          waiter.await(SelectionKey.OP_READ, deadlineNanos);
        } catch (NeedsWriteException e) {
          waiter.await(SelectionKey.OP_WRITE, deadlineNanos);
        } catch (ClosedChannelException e) {
          if (!channel.isOpen()) {
            throw e; // closed to stop the attempt
          }
          // The TLS channel's way of saying the server closed the connection. Like the JDK's TLS socket.
          SSLHandshakeException failure = new SSLHandshakeException("Remote host terminated the handshake");
          failure.initCause(new EOFException("SSL peer shut down incorrectly"));
          throw failure;
        }
      }

    } catch (Throwable t) { // including an Error (for example, from the TLS channel), so the waiter's selector is closed
      closeQuietly(tls != null ? tls : channel);
      closeQuietly(waiter);
      throw t;
    }
  }

  private static SocketChannel openPlainChannel() throws IOException {
    SocketChannel channel = SocketChannel.open();
    try {
      channel.configureBlocking(false);
      return channel;
    } catch (IOException | RuntimeException e) {
      channel.close();
      throw e;
    }
  }

  /**
   * Connects the non-blocking channel, waiting with the waiter until it's connected or the deadline passes.
   *
   * @throws ConnectException if the deadline passes, so it gets the same hint as a refused connection
   * (see {@link NioKeyValueEndpoint#annotateConnectException}).
   */
  private static void connectTcp(SocketChannel channel, KeyValueChannelWaiter waiter, InetSocketAddress address, long deadlineNanos) throws IOException {
    long timeoutMillis = 0;
    try {
      timeoutMillis = remainingMillis(deadlineNanos);
      if (channel.connect(address)) {
        return;
      }
      while (true) {
        waiter.await(SelectionKey.OP_CONNECT, deadlineNanos);
        if (channel.finishConnect()) {
          return;
        }
      }
    } catch (SocketTimeoutException e) {
      ConnectException timedOut = new ConnectException("connection timed out after " + timeoutMillis + " ms: " + address);
      timedOut.initCause(e);
      throw timedOut;
    }
  }

  /**
   * Resolves the host name, giving up when the deadline passes, so the connect timeout includes resolving
   * the host name. (A lookup can't be interrupted, so one that's still going carries on
   * in the background, until the OS gives up. Meanwhile, connect attempts to the same host wait for it.)
   *
   * @throws UnknownHostException if the host name can't be resolved, or the deadline passes first.
   */
  private InetSocketAddress resolve(String host, int port, long deadlineNanos) throws IOException {
    int timeoutMillis = remainingMillis(deadlineNanos);
    CompletableFuture<InetAddress> lookup = lookup(host);
    try {
      return new InetSocketAddress(lookup.get(timeoutMillis, TimeUnit.MILLISECONDS), port);
    } catch (TimeoutException e) {
      // Like the JDK's message when the OS gives up.
      throw new UnknownHostException(host + ": timed out after " + timeoutMillis + " ms");
    } catch (ExecutionException e) {
      // A new exception for each connect attempt, since attempts can share a lookup (and its failure).
      Throwable cause = e.getCause();
      UnknownHostException failure = new UnknownHostException(cause.getMessage());
      failure.initCause(cause);
      throw failure;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while resolving host name: " + host);
    }
  }

  /**
   * Returns the lookup in progress for the host name, or starts one.
   */
  private CompletableFuture<InetAddress> lookup(String host) {
    CompletableFuture<InetAddress> newLookup = new CompletableFuture<>();
    CompletableFuture<InetAddress> inProgress = lookupsInProgress.putIfAbsent(host, newLookup);
    if (inProgress != null) {
      return inProgress;
    }

    // Started once it's in the map, so it can remove itself when it's done (and the next connect looks up again).
    try {
      hostNameLookups.execute(() -> {
        try {
          newLookup.complete(hostResolver.resolve(host));
        } catch (Throwable t) {
          newLookup.completeExceptionally(t);
        } finally {
          lookupsInProgress.remove(host, newLookup);
        }
      });
    } catch (RuntimeException e) {
      lookupsInProgress.remove(host, newLookup);
      newLookup.completeExceptionally(e);
    }
    return newLookup;
  }

  /**
   * Returns the milliseconds left before the deadline, at least 1 (since 0 means "no timeout").
   *
   * @throws SocketTimeoutException if the deadline has passed.
   */
  static int remainingMillis(long deadlineNanos) throws SocketTimeoutException {
    long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new SocketTimeoutException("Timed out");
    }
    return (int) Math.max(1, Math.min(Integer.MAX_VALUE, Duration.ofNanos(remainingNanos).toMillis()));
  }

  static void closeQuietly(@Nullable Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException ignore) {
        // nothing to do
      }
    }
  }
}
