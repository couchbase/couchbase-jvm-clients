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
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.SecurityConfig;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Opens the sockets for KV connections that don't use Netty: TCP, with the same socket options as the
 * OkHttp-based services, and TLS (if enabled) with the same certificates and TLS settings.
 * <p>
 * One per {@link com.couchbase.client.core.Core}. Thread-safe.
 */
@NullMarked
@Stability.Internal
public final class KeyValueSocketConnector {
  private final SocketFactory socketFactory;
  private final SecurityConfig securityConfig;
  private final @Nullable SSLSocketFactory tlsSocketFactory;

  /**
   * @param socketFactory creates unconnected TCP sockets, and sets their options once they connect
   * (see {@link CouchbaseSocketFactory}).
   */
  KeyValueSocketConnector(
    SocketFactory socketFactory,
    SecurityConfig securityConfig,
    Authenticator authenticator,
    Duration handshakeTimeout
  ) {
    this.socketFactory = requireNonNull(socketFactory);
    this.securityConfig = requireNonNull(securityConfig);
    this.tlsSocketFactory = securityConfig.tlsEnabled()
      ? OkHttpTlsHelper.newTlsSocketFactory(authenticator::getKeyManagerFactory, securityConfig, handshakeTimeout)
      : null;
  }

  /**
   * Creates a connector whose sockets have the same options and TLS settings as the OkHttp client's.
   */
  public static KeyValueSocketConnector create(
    CouchbaseOkHttpClient okHttpClient,
    SecurityConfig securityConfig,
    Authenticator authenticator,
    Duration handshakeTimeout
  ) {
    return new KeyValueSocketConnector(okHttpClient.socketFactory(), securityConfig, authenticator, handshakeTimeout);
  }

  public boolean tlsEnabled() {
    return tlsSocketFactory != null;
  }

  /**
   * Connects to the host, and does the TLS handshake if TLS is enabled. Blocks until it's done,
   * or the deadline passes.
   *
   * @param deadlineNanos when to give up, in {@link System#nanoTime()} terms.
   * @param onSocket called with each socket as soon as it's created (first the TCP socket, then the TLS socket),
   * so the caller can close it to stop the attempt.
   * @param onTcpConnected called once the TCP connection is open, before the TLS handshake (if any).
   * @return the connected socket. Its read timeout is whatever time is left before the deadline.
   * @throws SocketTimeoutException if the deadline passes.
   * @throws IOException if connecting fails. If the TLS handshake fails, it's (or it's caused by)
   * an {@link javax.net.ssl.SSLException}.
   */
  public Socket connect(
    String host,
    int port,
    long deadlineNanos,
    Consumer<Socket> onSocket,
    Runnable onTcpConnected
  ) throws IOException {
    Socket socket = socketFactory.createSocket();
    try {
      onSocket.accept(socket);
      // Resolves the host name (blocking, with the OS's own timeout), like Netty's default resolver.
      InetSocketAddress address = new InetSocketAddress(host, port);
      socket.connect(address, remainingMillis(deadlineNanos));
      onTcpConnected.run();

      if (tlsSocketFactory == null) {
        socket.setSoTimeout(remainingMillis(deadlineNanos));
        return socket;
      }

      // Uses the host name we were given (not the resolved address) for SNI and host name verification.
      SSLSocket tlsSocket = (SSLSocket) tlsSocketFactory.createSocket(socket, host, port, true);
      onSocket.accept(tlsSocket);
      socket = tlsSocket;
      OkHttpTlsHelper.configureTlsSocket(tlsSocket, securityConfig);
      tlsSocket.setSoTimeout(remainingMillis(deadlineNanos));
      tlsSocket.startHandshake();
      return tlsSocket;

    } catch (IOException | RuntimeException e) {
      closeQuietly(socket);
      throw e;
    }
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

  static void closeQuietly(@Nullable Socket socket) {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException ignore) {
        // nothing to do
      }
    }
  }
}
