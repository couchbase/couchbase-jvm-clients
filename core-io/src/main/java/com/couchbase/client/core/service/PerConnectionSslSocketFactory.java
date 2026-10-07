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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * An SSL socket factory that uses a new {@link SSLContext} for every connection, like the Netty
 * implementation (which built a new {@code SslContext} for every connection).
 * <p>
 * Each context is created with the client certificate and trust manager the suppliers provide at that
 * moment, so every new connection presents the current client certificate, and trusts the currently
 * trusted server certificates. And since TLS sessions are cached per context, a new connection
 * never resumes an earlier session (which would skip presenting a certificate, and keep the identity
 * it was established with). The cost is a full handshake for every new connection.
 * <p>
 * OkHttp is configured with this one factory instance for the life of the client, so connection
 * pooling (which compares socket factories) isn't affected.
 * <p>
 * Only layers TLS over an existing socket, which is how OkHttp uses it: OkHttp creates the TCP socket with
 * its own socket factory (see {@link CouchbaseSocketFactory}), connects it, then calls
 * {@link #createSocket(Socket, String, int, boolean)}. The other ways of creating a socket throw,
 * because their sockets would miss the TCP keepalive settings and the handshake timeout.
 * <p>
 * Thread-safe.
 */
@NullMarked
final class PerConnectionSslSocketFactory extends SSLSocketFactory {
  private final Supplier<@Nullable KeyManagerFactory> clientCertificates;
  private final Supplier<X509TrustManager> trustManagers;
  private final int handshakeTimeoutMillis;

  /**
   * The untrusted server certificate chains already logged at warning level (see CertificateLoggingTrustManager).
   */
  private final Set<List<X509Certificate>> reportedUntrustedChains = CertificateLoggingTrustManager.newReportedChains();

  /**
   * Answers questions about cipher suites, which don't depend on the certificates.
   * Created without a client certificate, so creating this factory never asks for one.
   */
  private final SSLSocketFactory cipherSuiteInfo;

  /**
   * @param clientCertificates supplies the client certificate (if any), asked once per new connection.
   * Typically the authenticator's {@code getKeyManagerFactory}.
   * @param trustManagers supplies the trust manager, asked once per new connection.
   * @param handshakeTimeout read timeout for the TLS handshake, set on each socket that OkHttp layers TLS over.
   * <p>
   * If either supplier throws, the connection fails with an {@link SSLException} that has the exception as its cause.
   */
  PerConnectionSslSocketFactory(
    Supplier<@Nullable KeyManagerFactory> clientCertificates,
    Supplier<X509TrustManager> trustManagers,
    Duration handshakeTimeout
  ) {
    this.clientCertificates = requireNonNull(clientCertificates);
    this.trustManagers = requireNonNull(trustManagers);
    this.handshakeTimeoutMillis = (int) Math.max(1, Math.min(Integer.MAX_VALUE, handshakeTimeout.toMillis()));
    this.cipherSuiteInfo = newSslContext(null, trustManagers.get()).getSocketFactory();
  }

  private SSLSocketFactory newFactory() throws SSLException {
    KeyManager[] keyManagers;
    X509TrustManager trustManager;
    try {
      KeyManagerFactory factory = clientCertificates.get();
      // Like the Netty implementation, null if there's no client certificate.
      // (With the JDK's TLS provider, that means no client certificate is presented.)
      keyManagers = factory == null ? null : factory.getKeyManagers();
      // Logs the server's certificate chain: at debug level if it's trusted, like the Netty implementation's
      // TLS session logging, and at warning level if it isn't, so a failed handshake can be diagnosed.
      trustManager = CertificateLoggingTrustManager.wrap(trustManagers.get(), reportedUntrustedChains);
    } catch (RuntimeException e) {
      // For example, the authenticator couldn't provide the client certificate.
      throw new SSLException("Failed to get the certificates for a new connection.", e);
    }
    return newSslContext(keyManagers, trustManager).getSocketFactory();
  }

  private static SSLContext newSslContext(KeyManager @Nullable [] keyManagers, X509TrustManager trustManager) {
    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyManagers, new TrustManager[]{trustManager}, null);
      return sslContext;
    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return cipherSuiteInfo.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return cipherSuiteInfo.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
    // This is the one OkHttp uses: it layers TLS over a connected TCP socket, then does the handshake.
    // The client has no OkHttp read timeout (see CouchbaseOkHttpClient), so without this, a server that
    // accepts the connection but never completes the handshake would only be stopped by the SDK's request
    // timeout cancelling the call. OkHttp replaces this read timeout before sending each request.
    socket.setSoTimeout(handshakeTimeoutMillis);
    return newFactory().createSocket(socket, host, port, autoClose);
  }

  // createSocket() (an unconnected socket) isn't overridden, so it throws, like the JDK's default.

  @Override
  public Socket createSocket(String host, int port) {
    throw onlyLayeredSockets();
  }

  @Override
  public Socket createSocket(String host, int port, @Nullable InetAddress localHost, int localPort) {
    throw onlyLayeredSockets();
  }

  @Override
  public Socket createSocket(InetAddress host, int port) {
    throw onlyLayeredSockets();
  }

  @Override
  public Socket createSocket(InetAddress address, int port, @Nullable InetAddress localAddress, int localPort) {
    throw onlyLayeredSockets();
  }

  private static UnsupportedOperationException onlyLayeredSockets() {
    return new UnsupportedOperationException(
      "Only supports layering TLS over an existing socket, with createSocket(Socket, String, int, boolean)."
    );
  }

  @Override
  public String toString() {
    return "PerConnectionSslSocketFactory";
  }
}
