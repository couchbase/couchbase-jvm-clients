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

import com.couchbase.client.core.io.netty.SslSessionLoggingHandler;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Logs the server's certificate chain when the trust manager it wraps checks it: at debug level if it's trusted
 * (with the cipher suite, like the Netty implementation's TLS session logging, and under the same logger:
 * {@link SslSessionLoggingHandler}), and at warning level if it isn't, so a failed TLS handshake can be diagnosed.
 * (A failed handshake's chain isn't available any other way: there's no completed session to get it from.)
 * Otherwise delegates everything to the wrapped trust manager.
 * <p>
 * Each untrusted chain is logged at warning level only the first time it's seen (per set of reported chains,
 * which the caller keeps for the life of its client). Later failures with the same chain are logged at debug
 * level, without the chain, since retries would otherwise repeat it for every connection attempt.
 * <p>
 * Thread-safe, if the wrapped trust manager is.
 */
@NullMarked
final class CertificateLoggingTrustManager extends X509ExtendedTrustManager {
  private static final Logger log = LoggerFactory.getLogger(CertificateLoggingTrustManager.class);

  // Trusted chains are logged under the Netty implementation's TLS session logger, so one logger controls both.
  private static final Logger sessionLog = LoggerFactory.getLogger(SslSessionLoggingHandler.class);

  /**
   * Bounds the set of reported chains. If it fills up, it's cleared, so a chain may be logged at warning level again.
   */
  static final int MAX_REPORTED_CHAINS = 100;

  private final X509ExtendedTrustManager delegate;
  private final Set<List<X509Certificate>> reportedChains;

  private CertificateLoggingTrustManager(X509ExtendedTrustManager delegate, Set<List<X509Certificate>> reportedChains) {
    this.delegate = requireNonNull(delegate);
    this.reportedChains = requireNonNull(reportedChains);
  }

  /**
   * Returns a new set for {@link #wrap}'s reported chains (thread-safe).
   */
  static Set<List<X509Certificate>> newReportedChains() {
    return ConcurrentHashMap.newKeySet();
  }

  /**
   * Returns the trust manager, wrapped. An {@link X509ExtendedTrustManager} (as the JDK's are) gets a wrapper that's
   * one too. A plain {@link X509TrustManager} gets a plain wrapper: the JDK adds checks of its own around a plain
   * trust manager (for example, algorithm constraints), which an extended wrapper would bypass.
   */
  static X509TrustManager wrap(X509TrustManager trustManager, Set<List<X509Certificate>> reportedChains) {
    return trustManager instanceof X509ExtendedTrustManager
      ? new CertificateLoggingTrustManager((X509ExtendedTrustManager) trustManager, reportedChains)
      : new Plain(trustManager, reportedChains);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, @Nullable Socket socket) throws CertificateException {
    SSLSession session = socket instanceof SSLSocket ? ((SSLSocket) socket).getHandshakeSession() : null;
    try {
      delegate.checkServerTrusted(chain, authType, socket);
    } catch (CertificateException e) {
      logUntrusted(reportedChains, chain, e, session != null ? remote(session) : socket == null ? null : socket.getRemoteSocketAddress());
      throw e;
    }
    logTrusted(chain, session);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, @Nullable SSLEngine engine) throws CertificateException {
    SSLSession session = engine == null ? null : engine.getHandshakeSession();
    try {
      delegate.checkServerTrusted(chain, authType, engine);
    } catch (CertificateException e) {
      logUntrusted(reportedChains, chain, e, session != null ? remote(session) : engine == null ? null : engine.getPeerHost() + ":" + engine.getPeerPort());
      throw e;
    }
    logTrusted(chain, session);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    checkServerTrusted(delegate, reportedChains, chain, authType);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, @Nullable Socket socket) throws CertificateException {
    delegate.checkClientTrusted(chain, authType, socket);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, @Nullable SSLEngine engine) throws CertificateException {
    delegate.checkClientTrusted(chain, authType, engine);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    delegate.checkClientTrusted(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return delegate.getAcceptedIssuers();
  }

  @Override
  public String toString() {
    return "CertificateLoggingTrustManager{" + delegate + "}";
  }

  /**
   * The wrapper for a plain {@link X509TrustManager}. Plain too, so the JDK still adds its own checks around it.
   */
  static final class Plain implements X509TrustManager {
    private final X509TrustManager delegate;
    private final Set<List<X509Certificate>> reportedChains;

    private Plain(X509TrustManager delegate, Set<List<X509Certificate>> reportedChains) {
      this.delegate = requireNonNull(delegate);
      this.reportedChains = requireNonNull(reportedChains);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      CertificateLoggingTrustManager.checkServerTrusted(delegate, reportedChains, chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return delegate.getAcceptedIssuers();
    }

    @Override
    public String toString() {
      return "CertificateLoggingTrustManager.Plain{" + delegate + "}";
    }
  }

  /**
   * Checks with the delegate's method that has no socket or engine (so no handshake session to report).
   */
  private static void checkServerTrusted(
    X509TrustManager delegate,
    Set<List<X509Certificate>> reportedChains,
    X509Certificate[] chain,
    String authType
  ) throws CertificateException {
    try {
      delegate.checkServerTrusted(chain, authType);
    } catch (CertificateException e) {
      logUntrusted(reportedChains, chain, e, null);
      throw e;
    }
    logTrusted(chain, null);
  }

  private static void logTrusted(X509Certificate[] chain, @Nullable SSLSession session) {
    if (sessionLog.isDebugEnabled()) {
      sessionLog.debug(
        "TLS server certificate chain trusted. remote = {} ; cipher suite = {} ; certificate chain = \n{}",
        session == null ? null : remote(session),
        session == null ? null : session.getCipherSuite(),
        SslSessionLoggingHandler.pemChain(chain)
      );
    }
  }

  private static void logUntrusted(
    Set<List<X509Certificate>> reportedChains,
    X509Certificate[] chain,
    CertificateException e,
    @Nullable Object remote
  ) {
    if (firstReport(reportedChains, chain)) {
      log.warn(
        "TLS handshake failed: the server's certificate chain isn't trusted. remote = {} ; reason = {} ;" +
          " certificate chain (further failures with this chain are logged at debug level) = \n{}",
        remote,
        e.toString(),
        SslSessionLoggingHandler.pemChain(chain)
      );
    } else {
      log.debug(
        "TLS handshake failed: the server's certificate chain isn't trusted (chain logged earlier). remote = {} ; reason = {}",
        remote,
        e.toString()
      );
    }
  }

  private static HostAndPort remote(SSLSession session) {
    return new HostAndPort(session.getPeerHost(), session.getPeerPort());
  }

  /**
   * Returns true the first time the chain is seen (certificates are equal if their encoded forms are),
   * and adds it to the reported chains.
   */
  static boolean firstReport(Set<List<X509Certificate>> reportedChains, X509Certificate[] chain) {
    if (reportedChains.size() >= MAX_REPORTED_CHAINS) {
      reportedChains.clear();
    }
    return reportedChains.add(Arrays.asList(chain.clone()));
  }
}
