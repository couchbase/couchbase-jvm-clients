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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;
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
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.util.CbThrowables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

/**
 * Logs the server's certificate chain when the trust manager it wraps checks it: at debug level if it's trusted
 * (with the cipher suite), and at warning level if it isn't, so a failed TLS handshake can be diagnosed.
 * (A failed handshake's chain isn't available any other way: there's no completed session to get it from.)
 * Otherwise delegates everything to the wrapped trust manager.
 * <p>
 * Each untrusted chain is logged at warning level at most once per {@link #WARNING_INTERVAL} (per map of reported
 * chains, which the caller keeps for the life of its client). Other failures with the same chain are logged at debug
 * level, without the chain, since retries would otherwise repeat it for every connection attempt. Repeating the
 * warning keeps a lasting problem visible in recent logs.
 * <p>
 * Thread-safe, if the wrapped trust manager is.
 */
@NullMarked
@Stability.Internal
public final class CertificateLoggingTrustManager extends X509ExtendedTrustManager {
  private static final Logger log = LoggerFactory.getLogger(CertificateLoggingTrustManager.class);

  /**
   * How often a chain that keeps failing is logged at warning level again.
   */
  static final Duration WARNING_INTERVAL = Duration.ofMinutes(15);

  /**
   * Bounds the reported chains. If they fill up, they're cleared, so a chain may be logged at warning level again.
   */
  static final int MAX_REPORTED_CHAINS = 100;

  private final X509ExtendedTrustManager delegate;
  private final Map<List<X509Certificate>, Long> reportedChains;

  private CertificateLoggingTrustManager(X509ExtendedTrustManager delegate, Map<List<X509Certificate>, Long> reportedChains) {
    this.delegate = requireNonNull(delegate);
    this.reportedChains = requireNonNull(reportedChains);
  }

  /**
   * Returns a new map for {@link #wrap}'s reported chains: when each was last logged at warning level (thread-safe).
   */
  public static Map<List<X509Certificate>, Long> newReportedChains() {
    return new ConcurrentHashMap<>();
  }

  /**
   * Returns the trust manager, wrapped. An {@link X509ExtendedTrustManager} (as the JDK's are) gets a wrapper that's
   * one too. A plain {@link X509TrustManager} gets a plain wrapper: the JDK adds checks of its own around a plain
   * trust manager (for example, algorithm constraints), which an extended wrapper would bypass.
   */
  public static X509TrustManager wrap(X509TrustManager trustManager, Map<List<X509Certificate>, Long> reportedChains) {
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
    private final Map<List<X509Certificate>, Long> reportedChains;

    private Plain(X509TrustManager delegate, Map<List<X509Certificate>, Long> reportedChains) {
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
    Map<List<X509Certificate>, Long> reportedChains,
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
    if (log.isDebugEnabled()) {
      log.debug(
        "TLS server certificate chain trusted. remote = {} ; cipher suite = {} ; certificate chain = \n{}",
        session == null ? null : remote(session),
        session == null ? null : session.getCipherSuite(),
        pemChain(chain)
      );
    }
  }

  private static void logUntrusted(
    Map<List<X509Certificate>, Long> reportedChains,
    X509Certificate[] chain,
    CertificateException e,
    @Nullable Object remote
  ) {
    if (shouldWarn(reportedChains, chain, System.nanoTime())) {
      log.warn(
        "TLS handshake failed: the server's certificate chain isn't trusted. remote = {} ; reason = {} ;" +
          " certificate chain (further failures with this chain are logged at debug level for {} minutes) = \n{}",
        remote,
        e.toString(),
        WARNING_INTERVAL.toMinutes(),
        pemChain(chain)
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
   * Returns true if the chain should be logged at warning level: if it wasn't, within the last
   * {@link #WARNING_INTERVAL} (certificates are equal if their encoded forms are). If so, records that it was.
   */
  static boolean shouldWarn(
    Map<List<X509Certificate>, Long> reportedChains,
    X509Certificate[] chain,
    long nowNanos
  ) {
    if (reportedChains.size() >= MAX_REPORTED_CHAINS) {
      reportedChains.clear();
    }
    List<X509Certificate> key = Arrays.asList(chain.clone());
    Long lastWarned = reportedChains.get(key);
    if (lastWarned != null && nowNanos - lastWarned < WARNING_INTERVAL.toNanos()) {
      return false;
    }
    // Two threads might both warn about the same chain, which does no harm.
    reportedChains.put(key, nowNanos);
    return true;
  }

  /**
   * Returns the certificates in PEM format, or an explanation if they can't be encoded.
   */
  static String pemChain(Certificate[] chain) {
    StringBuilder pemChain = new StringBuilder();
    try {
      for (Certificate cert : chain) {
        String base64Encoded = Base64.getMimeEncoder().encodeToString(cert.getEncoded());
        pemChain.append("-----BEGIN CERTIFICATE-----\n");
        pemChain.append(base64Encoded).append("\n");
        pemChain.append("-----END CERTIFICATE-----\n");
      }
    } catch (CertificateEncodingException e) {
      pemChain.setLength(0);
      pemChain.append("Can't display encoded certificate chain; ");
      pemChain.append(getStackTraceAsString(e));
    }
    return pemChain.toString();
  }
}
