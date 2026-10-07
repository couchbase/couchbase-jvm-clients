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
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import okhttp3.ConnectionSpec;
import okhttp3.OkHttpClient;
import okhttp3.TlsVersion;
import okhttp3.tls.HandshakeCertificates;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Objects.requireNonNull;
import static okhttp3.ConnectionSpec.MODERN_TLS;

@Stability.Internal
class OkHttpTlsHelper {

  private static final Logger log = LoggerFactory.getLogger(OkHttpTlsHelper.class);

  private OkHttpTlsHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * @param clientCertificates supplies the client certificate (if any), asked once per new connection.
   * Typically the authenticator's {@code getKeyManagerFactory}.
   */
  static void configureTls(
    OkHttpClient.Builder clientBuilder,
    Supplier<@Nullable KeyManagerFactory> clientCertificates,
    SecurityConfig securityConfig,
    Duration handshakeTimeout
  ) {
    if (!securityConfig.tlsEnabled()) {
      clientBuilder.connectionSpecs(listOf(ConnectionSpec.CLEARTEXT));
      return;
    }

    clientBuilder.connectionSpecs(listOf(secureConnectionSpec(securityConfig)));

    // Fails now if the trust manager factory is unusable (for example, has no X.509 trust manager).
    X509TrustManager initialTrustManager = getTrustManager(securityConfig);

    // Like the Netty implementation, ask a user-supplied trust manager factory for each new connection,
    // so trusted certificates can change (for example, if the user re-initializes the factory).
    TrustManagerFactory tmf = securityConfig.trustManagerFactory();
    Supplier<X509TrustManager> trustManagers = tmf == null || certificateVerificationDisabled(securityConfig)
      ? () -> initialTrustManager
      : () -> firstX509TrustManager(tmf);

    // OkHttp never uses "initialTrustManager" to decide what to trust; each connection's own TLS context
    // (with its own trust manager) does that. OkHttp only uses it to clean up certificate chains for:
    // certificate pinning (which we don't enable), HTTP/2 connection coalescing (we use HTTP/1.1 only),
    // and the certificates reported by Response.handshake() (which the SDK doesn't read).
    clientBuilder.sslSocketFactory(new PerConnectionSslSocketFactory(clientCertificates, trustManagers, handshakeTimeout), initialTrustManager);

    if (certificateVerificationDisabled(securityConfig) || !securityConfig.hostnameVerificationEnabled()) {
      clientBuilder.hostnameVerifier(insecureHostnameVerifier);
    }
  }

  private static boolean certificateVerificationDisabled(SecurityConfig securityConfig) {
    return securityConfig.trustManagerFactory() instanceof InsecureTrustManagerFactory;
  }

  /**
   * Returns the factory's first X.509 trust manager.
   */
  private static X509TrustManager firstX509TrustManager(TrustManagerFactory tmf) {
    return Arrays.stream(tmf.getTrustManagers())
      .filter(it -> it instanceof X509TrustManager)
      .map(it -> (X509TrustManager) it)
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("The provided TrustManagerFactory did not return an X509TrustManager."));
  }

  private static X509TrustManager getTrustManager(SecurityConfig securityConfig) {
    if (certificateVerificationDisabled(securityConfig)) {
      return insecureTrustManager;
    }

    TrustManagerFactory tmf = securityConfig.trustManagerFactory();
    if (tmf != null) {
      // Warn once, when the client is created, not for every connection.
      if (tmf.getTrustManagers().length != 1) {
        log.warn("The provided TrustManagerFactory returned multiple trust managers. Only the first X509TrustManager will be used.");
      }
      return firstX509TrustManager(tmf);
    }

    HandshakeCertificates.Builder builder = new HandshakeCertificates.Builder();
    List<X509Certificate> certs = securityConfig.trustCertificates();
    if (certs != null) {
      certs.forEach(builder::addTrustedCertificate);
    }
    return builder.build().trustManager();
  }

  private static ConnectionSpec secureConnectionSpec(SecurityConfig security) {
    if (security.ciphers().isEmpty()) return MODERN_TLS;

    return new ConnectionSpec.Builder(true)
      .tlsVersions(requireNonNull(MODERN_TLS.tlsVersions()).toArray(new TlsVersion[0]))
      .cipherSuites(security.ciphers().toArray(new String[0]))
      .build();
  }

  private static final X509Certificate[] EMPTY_CERT_ARRAY = new X509Certificate[0];

  private static final X509TrustManager insecureTrustManager = new X509ExtendedTrustManager() {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return EMPTY_CERT_ARRAY;
    }

    @Override
    public String toString() {
      return "InsecureTrustManager";
    }
  };

  private static final HostnameVerifier insecureHostnameVerifier = new HostnameVerifier() {
    @Override
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }

    @Override
    public String toString() {
      return "InsecureHostnameVerifier";
    }
  };

}
