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
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.SecurityConfig;
import okhttp3.ConnectionSpec;
import okhttp3.OkHttpClient;
import okhttp3.TlsVersion;
import okhttp3.tls.HandshakeCertificates;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Objects.requireNonNull;
import static okhttp3.ConnectionSpec.MODERN_TLS;

@Stability.Internal
class OkHttpTlsHelper {

  private static final Logger log = LoggerFactory.getLogger(OkHttpTlsHelper.class);

  private OkHttpTlsHelper() {
  }

  static void configureTls(
    OkHttpClient.Builder clientBuilder,
    Authenticator credential,
    SecurityConfig securityConfig
  ) {
    if (!securityConfig.tlsEnabled()) {
      clientBuilder.connectionSpecs(listOf(ConnectionSpec.CLEARTEXT));
      return;
    }

    clientBuilder.connectionSpecs(listOf(secureConnectionSpec(securityConfig)));

    KeyManager[] keyManagers = getKeyManagers(credential);
    X509TrustManager trustManager = getTrustManager(securityConfig);
    clientBuilder.sslSocketFactory(
      newSocketFactory(keyManagers, trustManager),
      trustManager
    );

    if (certificateVerificationDisabled(securityConfig) || !securityConfig.hostnameVerificationEnabled()) {
      clientBuilder.hostnameVerifier(insecureHostnameVerifier);
    }
  }

  private static SSLSocketFactory newSocketFactory(
    KeyManager @Nullable [] keyManagers,
    TrustManager trustManager
  ) {
    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(
        keyManagers,
        new TrustManager[]{trustManager},
        null
      );
      return sslContext.getSocketFactory();

    } catch (KeyManagementException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean certificateVerificationDisabled(SecurityConfig securityConfig) {
    return securityConfig.trustManagerFactory() instanceof InsecureTrustManagerFactory;
  }

  private static X509TrustManager getFirstX509TrustManager(TrustManagerFactory tmf) {
      TrustManager[] tms = tmf.getTrustManagers();
      X509TrustManager tm = Arrays.stream(tms)
        .filter(it -> it instanceof X509TrustManager)
        .map(it -> (X509TrustManager) it)
        .findFirst().orElse(null);

      if (tm == null) {
        throw new IllegalArgumentException("The provided TrustManagerFactory did not return an X509TrustManager.");
      }
      if (tms.length != 1) {
        log.warn("The provided TrustManagerFactory returned multiple trust managers. Only the first X509TrustManager will be used.");
      }
      return tm;
  }

  private static X509TrustManager getTrustManager(SecurityConfig securityConfig) {
    if (certificateVerificationDisabled(securityConfig)) {
      return insecureTrustManager;
    }

    TrustManagerFactory tmf = securityConfig.trustManagerFactory();
    if (tmf != null) {
      return getFirstX509TrustManager(tmf);
    }

    HandshakeCertificates.Builder builder = new HandshakeCertificates.Builder();
    List<X509Certificate> certs = securityConfig.trustCertificates();
    if (certs != null) {
      certs.forEach(builder::addTrustedCertificate);
    }
    return builder.build().trustManager();
  }

  private static KeyManager @Nullable [] getKeyManagers(Authenticator auth) {
    KeyManagerFactory kmf = auth.getKeyManagerFactory();
    return kmf == null ? null : kmf.getKeyManagers();
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
