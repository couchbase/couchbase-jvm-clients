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
import com.couchbase.client.core.deps.okhttp3.CipherSuite;
import com.couchbase.client.core.deps.okhttp3.ConnectionSpec;
import com.couchbase.client.core.deps.okhttp3.OkHttpClient;
import com.couchbase.client.core.deps.okhttp3.TlsVersion;
import com.couchbase.client.core.deps.okhttp3.tls.HandshakeCertificates;
import com.couchbase.client.core.env.SecurityConfig;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.EOFException;
import java.net.Socket;
import java.net.SocketException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.couchbase.client.core.deps.okhttp3.ConnectionSpec.MODERN_TLS;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbThrowables.causesAndSuppressed;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

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
    Supplier<X509TrustManager> trustManagers = trustManagers(securityConfig, initialTrustManager);

    // OkHttp never uses "initialTrustManager" to decide what to trust; each connection's own TLS context
    // (with its own trust manager) does that. OkHttp only uses it to clean up certificate chains for:
    // certificate pinning (which we don't enable), HTTP/2 connection coalescing (we use HTTP/1.1 only),
    // and the certificates reported by Response.handshake() (which the SDK doesn't read).
    PerConnectionSslSocketFactory socketFactory = new PerConnectionSslSocketFactory(clientCertificates, trustManagers, handshakeTimeout);
    clientBuilder.sslSocketFactory(socketFactory, initialTrustManager);

    // Here only, so it's logged once per Core: KV connections that don't use Netty (see newTlsSocketFactory)
    // use the same cipher suites.
    List<String> unsupported = unsupportedCipherSuites(socketFactory, securityConfig);
    if (!unsupported.isEmpty()) {
      log.warn("Ignoring configured TLS cipher suites the JVM doesn't support (see SecurityConfig.ciphers): {}", unsupported);
    }

    if (certificateVerificationDisabled(securityConfig) || !securityConfig.hostnameVerificationEnabled()) {
      clientBuilder.hostnameVerifier(insecureHostnameVerifier);
    }
  }

  /**
   * Like the Netty implementation, asks a user-supplied trust manager factory for each new connection,
   * so trusted certificates can change (for example, if the user re-initializes the factory).
   */
  private static Supplier<X509TrustManager> trustManagers(SecurityConfig securityConfig, X509TrustManager initialTrustManager) {
    TrustManagerFactory tmf = securityConfig.trustManagerFactory();
    return tmf == null || certificateVerificationDisabled(securityConfig)
      ? () -> initialTrustManager
      : () -> firstX509TrustManager(tmf);
  }

  /**
   * Returns a factory for TLS connections that don't go through OkHttp (KV connections), with the same certificates
   * as {@link #configureTls}. They use its engines (see {@link PerConnectionSslSocketFactory#newEngine}); use
   * {@link #configureTlsEngine} on each one.
   *
   * @param clientCertificates supplies the client certificate (if any), asked once per new connection.
   * Typically the authenticator's {@code getKeyManagerFactory}.
   * @throws IllegalArgumentException if TLS isn't enabled.
   */
  static SSLSocketFactory newTlsSocketFactory(
    Supplier<@Nullable KeyManagerFactory> clientCertificates,
    SecurityConfig securityConfig,
    Duration handshakeTimeout
  ) {
    if (!securityConfig.tlsEnabled()) {
      throw new IllegalArgumentException("TLS is not enabled.");
    }
    X509TrustManager initialTrustManager = getTrustManager(securityConfig);
    return new PerConnectionSslSocketFactory(clientCertificates, trustManagers(securityConfig, initialTrustManager), handshakeTimeout);
  }

  /**
   * Returns the configured cipher suites the socket factory doesn't support, which connections leave out.
   * (The Netty implementation fails every connection instead, unless its TLS provider is OpenSSL,
   * and OpenSSL supports them.)
   */
  static List<String> unsupportedCipherSuites(SSLSocketFactory socketFactory, SecurityConfig securityConfig) {
    if (securityConfig.ciphers().isEmpty()) {
      return emptyList(); // the defaults
    }
    List<CipherSuite> configured = requireNonNull(secureConnectionSpec(securityConfig).cipherSuites());
    Set<CipherSuite> supported = Arrays.stream(socketFactory.getSupportedCipherSuites())
      .map(CipherSuite::forJavaName) // like OkHttp, matches names that differ only in their "TLS_" or "SSL_" prefix
      .collect(toSet());
    return configured.stream()
      .filter(it -> !supported.contains(it))
      .map(CipherSuite::javaName)
      .collect(toList());
  }

  /**
   * Returns the socket's names for the configured cipher suites it supports, in the configured order.
   * Like OkHttp, matches names that differ only in their "TLS_" or "SSL_" prefix.
   */
  private static List<String> supportedCipherSuites(List<CipherSuite> configured, String[] supported) {
    Map<CipherSuite, String> supportedNames = new HashMap<>();
    for (String name : supported) {
      supportedNames.putIfAbsent(CipherSuite.forJavaName(name), name);
    }
    return configured.stream()
      .map(supportedNames::get)
      .filter(Objects::nonNull)
      .collect(toList());
  }

  /**
   * Configures an engine (for example, from {@link PerConnectionSslSocketFactory#newEngine}) like
   * {@link #configureTls} configures OkHttp's sockets: the same TLS versions and cipher suites, and checks
   * the server's host name unless that's disabled. Call before the handshake starts.
   */
  static void configureTlsEngine(SSLEngine engine, SecurityConfig securityConfig) {
    engine.setSSLParameters(tlsParameters(engine.getSSLParameters(), engine.getSupportedProtocols(), engine.getSupportedCipherSuites(), securityConfig));
  }

  private static SSLParameters tlsParameters(
    SSLParameters params,
    String[] supportedProtocolNames,
    String[] supportedCipherSuites,
    SecurityConfig securityConfig
  ) {
    ConnectionSpec spec = secureConnectionSpec(securityConfig);

    // Only the versions and cipher suites the socket supports, like OkHttp does.
    List<String> supportedProtocols = Arrays.asList(supportedProtocolNames);
    params.setProtocols(requireNonNull(spec.tlsVersions()).stream()
      .map(TlsVersion::javaName)
      .filter(supportedProtocols::contains)
      .toArray(String[]::new));

    List<CipherSuite> cipherSuites = spec.cipherSuites();
    if (cipherSuites != null) {
      params.setCipherSuites(supportedCipherSuites(cipherSuites, supportedCipherSuites).toArray(new String[0]));
    }

    // OkHttp checks the host name itself, after the handshake. Here, the JDK checks it during the handshake,
    // like the Netty implementation (so a mismatch fails with a CertificateException, which gets CERTIFICATE_HINT).
    if (!certificateVerificationDisabled(securityConfig) && securityConfig.hostnameVerificationEnabled()) {
      params.setEndpointIdentificationAlgorithm("HTTPS");
    }
    return params;
  }

  static final String CERTIFICATE_HINT =
    "Check that the SDK trusts the cluster's certificate, and that the certificate matches the host name.";
  static final String CLIENT_CERTIFICATE_HINT =
    "The server rejected the SDK's client certificate. Check the certificate (for example, that it hasn't expired), and that the cluster trusts its issuer.";
  static final String CLIENT_CERTIFICATE_REQUIRED_HINT =
    "The server requires a client certificate, but the SDK didn't send one. Check that the SDK is configured with a client certificate (for example, with CertificateAuthenticator), and that the cluster trusts its issuer (the SDK only sends a certificate whose issuer the server accepts).";
  static final String NOT_TLS_HINT =
    "The server doesn't use TLS on this port. Check server ports, and whether the cluster uses TLS (couchbases://) or not (couchbase://).";
  static final String NO_COMMON_PARAMETERS_HINT =
    "The SDK and the server have no TLS version or cipher suite in common. Check the SDK's and the cluster's TLS settings.";

  /**
   * Hints for the TLS alerts a server sends when retrying won't help.
   * <p>
   * When the server rejects the client's certificate, Couchbase Server's cluster manager sends "bad_certificate",
   * and its services written in Go send "unknown_ca". With TLS 1.3, a server that requires a client certificate
   * sends "certificate_required" if the client doesn't send one (as the data service does). When there's no TLS version or cipher suite in common,
   * the server sends the alert in reply to the ClientHello, having read everything the client sent, so the alert
   * reaches the client. (With no cipher suite in common, the cluster manager sends "insufficient_security",
   * and the Go services send "handshake_failure".)
   */
  private static final Map<String, String> ALERT_HINTS = alertHints();

  private static Map<String, String> alertHints() {
    Map<String, String> result = new HashMap<>();
    for (String alert : listOf(
      "bad_certificate",
      "unsupported_certificate",
      "certificate_revoked",
      "certificate_expired",
      "certificate_unknown",
      "unknown_ca",
      "access_denied"
    )) {
      result.put(alert, CLIENT_CERTIFICATE_HINT);
    }
    result.put("certificate_required", CLIENT_CERTIFICATE_REQUIRED_HINT);
    for (String alert : listOf(
      "protocol_version",
      "handshake_failure",
      "insufficient_security"
    )) {
      result.put(alert, NO_COMMON_PARAMETERS_HINT);
    }
    return unmodifiableMap(result);
  }

  /**
   * Splits a message into words that could be alert names, which are lowercase with underscores.
   * Matching whole words, not wording around them, works with the JDK's messages for an alert from the server,
   * both "Received fatal alert: handshake_failure" from older JDKs and
   * "(handshake_failure) Received fatal alert: handshake_failure" from newer ones.
   */
  private static final Pattern NOT_ALERT_NAME_CHARACTERS = Pattern.compile("[^a-z_]+");

  /**
   * Hints for the JDK's own TLS failures, by part of their message.
   */
  private static final Map<String, String> MESSAGE_HINTS = mapOf(
    "Unsupported or unrecognized SSL message", NOT_TLS_HINT, // the server's reply isn't TLS
    "No appropriate protocol", NO_COMMON_PARAMETERS_HINT, // the client's settings leave no usable version or cipher suite
    "not accepted by client preferences", NO_COMMON_PARAMETERS_HINT // the server chose a version the client won't use
  );

  /**
   * If the failure is a TLS failure that retrying won't fix, because it needs someone to change the certificates
   * or the TLS settings, returns a hint saying what to check. Otherwise (it isn't a TLS failure, or it might be
   * temporary, like the server closing the connection during the handshake because it's restarting) returns null.
   * <p>
   * Detects:
   * <ul>
   *   <li>The client doesn't trust the server's certificate, or the certificate doesn't match the host name.
   *   <li>The server rejected the client's certificate, if its alert reaches the client. With Couchbase Server,
   *       it does with TLS 1.3. With TLS 1.2, the services written in Go typically close the connection
   *       without reading the rest of the handshake, so the connection is reset before the client sees why.
   *   <li>The server doesn't use TLS on that port, if it answers the ClientHello, as Couchbase Server's
   *       HTTP ports do. (A server that closes the connection, or waits for more of what it takes to be a request
   *       until the handshake times out, isn't detected.)
   *   <li>The client and server have no TLS version or cipher suite in common.
   * </ul>
   * Some failures are recognized by the JDK's messages. With a TLS provider that words them differently,
   * they're treated as temporary.
   */
  static @Nullable String permanentTlsFailureHint(Throwable t) {
    List<SSLException> failures = tlsFailures(t);
    if (certificatesUnavailable(failures)) {
      return null;
    }
    return failures.stream()
      .map(OkHttpTlsHelper::hint)
      .filter(Objects::nonNull)
      .findFirst()
      .orElse(null);
  }

  /**
   * Returns the TLS failure among the failure's causes and suppressed exceptions, or null if there's none.
   * If retrying won't fix it, returns a new exception whose message is extended with a hint saying what to check
   * (see {@link #permanentTlsFailureHint}), and whose cause is the given failure.
   */
  static @Nullable SSLException describeTlsFailure(Throwable t) {
    List<SSLException> failures = tlsFailures(t);
    if (!certificatesUnavailable(failures)) {
      for (SSLException e : failures) {
        String hint = hint(e);
        if (hint != null) {
          // A plain SSLException, since events and log messages show the exception's class name.
          return new SSLException(e.getMessage() + " - " + hint, t);
        }
      }
    }
    return failures.isEmpty() ? null : failures.get(0);
  }

  /**
   * Returns true if the TLS failure is the connection failing (for example, the server closed it during the handshake
   * because it's restarting), or the SDK couldn't get the certificates, rather than TLS rejecting the connection.
   * Like the Netty implementation, which doesn't count those as TLS failures, either.
   */
  static boolean connectionFailedDuringTls(Throwable t) {
    List<SSLException> failures = tlsFailures(t);
    return certificatesUnavailable(failures)
      || failures.stream().allMatch(it -> hasCause(it, EOFException.class) || hasCause(it, SocketException.class));
  }

  /**
   * Returns true if the SDK couldn't get the certificates for the connection. That might be temporary,
   * whatever the supplier's exception says.
   */
  private static boolean certificatesUnavailable(List<SSLException> failures) {
    return failures.stream().anyMatch(it -> it instanceof PerConnectionSslSocketFactory.CertificatesUnavailableException);
  }

  /**
   * Returns the TLS failures among the failure's causes and suppressed exceptions (and theirs).
   * <p>
   * A server's alert isn't always the cause. For example, if the server rejects the client's certificate
   * after the client has finished its side of the handshake (TLS 1.3), writing the request fails with
   * "Broken pipe", and OkHttp adds the alert to that exception as a suppressed exception.
   */
  private static List<SSLException> tlsFailures(Throwable t) {
    return causesAndSuppressed(t).stream()
      .filter(it -> it instanceof SSLException)
      .map(it -> (SSLException) it)
      .collect(toList());
  }

  private static @Nullable String hint(SSLException e) {
    if (e instanceof SSLPeerUnverifiedException // OkHttp's hostname verification failed
      || hasCause(e, CertificateException.class)) { // untrusted, expired, or (JDK's check) doesn't match the host name
      return CERTIFICATE_HINT;
    }

    // Newer JDKs also name alerts the client raises, as in "(certificate_unknown) PKIX path building failed",
    // so this comes after the certificate checks above.
    String message = String.valueOf(e.getMessage());
    String alertHint = Arrays.stream(NOT_ALERT_NAME_CHARACTERS.split(message))
      .map(ALERT_HINTS::get)
      .filter(Objects::nonNull)
      .findFirst()
      .orElse(null);
    if (alertHint != null) {
      return alertHint;
    }

    return MESSAGE_HINTS.entrySet().stream()
      .filter(it -> message.contains(it.getKey()))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(null);
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
