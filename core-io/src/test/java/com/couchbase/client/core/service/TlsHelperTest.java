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

import com.couchbase.client.core.deps.okhttp3.OkHttpClient;
import com.couchbase.client.core.env.SecurityConfig;
import org.junit.jupiter.api.Test;

import javax.crypto.AEADBadTagException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocketFactory;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.security.cert.CertificateException;
import java.time.Duration;

import static com.couchbase.client.core.service.TlsHelper.CERTIFICATE_HINT;
import static com.couchbase.client.core.service.TlsHelper.CLIENT_CERTIFICATE_HINT;
import static com.couchbase.client.core.service.TlsHelper.CLIENT_CERTIFICATE_REQUIRED_HINT;
import static com.couchbase.client.core.service.TlsHelper.NOT_TLS_HINT;
import static com.couchbase.client.core.service.TlsHelper.NO_COMMON_PARAMETERS_HINT;
import static com.couchbase.client.core.service.TlsHelper.configureTls;
import static com.couchbase.client.core.service.TlsHelper.tlsFailureMayBeTemporary;
import static com.couchbase.client.core.service.TlsHelper.tlsFailure;
import static com.couchbase.client.core.service.TlsHelper.newEngineFactory;
import static com.couchbase.client.core.service.TlsHelper.permanentTlsFailureHint;
import static com.couchbase.client.core.service.TlsHelper.unsupportedCipherSuites;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TlsHelperTest {

  @Test
  void untrustedServerCertificate() {
    SSLHandshakeException e = new SSLHandshakeException("PKIX path building failed");
    e.initCause(new CertificateException("unable to find valid certification path to requested target"));
    assertEquals(CERTIFICATE_HINT, permanentTlsFailureHint(e));
  }

  @Test
  void hostNameMismatch() {
    assertEquals(CERTIFICATE_HINT, permanentTlsFailureHint(new SSLPeerUnverifiedException("Hostname node1 not verified")));
  }

  @Test
  void permanentFailureCanBeACause() {
    assertEquals(CERTIFICATE_HINT, permanentTlsFailureHint(new IOException(new SSLPeerUnverifiedException("Hostname node1 not verified"))));
  }

  @Test
  void serverDoesNotUseTls() {
    assertEquals(NOT_TLS_HINT, permanentTlsFailureHint(new SSLException("Unsupported or unrecognized SSL message")));
  }

  @Test
  void noTlsVersionOrCipherSuiteInCommon() {
    assertEquals(NO_COMMON_PARAMETERS_HINT, permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: protocol_version")));
    assertEquals(NO_COMMON_PARAMETERS_HINT, permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: handshake_failure")));
    assertEquals(NO_COMMON_PARAMETERS_HINT, permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: insufficient_security")));
    assertEquals(NO_COMMON_PARAMETERS_HINT, permanentTlsFailureHint(new SSLHandshakeException("No appropriate protocol (protocol is disabled or cipher suites are inappropriate)")));
    assertEquals(NO_COMMON_PARAMETERS_HINT, permanentTlsFailureHint(new SSLHandshakeException("The server selected protocol version TLS11 is not accepted by client preferences [TLS13, TLS12]")));
  }

  @Test
  void serverRejectedClientCertificate() {
    assertEquals(CLIENT_CERTIFICATE_HINT, permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: bad_certificate")));
    assertEquals(CLIENT_CERTIFICATE_HINT, permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: unknown_ca")));
    assertEquals(CLIENT_CERTIFICATE_HINT, permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: certificate_expired")));
    assertEquals(CLIENT_CERTIFICATE_REQUIRED_HINT, permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: certificate_required")));
  }

  @Test
  void alertWithNameBeforeTheMessage() {
    // Newer JDKs put the alert's name before the message.
    assertEquals(NO_COMMON_PARAMETERS_HINT, permanentTlsFailureHint(new SSLHandshakeException("(handshake_failure) Received fatal alert: handshake_failure")));
    assertEquals(CLIENT_CERTIFICATE_HINT, permanentTlsFailureHint(new SSLHandshakeException("(bad_certificate) Received fatal alert: bad_certificate")));
    assertEquals(CLIENT_CERTIFICATE_REQUIRED_HINT, permanentTlsFailureHint(new SSLHandshakeException("(certificate_required) Received fatal alert: certificate_required")));
  }

  @Test
  void alertWithOtherWording() {
    assertEquals(NO_COMMON_PARAMETERS_HINT, permanentTlsFailureHint(new SSLHandshakeException("handshake_failure(40)")));
    assertEquals(CLIENT_CERTIFICATE_HINT, permanentTlsFailureHint(new SSLHandshakeException("The server sent alert unknown_ca.")));
  }

  @Test
  void unknownAlertIsTemporary() {
    assertNull(permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: internal_error")));
    assertNull(permanentTlsFailureHint(new SSLHandshakeException("(internal_error) Received fatal alert: internal_error")));
  }

  @Test
  void alertNamesMatchWholeWords() {
    // Not "bad_certificate"
    assertNull(permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: bad_certificate_status_response")));
    assertNull(permanentTlsFailureHint(new SSLHandshakeException("(bad_certificate_hash_value) Received fatal alert: bad_certificate_hash_value")));
  }

  @Test
  void alertCanBeSuppressed() {
    // With TLS 1.3, the server can reject the client's certificate after the client has sent the request.
    // Writing the request fails, and OkHttp adds the alert as a suppressed exception.
    assertEquals(CLIENT_CERTIFICATE_HINT, permanentTlsFailureHint(brokenPipeWithSuppressed(new SSLHandshakeException("Received fatal alert: unknown_ca"))));
  }

  private static SocketException brokenPipeWithSuppressed(Exception suppressed) {
    SocketException e = new SocketException("Broken pipe");
    e.addSuppressed(suppressed);
    return e;
  }

  @Test
  void describedTlsFailureHasTheHint() {
    SSLHandshakeException alert = new SSLHandshakeException("Received fatal alert: unknown_ca");
    SocketException failure = brokenPipeWithSuppressed(alert);

    SSLException described = tlsFailure(failure);
    assertEquals("Received fatal alert: unknown_ca - " + CLIENT_CERTIFICATE_HINT, described.getMessage());
    assertSame(failure, described.getCause());
  }

  @Test
  void describedTlsFailureThatMightBeTemporaryIsUnchanged() {
    SSLHandshakeException terminated = new SSLHandshakeException("Remote host terminated the handshake");
    assertSame(terminated, tlsFailure(new IOException(terminated)));
    assertNull(tlsFailure(new ConnectException("Connection refused")));
  }

  @Test
  void otherTlsFailuresMightBeTemporary() {
    SSLHandshakeException terminated = new SSLHandshakeException("Remote host terminated the handshake");
    terminated.initCause(new EOFException("SSL peer shut down incorrectly"));
    assertNull(permanentTlsFailureHint(terminated));
    assertNull(permanentTlsFailureHint(new SSLHandshakeException("Received fatal alert: internal_error")));
  }

  @Test
  void corruptedRecordIsNotACertificateProblem() {
    // How the JDK reports a record that fails its integrity check: the cause is a GeneralSecurityException,
    // but not a CertificateException.
    assertNull(permanentTlsFailureHint(new SSLException("Tag mismatch", new AEADBadTagException("Tag mismatch"))));
  }

  @Test
  void unavailableCertificatesMightBeTemporary() {
    // For example, the authenticator couldn't load a key store, maybe because it's being replaced.
    Exception cause = new RuntimeException(new CertificateException("Unable to initialize"));
    assertNull(permanentTlsFailureHint(new PerConnectionSslSocketFactory.CertificatesUnavailableException(cause)));
    assertNull(permanentTlsFailureHint(brokenPipeWithSuppressed(new PerConnectionSslSocketFactory.CertificatesUnavailableException(cause))));
  }

  @Test
  void connectionFailingDuringTls() {
    SSLHandshakeException terminated = new SSLHandshakeException("Remote host terminated the handshake");
    terminated.initCause(new EOFException("SSL peer shut down incorrectly"));
    assertTrue(tlsFailureMayBeTemporary(terminated));
    assertTrue(tlsFailureMayBeTemporary(new SSLException("Connection reset", new SocketException("Connection reset"))));
    Exception cause = new RuntimeException(new CertificateException("Unable to initialize"));
    assertTrue(tlsFailureMayBeTemporary(new PerConnectionSslSocketFactory.CertificatesUnavailableException(cause)));

    // TLS rejecting the connection, even if retrying might help.
    assertFalse(tlsFailureMayBeTemporary(new SSLHandshakeException("Received fatal alert: internal_error")));
    assertFalse(tlsFailureMayBeTemporary(new SSLException("Tag mismatch", new AEADBadTagException("Tag mismatch"))));
    // The connection failed because of the alert.
    assertFalse(tlsFailureMayBeTemporary(brokenPipeWithSuppressed(new SSLHandshakeException("Received fatal alert: unknown_ca"))));
  }

  @Test
  void unsupportedCipherSuitesAreIgnored() {
    SecurityConfig securityConfig = SecurityConfig.builder()
      .enableTls(true)
      .ciphers(listOf("TLS_AES_128_GCM_SHA256", "TLS_BOGUS_WITH_NOTHING"))
      .build();

    SSLSocketFactory socketFactory = newEngineFactory(() -> null, securityConfig, Duration.ofSeconds(1));
    assertEquals(listOf("TLS_BOGUS_WITH_NOTHING"), unsupportedCipherSuites(socketFactory, securityConfig));
    // Only logged.
    configureTls(new OkHttpClient.Builder(), () -> null, securityConfig, Duration.ofSeconds(1));
  }

  @Test
  void supportedAndDefaultCipherSuitesAreNotReported() {
    SecurityConfig securityConfig = SecurityConfig.builder()
      .enableTls(true)
      .ciphers(listOf("TLS_AES_128_GCM_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"))
      .build();
    SSLSocketFactory socketFactory = newEngineFactory(() -> null, securityConfig, Duration.ofSeconds(1));
    assertEquals(emptyList(), unsupportedCipherSuites(socketFactory, securityConfig));

    // OkHttp's defaults include some that recent JVMs don't support.
    SecurityConfig defaults = SecurityConfig.builder().enableTls(true).build();
    assertEquals(emptyList(), unsupportedCipherSuites(socketFactory, defaults));
  }

  @Test
  void nonTlsFailuresAreIgnored() {
    assertNull(permanentTlsFailureHint(new ConnectException("Connection refused")));
    assertNull(permanentTlsFailureHint(new CertificateException("not wrapped in an SSLException")));
  }
}
