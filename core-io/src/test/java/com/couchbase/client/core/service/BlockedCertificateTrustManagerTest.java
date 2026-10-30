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

import com.couchbase.client.core.deps.okhttp3.CertificatePinner;
import com.couchbase.client.core.deps.okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class BlockedCertificateTrustManagerTest {
  private static final HeldCertificate ROOT = new HeldCertificate.Builder()
    .commonName("root")
    .certificateAuthority(1)
    .build();

  private static final HeldCertificate INTERMEDIATE = new HeldCertificate.Builder()
    .commonName("intermediate")
    .certificateAuthority(0)
    .signedBy(ROOT)
    .build();

  private static final HeldCertificate SERVER = new HeldCertificate.Builder()
    .commonName("server")
    .signedBy(INTERMEDIATE)
    .build();

  private static final X509Certificate[] CHAIN = {SERVER.certificate(), INTERMEDIATE.certificate()};

  private static final Set<String> INTERMEDIATE_BLOCKED = setOf(BlockedCertificateTrustManager.publicKeySha256(INTERMEDIATE.certificate()));

  @Test
  void hashesPublicKeysLikeOkHttp() {
    // OkHttp's blocklist uses CertificatePinner's hash: "sha256/" + base64 of the SHA-256 of the SubjectPublicKeyInfo.
    String pin = CertificatePinner.pin(SERVER.certificate());
    byte[] okHttpHash = Base64.getDecoder().decode(pin.substring("sha256/".length()));

    StringBuilder hex = new StringBuilder();
    for (byte b : okHttpHash) {
      hex.append(String.format("%02x", b));
    }
    assertEquals(hex.toString(), BlockedCertificateTrustManager.publicKeySha256(SERVER.certificate()));
  }

  @Test
  void blocksEightCertificatesByDefault() {
    assertEquals(8, BlockedCertificateTrustManager.BLOCKED_PUBLIC_KEY_SHA256.size());
    for (String hash : BlockedCertificateTrustManager.BLOCKED_PUBLIC_KEY_SHA256) {
      assertTrue(hash.matches("[0-9a-f]{64}"), hash);
    }
  }

  @Test
  void rejectsATrustedChainThatIncludesABlockedCertificate() throws Exception {
    X509ExtendedTrustManager tm = (X509ExtendedTrustManager) BlockedCertificateTrustManager.wrap(
      mock(X509ExtendedTrustManager.class), // trusts everything
      INTERMEDIATE_BLOCKED
    );

    assertBlocked(() -> tm.checkServerTrusted(CHAIN, "RSA", mock(Socket.class)));
    assertBlocked(() -> tm.checkServerTrusted(CHAIN, "RSA", mock(SSLEngine.class)));
    assertBlocked(() -> tm.checkServerTrusted(CHAIN, "RSA"));
  }

  @Test
  void acceptsATrustedChainWithoutBlockedCertificates() throws Exception {
    X509ExtendedTrustManager delegate = mock(X509ExtendedTrustManager.class);
    X509ExtendedTrustManager tm = (X509ExtendedTrustManager) BlockedCertificateTrustManager.wrap(
      delegate,
      setOf(BlockedCertificateTrustManager.publicKeySha256(ROOT.certificate())) // not in the chain the server sent
    );
    Socket socket = mock(Socket.class);

    tm.checkServerTrusted(CHAIN, "RSA", socket);
    verify(delegate).checkServerTrusted(CHAIN, "RSA", socket);
  }

  @Test
  void rethrowsTheDelegatesExceptionWhenTheChainIsNotTrusted() throws Exception {
    X509ExtendedTrustManager delegate = mock(X509ExtendedTrustManager.class);
    CertificateException untrusted = new CertificateException("PKIX path building failed");
    doThrow(untrusted).when(delegate).checkServerTrusted(any(X509Certificate[].class), anyString(), any(Socket.class));

    X509ExtendedTrustManager tm = (X509ExtendedTrustManager) BlockedCertificateTrustManager.wrap(delegate, INTERMEDIATE_BLOCKED);
    assertSame(untrusted, assertThrows(CertificateException.class, () -> tm.checkServerTrusted(CHAIN, "RSA", mock(Socket.class))));
  }

  @Test
  void wrapsPlainTrustManagersInAPlainWrapper() {
    // The JDK adds checks of its own around a plain X509TrustManager, so its wrapper must be plain too.
    assertInstanceOf(BlockedCertificateTrustManager.class, BlockedCertificateTrustManager.wrap(mock(X509ExtendedTrustManager.class)));

    X509TrustManager wrapped = BlockedCertificateTrustManager.wrap(mock(X509TrustManager.class), INTERMEDIATE_BLOCKED);
    assertFalse(wrapped instanceof X509ExtendedTrustManager);
    assertBlocked(() -> wrapped.checkServerTrusted(CHAIN, "RSA"));
  }

  @Test
  void delegatesEverythingElse() throws Exception {
    X509ExtendedTrustManager delegate = mock(X509ExtendedTrustManager.class);
    X509ExtendedTrustManager tm = (X509ExtendedTrustManager) BlockedCertificateTrustManager.wrap(delegate, INTERMEDIATE_BLOCKED);
    Socket socket = mock(Socket.class);

    tm.checkClientTrusted(CHAIN, "RSA", socket); // client chains aren't checked against the blocklist
    tm.getAcceptedIssuers();

    verify(delegate).checkClientTrusted(CHAIN, "RSA", socket);
    verify(delegate).getAcceptedIssuers();
  }

  private interface Check {
    void run() throws CertificateException;
  }

  private static void assertBlocked(Check check) {
    CertificateException e = assertThrows(CertificateException.class, check::run);
    assertTrue(e.getMessage().contains("blocked"), e.getMessage());
    assertTrue(e.getMessage().contains("CN=intermediate"), e.getMessage());
  }
}
