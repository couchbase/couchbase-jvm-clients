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

import com.couchbase.client.core.deps.okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

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

class CertificateLoggingTrustManagerTest {
  private static final X509Certificate[] CHAIN = {
    new HeldCertificate.Builder().commonName("server").build().certificate()
  };

  @Test
  void wrapsExtendedTrustManagersInAnExtendedWrapperAndPlainOnesInAPlainWrapper() throws Exception {
    assertInstanceOf(CertificateLoggingTrustManager.class, CertificateLoggingTrustManager.wrap(mock(X509ExtendedTrustManager.class), CertificateLoggingTrustManager.newReportedChains()));

    // The JDK adds checks of its own around a plain X509TrustManager, so its wrapper must be plain too.
    X509TrustManager plain = mock(X509TrustManager.class);
    CertificateException untrusted = new CertificateException("PKIX path building failed");
    doThrow(untrusted).when(plain).checkServerTrusted(any(X509Certificate[].class), anyString());

    X509TrustManager wrapped = CertificateLoggingTrustManager.wrap(plain, CertificateLoggingTrustManager.newReportedChains());
    assertFalse(wrapped instanceof X509ExtendedTrustManager);
    assertSame(untrusted, assertThrows(CertificateException.class, () -> wrapped.checkServerTrusted(CHAIN, "RSA")));
  }

  @Test
  void rethrowsTheSameExceptionWhenTheChainIsNotTrusted() throws Exception {
    X509ExtendedTrustManager delegate = mock(X509ExtendedTrustManager.class);
    CertificateException untrusted = new CertificateException("PKIX path building failed");
    doThrow(untrusted).when(delegate).checkServerTrusted(any(X509Certificate[].class), anyString(), any(Socket.class));
    doThrow(untrusted).when(delegate).checkServerTrusted(any(X509Certificate[].class), anyString(), any(SSLEngine.class));
    doThrow(untrusted).when(delegate).checkServerTrusted(any(X509Certificate[].class), anyString());

    X509ExtendedTrustManager tm = (X509ExtendedTrustManager) CertificateLoggingTrustManager.wrap(delegate, CertificateLoggingTrustManager.newReportedChains());
    assertSame(untrusted, assertThrows(CertificateException.class, () -> tm.checkServerTrusted(CHAIN, "RSA", mock(Socket.class))));
    assertSame(untrusted, assertThrows(CertificateException.class, () -> tm.checkServerTrusted(CHAIN, "RSA", mock(SSLEngine.class))));
    assertSame(untrusted, assertThrows(CertificateException.class, () -> tm.checkServerTrusted(CHAIN, "RSA")));
  }

  @Test
  void delegatesEverythingElse() throws Exception {
    X509ExtendedTrustManager delegate = mock(X509ExtendedTrustManager.class);
    X509ExtendedTrustManager tm = (X509ExtendedTrustManager) CertificateLoggingTrustManager.wrap(delegate, CertificateLoggingTrustManager.newReportedChains());
    Socket socket = mock(Socket.class);

    tm.checkServerTrusted(CHAIN, "RSA", socket); // trusted: no exception
    tm.checkClientTrusted(CHAIN, "RSA", socket);
    tm.getAcceptedIssuers();

    verify(delegate).checkServerTrusted(CHAIN, "RSA", socket);
    verify(delegate).checkClientTrusted(CHAIN, "RSA", socket);
    verify(delegate).getAcceptedIssuers();
  }

  @Test
  void warnsAboutEachChainOnlyOnceWithinTheInterval() {
    Map<List<X509Certificate>, Long> reported = CertificateLoggingTrustManager.newReportedChains();
    X509Certificate[] other = {new HeldCertificate.Builder().commonName("other").build().certificate()};
    long interval = CertificateLoggingTrustManager.WARNING_INTERVAL.toNanos();

    assertTrue(CertificateLoggingTrustManager.shouldWarn(reported, CHAIN, 0));
    assertFalse(CertificateLoggingTrustManager.shouldWarn(reported, CHAIN.clone(), interval - 1), "an equal chain was just warned about");
    assertTrue(CertificateLoggingTrustManager.shouldWarn(reported, other, interval - 1), "a different chain is warned about too");
    assertTrue(CertificateLoggingTrustManager.shouldWarn(reported, CHAIN, interval), "warned about again once the interval has passed");
    assertFalse(CertificateLoggingTrustManager.shouldWarn(reported, CHAIN, interval + 1), "and the interval starts again");
  }

  @Test
  void reportedChainsAreBounded() {
    Map<List<X509Certificate>, Long> reported = CertificateLoggingTrustManager.newReportedChains();
    for (int i = 0; i < CertificateLoggingTrustManager.MAX_REPORTED_CHAINS * 2; i++) {
      CertificateLoggingTrustManager.shouldWarn(reported, new X509Certificate[]{
        new HeldCertificate.Builder().commonName("server-" + i).build().certificate()
      }, 0);
      assertTrue(reported.size() <= CertificateLoggingTrustManager.MAX_REPORTED_CHAINS);
    }
  }
}
