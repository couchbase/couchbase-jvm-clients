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

import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

class InsecureTrustManagerFactoryTest {

  @Test
  void trustsAnyCertificate() throws Exception {
    InsecureTrustManagerFactory.INSTANCE.init((KeyStore) null);
    TrustManager[] trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
    assertEquals(1, trustManagers.length);

    // Extended, so the JDK doesn't wrap it with checks of its own (like the host name).
    X509ExtendedTrustManager trustManager = assertInstanceOf(X509ExtendedTrustManager.class, trustManagers[0]);
    X509Certificate[] chain = {mock(X509Certificate.class)};
    trustManager.checkServerTrusted(chain, "RSA");
    trustManager.checkClientTrusted(chain, "RSA");
    assertEquals(0, trustManager.getAcceptedIssuers().length);
  }

  @Test
  @SuppressWarnings("deprecation")
  void deprecatedInstanceIsTheSameInstance() {
    assertSame(
      InsecureTrustManagerFactory.INSTANCE,
      com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE
    );
  }
}
