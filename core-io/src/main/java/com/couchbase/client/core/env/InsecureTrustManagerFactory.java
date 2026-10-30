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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.KeyStore;
import java.security.Provider;
import java.security.cert.X509Certificate;

/**
 * A {@link TrustManagerFactory} that trusts every certificate, without checking it.
 * <p>
 * <strong>For testing only.</strong> A connection that trusts every certificate isn't secure.
 */
@Stability.Internal
public final class InsecureTrustManagerFactory extends TrustManagerFactory {
  private static final Logger log = LoggerFactory.getLogger(InsecureTrustManagerFactory.class);

  private static final X509Certificate[] NO_CERTIFICATES = new X509Certificate[0];

  private static final TrustManager trustManager = new X509ExtendedTrustManager() {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
      if (log.isDebugEnabled()) {
        log.debug("Accepting a client certificate: {}", chain[0].getSubjectX500Principal());
      }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {
      if (log.isDebugEnabled()) {
        log.debug("Accepting a server certificate: {}", chain[0].getSubjectX500Principal());
      }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
      checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
      checkServerTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
      checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
      checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return NO_CERTIFICATES;
    }
  };

  @SuppressWarnings("deprecation") // Provider(String, double, String) is deprecated in Java 9, but Java 8 lacks the replacement
  private static final Provider PROVIDER = new Provider("", 0.0, "") {
  };

  // After the fields the constructor uses.
  public static final TrustManagerFactory INSTANCE = new InsecureTrustManagerFactory();

  private InsecureTrustManagerFactory() {
    super(new Spi(), PROVIDER, "");
  }

  private static final class Spi extends TrustManagerFactorySpi {
    @Override
    protected void engineInit(KeyStore keyStore) {
    }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) {
    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
      return new TrustManager[]{trustManager};
    }
  }
}
