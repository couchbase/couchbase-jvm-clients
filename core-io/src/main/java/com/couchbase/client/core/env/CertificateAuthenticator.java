/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.env;

import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.KeyManagerFactory;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Performs authentication through a client certificate.
 */
public class CertificateAuthenticator implements Authenticator {

  private final PrivateKey key;
  private final String keyPassword;
  private final List<X509Certificate> keyCertChain;
  private final Supplier<KeyManagerFactory> keyManagerFactory;

  public static CertificateAuthenticator fromKeyManagerFactory(final Supplier<KeyManagerFactory> keyManagerFactory) {
    notNull(keyManagerFactory, "KeyManagerFactory");
    return new CertificateAuthenticator(null, null, null, keyManagerFactory);
  }

  public static CertificateAuthenticator fromKey(final PrivateKey key, final String keyPassword,
                                                 final List<X509Certificate> keyCertChain) {
    notNull(key, "PrivateKey");
    return new CertificateAuthenticator(key, keyPassword, keyCertChain, null);
  }

  private CertificateAuthenticator(final PrivateKey key, final String keyPassword,
                                   final List<X509Certificate> keyCertChain,
                                   final Supplier<KeyManagerFactory> keyManagerFactory) {
    this.key = key;
    this.keyPassword = keyPassword;
    this.keyCertChain = keyCertChain;
    this.keyManagerFactory = keyManagerFactory;

    if (key != null && keyManagerFactory != null) {
      throw new IllegalArgumentException("Either a key certificate or a key manager factory" +
        " can be provided, but not both!");
    }
  }

  @Override
  public void applyTlsProperties(final SslContextBuilder context) {
    if (keyManagerFactory != null) {
      context.keyManager(keyManagerFactory.get());
    } else if (key != null) {
      context.keyManager(key, keyPassword, keyCertChain.toArray(new X509Certificate[0]));
    }
  }

  @Override
  public boolean supportsNonTls() {
    return false;
  }

  @Override
  public String toString() {
    return "CertificateAuthenticator{" +
      "key=" + key +
      ", keyPassword='" + keyPassword + '\'' +
      ", keyCertChain=" + keyCertChain +
      ", keyManagerFactory=" + keyManagerFactory +
      '}';
  }
}
