/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.columnar.client.java.internal.Certificates;
import com.couchbase.columnar.client.java.internal.TrustSource;
import reactor.util.annotation.Nullable;

import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listCopyOf;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class SecurityOptions {
  SecurityOptions() {
  }

  private List<String> cipherSuites = emptyList();
  @Nullable private TrustSource trustSource = null;
  private boolean disableServerCertificateVerification = false;

  Unmodifiable build() {
    return new Unmodifiable(this);
  }

  /**
   * @param cipherSuites Names of the cipher suites to allow for TLS,
   * or empty list to allow any suite supported by the runtime environment.
   */
  public SecurityOptions cipherSuites(List<String> cipherSuites) {
    this.cipherSuites = listCopyOf(cipherSuites);
    return this;
  }

  /**
   * Clears any existing trust settings, and tells the SDK to trust
   * only the Capella CA certificates bundled with this SDK.
   * <p>
   * This is the default trust setting.
   */
  public SecurityOptions trustOnlyCapella() {
    return trustOnlyCertificates(Certificates.getCapellaCertificates());
  }

  /**
   * Clears any existing trust settings, and tells the SDK to trust
   * only the certificates in the specified PEM file.
   */
  public SecurityOptions trustOnlyPemFile(Path pemFile) {
    return trustSource(TrustSource.from(pemFile));
  }

  /**
   * Clears any existing trust settings, and tells the SDK to trust
   * only the PEM-encoded certificates contained in the given string.
   */
  public SecurityOptions trustOnlyPemString(String pemEncodedCertificates) {
    return trustSource(TrustSource.from(Certificates.parse(pemEncodedCertificates)));
  }

  /**
   * Clears any existing trust settings, and tells the SDK to trust
   * only the specified certificates.
   */
  public SecurityOptions trustOnlyCertificates(List<X509Certificate> certificates) {
    return trustSource(TrustSource.from(certificates));
  }

  /**
   * Clears any existing trust settings, and tells the SDK to trust
   * only the certificates trusted by the Java runtime environment.
   */
  public SecurityOptions trustOnlyJvm() {
    return trustSource(TrustSource.fromJvm());
  }

  /**
   * Clears any existing trust settings, and tells the SDK to use
   * the specified factory to verify server certificates.
   * <p>
   * For advanced use cases only.
   *
   * @see #trustOnlyPemFile(Path)
   */
  public SecurityOptions trustOnlyFactory(TrustManagerFactory factory) {
    return trustSource(TrustSource.from(factory));
  }

  /**
   * Server certification verification is enabled by default.
   * You can disable it by passing true to this method,
   * but you almost certainly shouldn't. Instead, call one of the
   * {@code trust} methods to tell the SDK which certificates
   * it should trust.
   * <p>
   * <b>IMPORTANT:</b> Disabling verification is insecure
   * because it exposes you to on-path attacks. Never do this in production.
   * In fact, you probably shouldn't do it anywhere.
   *
   * @param disable If true, the SDK does not verify the certificate
   * presented by the server.
   * @see #trustOnlyPemFile(Path)
   * @see #trustOnlyPemString(String)
   * @see #trustOnlyCertificates(List)
   * @see #trustOnlyFactory(TrustManagerFactory)
   * @deprecated Not really deprecated, but disabling verification
   * is almost always a bad idea.
   */
  @Deprecated
  public SecurityOptions disableServerCertificateVerification(boolean disable) {
    this.disableServerCertificateVerification = disable;
    return this;
  }

  private SecurityOptions trustSource(TrustSource trustSource) {
    this.trustSource = requireNonNull(trustSource);
    return this;
  }

  static class Unmodifiable {
    private final List<String> cipherSuites;
    private final TrustSource trustSource;

    Unmodifiable(SecurityOptions builder) {
      this.cipherSuites = builder.cipherSuites;
      this.trustSource = builder.disableServerCertificateVerification
        ? TrustSource.insecure()
        : (builder.trustSource != null ? builder.trustSource : TrustSource.from(Certificates.getCapellaCertificates()));
    }

    public List<String> cipherSuites() {
      return cipherSuites;
    }

    // todo for users who want to inspect this, should we make TrustSource public,
    // or expose separate fields for certificates and factory?
    @Stability.Internal
    TrustSource trustSource() {
      return trustSource;
    }

    @Override
    public String toString() {
      return "SecuritySettings{" +
        "cipherSuites=" + cipherSuites +
        ", trustSource=" + trustSource +
        '}';
    }

  }
}
