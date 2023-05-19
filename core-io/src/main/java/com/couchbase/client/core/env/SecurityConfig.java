/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.netty.SslHandlerFactory;
import com.couchbase.client.core.util.Bytes;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

/**
 * The {@link SecurityConfig} allows to enable transport encryption between the client and the servers.
 */
public class SecurityConfig {

  @Stability.Internal
  public static class Defaults {
    /**
     * By default, TLS is disabled.
     */
    public static final boolean DEFAULT_TLS_ENABLED = false;

    /**
     * By default, netty native tls (OpenSSL) is enabled for better performance.
     */
    public static final boolean DEFAULT_NATIVE_TLS_ENABLED = true;

    /**
     * By default, hostname verification for TLS connections is enabled.
     */
    public static final boolean DEFAULT_HOSTNAME_VERIFICATION_ENABLED = true;

    /**
     * By default, certificate verification for TLS connections is enabled.
     */
    public static final boolean DEFAULT_CERTIFICATE_VERIFICATION_ENABLED = true;
  }

  @Stability.Internal
  public static class InternalMethods {
    public static boolean userSpecifiedTrustSource(SecurityConfig config) {
      return config.userSpecifiedTrustSource;
    }
  }

  private final boolean nativeTlsEnabled;
  private final boolean hostnameVerificationEnabled;
  private final boolean tlsEnabled;
  private final List<X509Certificate> trustCertificates;
  private final TrustManagerFactory trustManagerFactory;
  private final List<String> ciphers;
  private final boolean userSpecifiedTrustSource;

  /**
   * Creates a builder to customize the {@link SecurityConfig} configuration.
   *
   * @return the builder to customize.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a {@link SecurityConfig} with the default configuration.
   *
   * @return the default security config.
   */
  public static SecurityConfig create() {
    return new SecurityConfig(builder());
  }

  /**
   * Enables TLS for all client/server communication (disabled by default).
   *
   * @param tlsEnabled true if enabled, false otherwise.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder enableTls(boolean tlsEnabled) {
    return builder().enableTls(tlsEnabled);
  }

  /**
   * Allows enabling or disabling hostname verification (enabled by default).
   * <p>
   * Note that disabling hostname verification will cause the TLS connection to not verify that the hostname/ip
   * is actually part of the certificate and as a result not detect certain kinds of attacks. Only disable if
   * you understand the impact and risks!
   *
   * @param hostnameVerificationEnabled set to true if it should be enabled, false for disabled.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder enableHostnameVerification(boolean hostnameVerificationEnabled) {
    return builder().enableHostnameVerification(hostnameVerificationEnabled);
  }

  /**
   * Enables/disables native TLS (enabled by default).
   *
   * @param nativeTlsEnabled true if it should be enabled, false otherwise.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder enableNativeTls(boolean nativeTlsEnabled) {
    return builder().enableNativeTls(nativeTlsEnabled);
  }

  /**
   * Loads the given list of X.509 certificates into the trust store.
   *
   * @param certificates the list of certificates to load.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder trustCertificates(final List<X509Certificate> certificates) {
    return builder().trustCertificates(certificates);
  }

  /**
   * Loads X.509 certificates from the specified file into the trust store.
   *
   * @param certificatePath the path to load the certificates from.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder trustCertificate(final Path certificatePath) {
    return builder().trustCertificate(certificatePath);
  }

  /**
   * Initializes the {@link TrustManagerFactory} with the given trust store.
   *
   * @param trustStore the loaded trust store to use.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder trustStore(final KeyStore trustStore) {
    return builder().trustStore(trustStore);
  }

  /**
   * Loads a trust store from a file path and password and initializes the {@link TrustManagerFactory}.
   *
   * @param trustStorePath the path to the truststore.
   * @param trustStorePassword the password (can be null if not password protected).
   * @param trustStoreType the type of the trust store. If empty, the {@link KeyStore#getDefaultType()} will be used.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder trustStore(final Path trustStorePath, final String trustStorePassword,
                                   final Optional<String> trustStoreType) {
    return builder().trustStore(trustStorePath, trustStorePassword, trustStoreType);
  }

  /**
   * Allows to provide a trust manager factory directly for maximum flexibility.
   * <p>
   * While providing the most flexibility, most users will find the other overloads more convenient, like passing
   * in a {@link #trustStore(KeyStore)} directly or via filepath {@link #trustStore(Path, String, Optional)}.
   *
   * @param trustManagerFactory the trust manager factory to use.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder trustManagerFactory(final TrustManagerFactory trustManagerFactory) {
    return builder().trustManagerFactory(trustManagerFactory);
  }

  /**
   * Allows to customize the list of ciphers that is negotiated with the cluster.
   * <p>
   * Note that this method is considered advanced API, please only customize the cipher list if you know what
   * you are doing (for example if you want to shrink the cipher list down  to a very specific subset for security
   * or compliance reasons).
   * <p>
   * If no custom ciphers are configured, the default set will be used.
   *
   * @param ciphers the custom list of ciphers to use.
   * @return this {@link Builder} for chaining purposes.
   */
  public static Builder ciphers(final List<String> ciphers) {
    return  builder().ciphers(ciphers);
  }

  private SecurityConfig(final Builder builder) {
    tlsEnabled = builder.tlsEnabled;
    nativeTlsEnabled = builder.nativeTlsEnabled;
    trustManagerFactory = builder.certificateVerificationEnabled
      ? builder.trustManagerFactory
      : InsecureTrustManagerFactory.INSTANCE;
    hostnameVerificationEnabled = builder.hostnameVerificationEnabled;
    ciphers = builder.ciphers;

    if (!tlsEnabled) {
      trustCertificates = builder.trustCertificates;
      userSpecifiedTrustSource = true; // well, they specified not to use TLS!
      return;
    }

    if (builder.trustCertificates != null && builder.trustManagerFactory != null) {
      throw InvalidArgumentException.fromMessage("Either trust certificates or a trust manager factory" +
        " can be provided, but not both!");
    }

    if (!builder.certificateVerificationEnabled) {
      userSpecifiedTrustSource = true; // well, they specified to trust any certificate!
      trustCertificates = null;
      return;
    }

    userSpecifiedTrustSource = builder.trustCertificates != null || builder.trustManagerFactory != null;

    // If no trust settings have been specified, trust the default CA certificates.
    trustCertificates = userSpecifiedTrustSource
      ? builder.trustCertificates
      : defaultCaCertificates();
  }

  /**
   * True if TLS is enabled, false otherwise.
   *
   * @return a boolean if tls/transport encryption is enabled.
   */
  public boolean tlsEnabled() {
    return tlsEnabled;
  }

  /**
   * True if TLS hostname verification is enabled, false otherwise.
   */
  public boolean hostnameVerificationEnabled() {
    return hostnameVerificationEnabled;
  }

  /**
   * The list of trust certificates that should be used, if present.
   *
   * @return the list of certificates.
   */
  public List<X509Certificate> trustCertificates() {
    return trustCertificates;
  }

  /**
   * The currently configured trust manager factory, if present.
   *
   * @return the trust manager factory.
   */
  public TrustManagerFactory trustManagerFactory() {
    return trustManagerFactory;
  }

  /**
   * Returns whether native TLS is enabled.
   *
   * @return true if enabled, false otherwise.
   */
  public boolean nativeTlsEnabled() {
    return nativeTlsEnabled;
  }

  /**
   * Returns the custom list of ciphers.
   *
   * @return the custom list of ciphers.
   */
  public List<String> ciphers() {
    return ciphers;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    final Map<String, Object> export = new LinkedHashMap<>();
    export.put("tlsEnabled", tlsEnabled);
    export.put("nativeTlsEnabled", nativeTlsEnabled);
    export.put("hostnameVerificationEnabled", hostnameVerificationEnabled);
    export.put("trustCertificates", trustCertificates != null ? trustCertificatesToString() : null);
    export.put("trustManagerFactory", trustManagerFactory != null ? trustManagerFactory.getClass().getSimpleName() : null);
    export.put("ciphers", ciphers);
    return export;
  }

  /**
   * This builder allows to customize the default security configuration.
   */
  public static class Builder {

    private boolean tlsEnabled = Defaults.DEFAULT_TLS_ENABLED;
    private boolean nativeTlsEnabled = Defaults.DEFAULT_NATIVE_TLS_ENABLED;
    private boolean hostnameVerificationEnabled = Defaults.DEFAULT_HOSTNAME_VERIFICATION_ENABLED;
    private boolean certificateVerificationEnabled = Defaults.DEFAULT_CERTIFICATE_VERIFICATION_ENABLED;

    private List<X509Certificate> trustCertificates = null;
    private TrustManagerFactory trustManagerFactory = null;
    private List<String> ciphers = Collections.emptyList();

    /**
     * Builds the {@link SecurityConfig} out of this builder.
     *
     * @return the built security config.
     */
    public SecurityConfig build() {
      return new SecurityConfig(this);
    }

    /**
     * Enables TLS for all client/server communication (disabled by default).
     *
     * @param tlsEnabled true if enabled, false otherwise.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enableTls(boolean tlsEnabled) {
      this.tlsEnabled = tlsEnabled;
      return this;
    }

    /**
     * Allows to enable or disable hostname verification (enabled by default).
     * <p>
     * Note that disabling hostname verification will cause the TLS connection to not verify that the hostname/ip
     * is actually part of the certificate and as a result not detect certain kinds of attacks. Only disable if
     * you understand the impact and risks!
     *
     * @param hostnameVerificationEnabled set to true if it should be enabled, false for disabled.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enableHostnameVerification(boolean hostnameVerificationEnabled) {
      this.hostnameVerificationEnabled = hostnameVerificationEnabled;
      return this;
    }

    /**
     * Pass false to bypass all TLS certificate verification checks.
     * This is equivalent to calling {@link #trustManagerFactory(TrustManagerFactory)}
     * with an argument of {@link InsecureTrustManagerFactory#INSTANCE}.
     * <p>
     * Certificate verification is enabled by default.
     * <p>
     * Certificate verification <b>must never be disabled in a production environment</b>, and should be disabled in
     * development only if there is no better solution. The better solution is almost always to specify
     * the CA certificate(s) to trust, by calling {@link #trustCertificate(Path)} or some variant.
     * <p>
     * See also {@link #enableHostnameVerification}, which can selectively disable just hostname verification.
     *
     * @param certificateVerificationEnabled Pass false to set the trust manager factory to {@link InsecureTrustManagerFactory#INSTANCE},
     * and bypass all TLS certificate verification checks.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Volatile
    public Builder enableCertificateVerification(final boolean certificateVerificationEnabled) {
      this.certificateVerificationEnabled = certificateVerificationEnabled;
      return this;
    }

    /**
     * Enables/disables native TLS (enabled by default).
     *
     * @param nativeTlsEnabled true if it should be enabled, false otherwise.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enableNativeTls(boolean nativeTlsEnabled) {
      this.nativeTlsEnabled = nativeTlsEnabled;
      return this;
    }

    /**
     * Loads the given list of X.509 certificates into the trust store.
     *
     * @param certificates the list of certificates to load.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder trustCertificates(final List<X509Certificate> certificates) {
      this.trustCertificates = notNullOrEmpty(certificates, "X509 Certificates");
      return this;
    }

    /**
     * Loads X.509 certificates from the file at the given path into the trust store.
     * <p>
     * TIP: If you have multiple certificate files in PEM format (for example,
     * "cert1.pem" and "cert2.pem"), and you want to create a single PEM file
     * containing all the certificates, concatenate the PEM files using this shell command:
     * <pre>
     * $ cat cert1.pem cert2.pem &gt; both-certs.pem
     * </pre>
     * Then, when configuring the SDK, call this method with the path to `both-certs.pem`
     * as the argument.
     *
     * @param certificatePath the file to load the certificates from.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder trustCertificate(final Path certificatePath) {
      notNull(certificatePath, "CertificatePath");

      try (InputStream is = Files.newInputStream(certificatePath)) {
        return trustCertificates(decodeCertificates(Bytes.readAllBytes(is)));

      } catch (IOException e) {
        throw InvalidArgumentException.fromMessage(
          "Could not read trust certificates from file \"" + certificatePath + "\"",
          e
        );
      }
    }

    /**
     * Allows to provide a trust manager factory directly for maximum flexibility.
     * <p>
     * While providing the most flexibility, most users will find the other overloads more convenient, like passing
     * in a {@link #trustStore(KeyStore)} directly or via filepath {@link #trustStore(Path, String, Optional)}.
     *
     * @param trustManagerFactory the trust manager factory to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder trustManagerFactory(final TrustManagerFactory trustManagerFactory) {
      this.trustManagerFactory = notNull(trustManagerFactory, "TrustManagerFactory");
      return this;
    }

    /**
     * Initializes the {@link TrustManagerFactory} with the given trust store.
     *
     * @param trustStore the loaded trust store to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder trustStore(final KeyStore trustStore) {
      notNull(trustStore, "TrustStore");

      try {
        final TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return trustManagerFactory(tmf);
      } catch (Exception ex) {
        throw InvalidArgumentException.fromMessage(
          "Could not initialize TrustManagerFactory from TrustStore",
          ex
        );
      }
    }

    /**
     * Loads a trust store from a file path and password and initializes the {@link TrustManagerFactory}.
     *
     * @param trustStorePath the path to the truststore.
     * @param trustStorePassword the password (can be null if not password protected).
     * @param trustStoreType the type of the trust store. If empty, the {@link KeyStore#getDefaultType()} will be used.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder trustStore(final Path trustStorePath, final String trustStorePassword,
                              final Optional<String> trustStoreType) {
      notNull(trustStorePath, "TrustStorePath");
      notNull(trustStoreType, "TrustStoreType");

      try (InputStream trustStoreInputStream = Files.newInputStream(trustStorePath)) {
        final KeyStore store = KeyStore.getInstance(trustStoreType.orElse(KeyStore.getDefaultType()));
        store.load(
          trustStoreInputStream,
          trustStorePassword != null ? trustStorePassword.toCharArray() : null
        );
        return trustStore(store);
      } catch (Exception ex) {
        throw InvalidArgumentException.fromMessage("Could not initialize TrustStore", ex);
      }
    }

    /**
     * Allows to customize the list of ciphers that is negotiated with the cluster.
     * <p>
     * Note that this method is considered advanced API, please only customize the cipher list if you know what
     * you are doing (for example if you want to shrink the cipher list down to a very specific subset for security
     * or compliance reasons).
     * <p>
     * If no custom ciphers are configured, the default set will be used.
     * <p>
     * If you wish to add additional ciphers instead of providing an exclusive list, you can use the static
     * {@link #defaultCiphers(boolean)} method to load the default list first, add your own ciphers and then
     * pass it into this method.
     *
     * @param ciphers the custom list of ciphers to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder ciphers(final List<String> ciphers) {
      this.ciphers = notNullOrEmpty(ciphers, "Ciphers");
      return this;
    }

  }

  /**
   * Helper method to decode string-encoded certificates into their x.509 format.
   *
   * @param certificates the string-encoded certificates.
   * @return the decoded certs in x.509 format.
   */
  public static List<X509Certificate> decodeCertificates(final List<String> certificates) {
    notNull(certificates, "Certificates");

    return certificates.stream()
        .flatMap(it -> decodeCertificates(it.getBytes(UTF_8)).stream())
        .collect(toList());
  }

  private static List<X509Certificate> decodeCertificates(byte[] bytes) {
    notNull(bytes, "bytes");

    try {
      //noinspection unchecked
      return (List<X509Certificate>) getX509CertificateFactory()
          .generateCertificates(new ByteArrayInputStream(bytes));
    } catch (CertificateException e) {
      String inputAsString = new String(bytes, UTF_8);
      throw InvalidArgumentException.fromMessage("Could not generate certificates from raw input: \"" + inputAsString + "\"", e);
    }
  }

  private static CertificateFactory getX509CertificateFactory() {
    try {
      return CertificateFactory.getInstance("X.509");
    } catch (CertificateException e) {
      throw new CouchbaseException("Could not instantiate X.509 CertificateFactory", e);
    }
  }

  /**
   * Lists the default ciphers used for this platform.
   * <p>
   * Note that the list of ciphers can differ whether native TLS is enabled or not, so the parameter should reflect
   * the actual security configuration used. Native TLS is enabled by default on the configuration, so if it is not
   * overridden it should be set to true here as well.
   *
   * @param nativeTlsEnabled if native TLS is enabled on the security configuration (defaults to yes there).
   * @return the list of default ciphers.
   */
  public static List<String> defaultCiphers(final boolean nativeTlsEnabled) {
    return SslHandlerFactory.defaultCiphers(nativeTlsEnabled);
  }

  private String trustCertificatesToString() {
    if (isNullOrEmpty(trustCertificates)) {
      return null;
    }

    return trustCertificates.stream()
        .map(it -> it.getSubjectDN() + " (valid from " + it.getNotBefore().toInstant() + " to " + it.getNotAfter().toInstant() + ")")
        .collect(toList())
        .toString();
  }

  /**
   * Returns the Certificate Authority (CA) certificates that are trusted if
   * no other certificate (or other trust source) is specified in the security config.
   * <p>
   * Includes the CA certificate(s) required for connecting to hosted Couchbase Capella clusters,
   * plus CA certificates trusted by the JVM's default trust manager.
   */
  @Stability.Volatile
  public static List<X509Certificate> defaultCaCertificates() {
    List<X509Certificate> result = new ArrayList<>();
    result.addAll(capellaCaCertificates());
    result.addAll(jvmCaCertificates());
    return result;
  }

  /**
   * Returns the Certificate Authority (CA) certificates required for connecting to Couchbase Capella.
   */
  @Stability.Volatile
  public static List<X509Certificate> capellaCaCertificates() {
    return decodeCertificates(getResourceAsBytes("capella-ca.pem"));
  }

  /**
   * Returns the Certificate Authority (CA) certificates trusted by the JVM's default trust manager.
   * This is a subset of the certificates returned by {@link #defaultCaCertificates()};
   * it does not include the Couchbase Capella CA certificate.
   */
  @Stability.Volatile
  public static List<X509Certificate> jvmCaCertificates() {
    return Arrays.stream(getDefaultTrustManagerFactory().getTrustManagers())
        .filter(it -> it instanceof X509TrustManager)
        .map(it -> (X509TrustManager) it)
        .flatMap(it -> Arrays.stream(it.getAcceptedIssuers()))
        .collect(toList());
  }

  private static TrustManagerFactory getDefaultTrustManagerFactory() {
    try {
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init((KeyStore) null); // passing null populates it with the default certificates
      return tmf;
    } catch (NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] getResourceAsBytes(String resourceName) {
    try (InputStream is = SecurityConfig.class.getResourceAsStream(resourceName)) {
      if (is == null) {
        throw new RuntimeException("Missing resource: " + resourceName);
      }
      return Bytes.readAllBytes(is);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
