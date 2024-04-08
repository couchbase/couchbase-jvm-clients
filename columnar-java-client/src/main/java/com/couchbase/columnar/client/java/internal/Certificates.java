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

package com.couchbase.columnar.client.java.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.SecurityConfig;

import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Utility methods for reading TLS certificates from various locations.
 */
@Stability.Internal
public class Certificates {
  private Certificates() {
    throw new AssertionError("not instantiable");
  }

  /**
   * @param pemCertificates The PEM-encoded certificate(s) to decode and return.
   */
  public static List<X509Certificate> parse(String pemCertificates) {
    return read(new ByteArrayInputStream(pemCertificates.getBytes(UTF_8)));
  }


  public static List<X509Certificate> read(InputStream pemStream) {
    return decodeCertificates(pemStream);
  }

  public static List<X509Certificate> read(Path pemFilePath) {
    try (InputStream is = Files.newInputStream(pemFilePath)) {
      return decodeCertificates(is);

    } catch (IOException e) {
      throw new UncheckedIOException(e);

    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to read certificates from file \"" + pemFilePath + "\"", e);
    }
  }

  /**
   * Returns the Certificate Authority (CA) certificates trusted by the JVM's default trust manager.
   */
  public static List<X509Certificate> getJvmCertificates() {
    return SecurityConfig.jvmCaCertificates();
//    return Arrays.stream(getDefaultTrustManagerFactory().getTrustManagers())
//      .filter(it -> it instanceof X509TrustManager)
//      .map(it -> (X509TrustManager) it)
//      .flatMap(it -> Arrays.stream(it.getAcceptedIssuers()))
//      .collect(toList());
  }

  /**
   * Returns the Certificate Authority (CA) certificates used by Couchbase Capella.
   */
  public static List<X509Certificate> getCapellaCertificates() {
    return SecurityConfig.capellaCaCertificates();
  }

  /**
   * Returns the Certificate Authority (CA) certificates used by
   * Couchbase internal pre-production clusters.
   */
  @Stability.Internal
  public static List<X509Certificate> getNonProdCertificates() {
    return parse("-----BEGIN CERTIFICATE-----\n" +
      "MIIDFTCCAf2gAwIBAgIRANguFcFZ7eVLTF2mnPqkkhYwDQYJKoZIhvcNAQELBQAw\n" +
      "JDESMBAGA1UECgwJQ291Y2hiYXNlMQ4wDAYDVQQLDAVDbG91ZDAeFw0xOTEwMTgx\n" +
      "NDUzMzRaFw0yOTEwMTgxNTUzMzRaMCQxEjAQBgNVBAoMCUNvdWNoYmFzZTEOMAwG\n" +
      "A1UECwwFQ2xvdWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDMoL2G\n" +
      "1yR4XKOL5KrAZbgJI11NkcooxqCSqoibr5nSM+GNARlou42XbopRhkLQlSMlmH7U\n" +
      "ZreI7xq2MqmCaQvP1jdS5al/GwuwAP+2kU2nz4IHzliCVV6YvYqNy0fygNpYky9/\n" +
      "wjCu32n8Ae0AZuxcsAzPUtJBvIIGHum08WlLYS3gNrYkfyds6LfvZvqMk703RL5X\n" +
      "Ny/RXWmbbBXAXh0chsavEK7EsDLI4t4WI2Iv8+lwS7Wo7Vh6NnEmJLPAAp7udNK4\n" +
      "U3nwjkL5p/yINROT7CxUE9x0IB2l2rZwZiJhgHCpee77J8QesDut+jZu38ZYY3le\n" +
      "PS38S81T6I6bSSgtAgMBAAGjQjBAMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYE\n" +
      "FLlocLdzgAeibrlCmEO4OH5Buf3vMA4GA1UdDwEB/wQEAwIBhjANBgkqhkiG9w0B\n" +
      "AQsFAAOCAQEAkoVX5CJ7rGx2ALfzy5C7Z+tmEmrZ6jdHjDtw4XwWNhlrsgMuuboU\n" +
      "Y9XMinSSm1TVfvIz4ru82MVMRxq4v1tPwPdZabbzKYclHkwSMxK5BkyEKWzF1Hoq\n" +
      "UcinTaT68lVzkTc0D8T+gkRzwXIqxjML2ZdruD1foHNzCgeGHzKzdsjYqrnHv17b\n" +
      "J+f5tqoa5CKbnyWl3HP0k7r3HHQP0GQequoqXcL3XlERX3Ne20Chck9mftNnHhKw\n" +
      "Dby7ylZaP97sphqOZQ/W/gza7x1JYylrLXvjfdv3Nmu7oSMKO/2cDyWwcbVGkpbk\n" +
      "8JOQtFENWmr9u2S0cQfwoCSYBWaK0ofivA==\n" +
      "-----END CERTIFICATE-----\n"
    );
  }

  private static List<X509Certificate> decodeCertificates(InputStream pem) {
    requireNonNull(pem);
    try {
      //noinspection unchecked
      return (List<X509Certificate>) getX509CertificateFactory()
        .generateCertificates(pem);
    } catch (CertificateException e) {
      throw new IllegalArgumentException("Failed to decode certificates", e);
    }
  }

  private static CertificateFactory getX509CertificateFactory() {
    try {
      return CertificateFactory.getInstance("X.509");
    } catch (CertificateException e) {
      throw new RuntimeException("Could not instantiate X.509 CertificateFactory", e);
    }
  }

//  public static List<X509Certificate> defaultCaCertificates() {
//    return SecurityConfig.defaultCaCertificates();
//    List<X509Certificate> result = new ArrayList<>();
//    result.addAll(capellaCaCertificates());
//    result.addAll(getJvmCertificates());
//    return result;
//  }

  private static TrustManagerFactory getDefaultTrustManagerFactory() {
    try {
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init((KeyStore) null); // passing null populates it with the default certificates
      return tmf;
    } catch (NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException(e);
    }
  }

  private static InputStream getResource(String resourceName) {
    try (InputStream is = SecurityConfig.class.getResourceAsStream(resourceName)) {
      if (is == null) {
        throw new RuntimeException("Missing resource: " + resourceName);
      }
      return is;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
