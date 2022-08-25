/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.netty.SslHandlerFactory;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityConfigTest {

  @Test
  void listsDefaultCiphersForNonNativeTls() {
    List<String> ciphers = SecurityConfig.defaultCiphers(false);
    assertFalse(ciphers.isEmpty());
    for (String cipher : ciphers) {
      assertTrue(cipher.startsWith("TLS_"));
    }
  }

  @Test
  void listsDefaultCiphersForNativeTls() {
    if (SslHandlerFactory.opensslAvailable()) {
      List<String> ciphers = SecurityConfig.defaultCiphers(true);
      assertFalse(ciphers.isEmpty());
      for (String cipher : ciphers) {
        assertTrue(cipher.startsWith("TLS_"));
      }
    } else {
      assertThrows(InvalidArgumentException.class, () -> SecurityConfig.defaultCiphers(true));
    }
  }

  @Test
  void trustOneCertificateFromFile() {
    checkCertificatesFromFile(
        "one-certificate.pem",
        listOf(
            "CN=Couchbase Server 1d6c9ec6"
        )
    );
  }

  @Test
  void trustTwoCertificatesFromFile() {
    checkCertificatesFromFile(
        "two-certificates.pem",
        listOf(
            "CN=Couchbase Server 1d6c9ec6",
            "CN=Couchbase Server f233ba43"
        )
    );
  }

  @Test
  void canReadDefaultCaCertificates() {
    List<X509Certificate> capella = SecurityConfig.capellaCaCertificates();
    List<X509Certificate> jvm = SecurityConfig.jvmCaCertificates();
    List<X509Certificate> defaults = SecurityConfig.defaultCaCertificates(); // capella + jvm
    assertFalse(capella.isEmpty());
    assertFalse(jvm.isEmpty());
    assertEquals(jvm.size() + capella.size(), defaults.size());
  }

  private void checkCertificatesFromFile(
      String resourceName,
      List<String> expectedSubjectDns
  ) {
    Path path = getResourceAsPath(getClass(), resourceName);
    SecurityConfig config = SecurityConfig.trustCertificate(path).build();

    assertEquals(
        expectedSubjectDns,
        config.trustCertificates().stream()
            .map(it -> it.getSubjectDN().getName())
            .collect(toList())
    );
  }

  private static Path getResourceAsPath(
      Class<?> loader,
      String resourceName
  ) {
    try {
      URL url = loader.getResource(resourceName);
      requireNonNull(url, "missing class path resource " + resourceName);
      return Paths.get(url.toURI());

    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }
}
