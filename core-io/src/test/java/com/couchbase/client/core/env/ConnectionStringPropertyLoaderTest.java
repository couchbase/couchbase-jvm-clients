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

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionStringPropertyLoaderTest {

  private static final String CERT_CONTENT = "-----BEGIN CERTIFICATE-----\n" +
    "MIIDAjCCAeqgAwIBAgIIFdYhhtJl+DIwDQYJKoZIhvcNAQELBQAwJDEiMCAGA1UE\n" +
    "AxMZQ291Y2hiYXNlIFNlcnZlciBjOTVkMTQxNDAeFw0xMzAxMDEwMDAwMDBaFw00\n" +
    "OTEyMzEyMzU5NTlaMCQxIjAgBgNVBAMTGUNvdWNoYmFzZSBTZXJ2ZXIgYzk1ZDE0\n" +
    "MTQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC3YNOnhnxgUDUlK7Le\n" +
    "bIvH6GRZOgQsXnjlI3GG41/4ljL8kjxAB+pnh5lNx/4Gg3+j20mK6kG1Ufku2FMD\n" +
    "JxtrEVUX3z8ShJWO2bIesDw5UOSwinBFr9D1p8hkZ+qqQaBbNbVdmSSwnO4OfC2K\n" +
    "BmKTK5X0N4sLc+aKcirUGvqJeNMG2+gLbWCWQFASuVyLU6OkfOO6/UJA9EUUhMVT\n" +
    "G9plDijp69bIy7HmR5wGVrrFm6hLizGg8Tz2II/Pz9pCbEr4KteWK2PkUeRq/iEA\n" +
    "FH7B9BKwJxhrO2Sv8PJbhLhIYKe87F/TWmwp0UA506vgLMymg4QU9vlxoU2jhLnq\n" +
    "1FBjAgMBAAGjODA2MA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggrBgEFBQcD\n" +
    "ATAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAk2xTOg1V0mzeM\n" +
    "STS/An9YxClmdeBccejAb2ZSHavJBAcmu4o2HFFtC0Wn1GkcL5kc1l4+2ryyAu1j\n" +
    "5vaDUShIfcmq26U1bLCeEaRKgF1Mu3KSM2Xl3rBiWQspSM2BCAMBaxOi19CP49o6\n" +
    "Rt7HIv3I+A6HlsVgZLW52aboAvKp/7Xv7kvbkwH4zdyQS88xns92y/51cdCgK83k\n" +
    "YP0mq+o400fBee8DYzwZ+UVp2JSqd6vb9mNHocdl3CvjB7O60X5FkGmHZuKgCxQl\n" +
    "wkP9wC+Uq+aF6/KGfC+GzKMrK0wKfbjj+Kwa2XGDdJCjQopRyHA5ByMJucMqBBrn\n" +
    "C+LUvohH\n" +
    "-----END CERTIFICATE-----\n";

  @Test
  void shouldApplyPropertiesFromConnectionString() {
    parse("couchbase://127.0.0.1?io.maxHttpConnections=23&io.configPollInterval=2m", env -> {
      assertEquals(23, env.ioConfig().maxHttpConnections());
      assertEquals(Duration.ofMinutes(2), env.ioConfig().configPollInterval());
    });
  }

  @Test
  void shouldEnableEncryptionWithScheme() throws IOException {
    String certPath = "cert.pem";
    try {
      Files.write(Paths.get(certPath), CERT_CONTENT.getBytes(StandardCharsets.UTF_8));
      parse("couchbases://127.0.0.1?security.trustCertificate=" + certPath, env -> {
        assertTrue(env.securityConfig().tlsEnabled());
        assertEquals(1, env.securityConfig().trustCertificates().size());
      });
    } finally {
      Files.delete(Paths.get(certPath));
    }
  }

  @Test
  void shouldEnableEncryptionWithCompatName() throws IOException {
    String certPath = "cert.pem";
    try {
      Files.write(Paths.get(certPath), CERT_CONTENT.getBytes(StandardCharsets.UTF_8));
      parse("couchbases://127.0.0.1?certpath=" + certPath, env -> {
        assertTrue(env.securityConfig().tlsEnabled());
        assertEquals(1, env.securityConfig().trustCertificates().size());
      });
    } finally {
      Files.delete(Paths.get(certPath));
    }
  }

  @Test
  void shouldSupportInsecureTlsWithCompatName() {
    parse(
      "couchbases://127.0.0.1?tls_verify=none",
      env -> assertSame(env.securityConfig().trustManagerFactory(), InsecureTrustManagerFactory.INSTANCE)
    );

    parse(
      "couchbases://127.0.0.1?tls_verify=peer",
      env -> assertNull(env.securityConfig().trustManagerFactory())
    );

    assertThrows(IllegalArgumentException.class, () ->
      parse(
        "couchbases://127.0.0.1?tls_verify=bogus",
        env -> {
        }
      )
    );
  }

  @Test
  void shouldParseVariousSdk3Compats() {
    parse(
      "couchbase://127.0.0.1?kv_connect_timeout=3s",
      env -> assertEquals(Duration.ofSeconds(3), env.timeoutConfig().connectTimeout())
    );
    parse(
      "couchbase://127.0.0.1?enable_mutation_tokens=false",
      env -> assertFalse(env.ioConfig().mutationTokensEnabled())
    );
    parse(
      "couchbase://127.0.0.1?num_kv_connections=4",
      env -> assertEquals(4, env.ioConfig().numKvConnections())
    );
    parse(
      "couchbase://127.0.0.1?network=external",
      env -> assertEquals(NetworkResolution.EXTERNAL, env.ioConfig().networkResolution())
    );
  }

  /**
   * Helper method to parse a connection string into the env and clean it up afterwards.
   *
   * @param connectionString the connection string to parse.
   * @param consumer the consumer which will get the full env to assert against.
   */
  private void parse(final String connectionString, final Consumer<CoreEnvironment> consumer) {
    CoreEnvironment.Builder<?> builder = CoreEnvironment.builder();
    ConnectionStringPropertyLoader loader = new ConnectionStringPropertyLoader(connectionString);
    loader.load(builder);
    CoreEnvironment built = builder.build();
    try {
      consumer.accept(built);
    } finally {
      built.shutdown();
    }
  }

}
