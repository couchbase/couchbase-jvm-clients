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

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.msg.RequestContext;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Checks that {@link CouchbaseOkHttpClient} asks a user-supplied {@link TrustManagerFactory} for its
 * trust manager on each new connection, like the Netty implementation, so the trusted certificates
 * can change without restarting the SDK.
 */
class ServerTrustRotationTest {

  /**
   * Connect to the IP address, not "localhost". If a host name has several addresses (like
   * 127.0.0.1 and ::1), OkHttp throws the exception from the last address it tried, which might
   * be "connection refused" from an address the server isn't listening on, instead of the TLS failure.
   */
  private static final String LOOPBACK_IP = "127.0.0.1";

  private static final HeldCertificate SERVER_X = new HeldCertificate.Builder()
    .commonName("server-x")
    .addSubjectAlternativeName(LOOPBACK_IP)
    .build();
  private static final HeldCertificate SERVER_Y = new HeldCertificate.Builder()
    .commonName("server-y")
    .addSubjectAlternativeName(LOOPBACK_IP)
    .build();

  private final List<TestHttpServer> servers = new ArrayList<>();
  private CouchbaseOkHttpClient client;

  @AfterEach
  void cleanup() {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      servers.forEach(TestHttpServer::close);
    }
  }

  @Test
  void newConnectionsUseTheTrustManagerFactorysCurrentTrustManager() throws Exception {
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trust(tmf, SERVER_X);

    client = new CouchbaseOkHttpClient(
      Duration.ofSeconds(5),
      IoConfig.create(),
      true, // native IO enabled, like the default environment
      SecurityConfig.builder().enableTls(true).trustManagerFactory(tmf).build(),
      PasswordAuthenticator.create("username", "password"),
      "test-user-agent"
    );
    CouchbaseOkHttpClient http = client;

    TestHttpServer serverX = startServer(SERVER_X);
    TestHttpServer serverY = startServer(SERVER_Y);

    get(http, serverX); // trusted

    IOException e = assertThrows(IOException.class, () -> get(http, serverY)); // not trusted (yet)
    assertTrue(hasCause(e, SSLHandshakeException.class), "expected a TLS handshake failure, but got: " + e);

    // The user re-initializes the same factory, now trusting Y too.
    trust(tmf, SERVER_X, SERVER_Y);

    get(http, serverY); // a new connection, which asks the factory again
    assertEquals(1, serverY.requests().size());
  }

  /**
   * (Re-)initializes the factory to trust the given certificates.
   */
  private static void trust(TrustManagerFactory tmf, HeldCertificate... certs) throws Exception {
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);
    for (int i = 0; i < certs.length; i++) {
      trustStore.setCertificateEntry("cert" + i, certs[i].certificate());
    }
    tmf.init(trustStore);
  }

  private TestHttpServer startServer(HeldCertificate serverCert) throws IOException {
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder().heldCertificate(serverCert).build();
    TestHttpServer server = TestHttpServer.startHttps(serverCerts.sslContext());
    servers.add(server);
    return server;
  }

  private static void get(CouchbaseOkHttpClient http, TestHttpServer server) throws IOException {
    server.enqueue("ok");
    HttpUrl url = new HttpUrl.Builder()
      .scheme("https")
      .host(LOOPBACK_IP)
      .port(server.port())
      .build();
    try (Response response = http.newCall(
      new Request.Builder().url(url),
      mock(RequestContext.class),
      Duration.ofSeconds(5)
    ).execute()) {
      assertEquals(200, response.code());
      response.body().string();
    }
  }
}
