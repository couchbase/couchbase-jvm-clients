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

import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.msg.RequestContext;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.IOException;
import java.net.Socket;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Checks that {@link CouchbaseOkHttpClient} presents a rotated client certificate on new connections,
 * using a real TLS server that requires client certificates.
 */
class ClientCertificateRotationTest {

  private static final String LOOPBACK_IP = "127.0.0.1";

  private static final HeldCertificate SERVER_CERT = new HeldCertificate.Builder()
    .addSubjectAlternativeName(TestHttpServer.HOST_NAME)
    .addSubjectAlternativeName(LOOPBACK_IP)
    .build();
  private static final HeldCertificate CERT_A = new HeldCertificate.Builder().commonName("client-a").build();
  private static final HeldCertificate CERT_B = new HeldCertificate.Builder().commonName("client-b").build();

  private final List<TestHttpServer> servers = new ArrayList<>();
  private final AtomicReference<KeyManagerFactory> currentClientCert = new AtomicReference<>();
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

  /**
   * Starts a server that requires a client certificate, and trusts both A and B.
   */
  private TestHttpServer startServer() throws IOException {
    HandshakeCertificates serverCerts = new HandshakeCertificates.Builder()
      .heldCertificate(SERVER_CERT)
      .addTrustedCertificate(CERT_A.certificate())
      .addTrustedCertificate(CERT_B.certificate())
      .build();
    TestHttpServer server = TestHttpServer.startHttps(serverCerts.sslContext(), true);
    servers.add(server);
    return server;
  }

  private CouchbaseOkHttpClient newClient(HeldCertificate initialClientCert) throws Exception {
    currentClientCert.set(keyManagerFactory(initialClientCert));
    return newClient(currentClientCert::get);
  }

  private CouchbaseOkHttpClient newClient(Supplier<KeyManagerFactory> clientCertSupplier) {
    client = new CouchbaseOkHttpClient(
      Duration.ofSeconds(5),
      IoConfig.create(),
      true, // native IO enabled, like the default environment
      SecurityConfig.builder()
        .enableTls(true)
        .trustCertificates(listOf(SERVER_CERT.certificate()))
        .build(),
      CertificateAuthenticator.fromKeyManagerFactory(clientCertSupplier),
      "test-user-agent"
    );
    return client;
  }

  private void rotateTo(HeldCertificate cert) throws Exception {
    currentClientCert.set(keyManagerFactory(cert));
  }

  private static void get(CouchbaseOkHttpClient http, TestHttpServer server) throws IOException {
    get(http, server, TestHttpServer.HOST_NAME);
  }

  private static void get(CouchbaseOkHttpClient http, TestHttpServer server, String host) throws IOException {
    server.enqueue("ok");
    HttpUrl url = new HttpUrl.Builder()
      .scheme("https")
      .host(host)
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

  private static X509Certificate presented(TestHttpServer server, int requestIndex) {
    return server.tlsInfo().get(requestIndex).clientCertificate;
  }

  @Test
  void presentsClientCertificate() throws Exception {
    CouchbaseOkHttpClient http = newClient(CERT_A);
    TestHttpServer server = startServer();

    get(http, server);

    assertEquals(CERT_A.certificate(), presented(server, 0));
  }

  @Test
  void newConnectionAfterRotationPresentsNewCertificate() throws Exception {
    CouchbaseOkHttpClient http = newClient(CERT_A);
    TestHttpServer server = startServer();
    get(http, server);

    rotateTo(CERT_B);
    http.getClientForTest().connectionPool().evictAll(); // as if the connection had idled out
    get(http, server);

    // The new connection presents B. In particular, it didn't resume the TLS session
    // established with A, which would skip presenting a certificate.
    assertEquals(CERT_A.certificate(), presented(server, 0));
    assertEquals(CERT_B.certificate(), presented(server, 1));
    assertNotEquals(server.tlsInfo().get(0).sessionId, server.tlsInfo().get(1).sessionId);
  }

  @Test
  void existingConnectionsKeepOldCertificateAfterRotation() throws Exception {
    // Like the Netty implementation: only new connections use the new certificate.
    CouchbaseOkHttpClient http = newClient(CERT_A);
    TestHttpServer node1 = startServer();
    TestHttpServer node2 = startServer();

    get(http, node1); // leaves an idle connection to node1, authenticated with A

    rotateTo(CERT_B);
    get(http, node2); // a new connection, so it uses B...

    get(http, node1); // ...but the existing connection to node1 is reused, still authenticated with A

    assertEquals(CERT_A.certificate(), presented(node1, 0));
    assertEquals(CERT_B.certificate(), presented(node2, 0));
    assertEquals(CERT_A.certificate(), presented(node1, 1));
    assertEquals(node1.tlsInfo().get(0).sessionId, node1.tlsInfo().get(1).sessionId, "should reuse the connection");
  }

  @Test
  void rotationByReinitializingTheSameFactory() throws Exception {
    CouchbaseOkHttpClient http = newClient(CERT_A);
    KeyManagerFactory shared = currentClientCert.get(); // the supplier keeps returning this instance
    TestHttpServer server = startServer();
    get(http, server);

    initialize(shared, CERT_B);
    http.getClientForTest().connectionPool().evictAll(); // as if the connection had idled out
    get(http, server);

    assertEquals(CERT_A.certificate(), presented(server, 0));
    assertEquals(CERT_B.certificate(), presented(server, 1));
  }

  @Test
  void keyManagerThatChangesInPlaceIsPickedUpByTheNextConnection() throws Exception {
    // The supplier keeps returning the same factory, whose key manager changes its certificate while
    // staying the same instance. Every new connection gets a new TLS context (so it can't resume a
    // session) and does a full handshake, which asks the key manager for its current certificate.
    InPlaceKeyManager keyManager = new InPlaceKeyManager(CERT_A);
    KeyManagerFactory factory = factoryWrapping(keyManager);
    CouchbaseOkHttpClient http = newClient(() -> factory);
    TestHttpServer server = startServer();
    get(http, server);

    keyManager.switchTo(CERT_B);
    http.getClientForTest().connectionPool().evictAll(); // as if the connection had idled out
    get(http, server);

    assertEquals(CERT_A.certificate(), presented(server, 0));
    assertEquals(CERT_B.certificate(), presented(server, 1));
  }

  // ---- Helpers ----

  static KeyManagerFactory keyManagerFactory(HeldCertificate cert) throws Exception {
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    initialize(kmf, cert);
    return kmf;
  }

  /**
   * (Re-)initializes the factory with the given certificate and its private key.
   */
  static void initialize(KeyManagerFactory kmf, HeldCertificate cert) throws Exception {
    char[] password = "password".toCharArray();
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry("client", cert.keyPair().getPrivate(), password, new Certificate[]{cert.certificate()});
    kmf.init(keyStore, password);
  }

  /**
   * Returns a new KeyManagerFactory whose only key manager is the given one.
   */
  private static KeyManagerFactory factoryWrapping(KeyManager keyManager) {
    KeyManagerFactorySpi spi = new KeyManagerFactorySpi() {
      @Override
      protected void engineInit(KeyStore ks, char[] password) {
      }

      @Override
      protected void engineInit(ManagerFactoryParameters spec) {
      }

      @Override
      protected KeyManager[] engineGetKeyManagers() {
        return new KeyManager[]{keyManager};
      }
    };
    return new KeyManagerFactory(spi, null, "test") {
    };
  }

  /**
   * A key manager that switches certificates while staying the same instance.
   */
  private static final class InPlaceKeyManager extends X509ExtendedKeyManager {
    private final AtomicReference<X509ExtendedKeyManager> delegate = new AtomicReference<>();

    InPlaceKeyManager(HeldCertificate cert) throws Exception {
      switchTo(cert);
    }

    void switchTo(HeldCertificate cert) throws Exception {
      delegate.set((X509ExtendedKeyManager) keyManagerFactory(cert).getKeyManagers()[0]);
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
      return delegate.get().chooseClientAlias(keyType, issuers, socket);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
      return delegate.get().chooseEngineClientAlias(keyType, issuers, engine);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
      return delegate.get().getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
      return delegate.get().getPrivateKey(alias);
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
      return delegate.get().getClientAliases(keyType, issuers);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
      return null;
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
      return null;
    }
  }

  private static final class KeyFileMissingException extends RuntimeException {
    KeyFileMissingException() {
      super("key file not found");
    }
  }

  @Test
  void connectionFailsWithCauseIfCertificateCantBeObtained() throws Exception {
    // The supplier fails. Like the Netty implementation, the connection fails.
    AtomicReference<KeyManagerFactory> available = new AtomicReference<>();
    CouchbaseOkHttpClient http = newClient(() -> {
      KeyManagerFactory kmf = available.get();
      if (kmf == null) {
        throw new KeyFileMissingException();
      }
      return kmf;
    });
    TestHttpServer server = startServer();

    // The connection fails, and the failure says why: the supplier's exception is in the cause chain,
    // instead of the server rejecting a handshake that has no certificate.
    //
    // Connect to the IP address, not "localhost". If a host name has several addresses (like
    // 127.0.0.1 and ::1), OkHttp throws the exception from the last address it tried, which might
    // be "connection refused" from an address the server isn't listening on.
    IOException e = assertThrows(IOException.class, () -> get(http, server, LOOPBACK_IP));
    assertTrue(hasCause(e, KeyFileMissingException.class), "expected the supplier's failure as a cause, but got: " + e);
    assertTrue(server.tlsInfo().isEmpty(), "the server shouldn't have received a request");

    // Works once the certificate becomes available.
    available.set(keyManagerFactory(CERT_A));
    get(http, server, LOOPBACK_IP);
    assertEquals(CERT_A.certificate(), presented(server, 0));
  }
}
