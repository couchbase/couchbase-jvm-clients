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
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.core.service.CouchbaseOkHttpClient.KEEP_ALIVE_FOREVER;
import static com.couchbase.client.core.service.CouchbaseOkHttpClient.keepAlive;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

class CouchbaseOkHttpClientTest {

  @Test
  void usesHttp11Only() {
    // Like the Netty implementation. HTTP/2 would multiplex requests over one connection,
    // which the per-node in-flight limits don't account for.
    try (CouchbaseOkHttpClient client = new CouchbaseOkHttpClient(
      Duration.ofSeconds(5),
      IoConfig.create(),
      true, // native IO enabled, like the default environment
      SecurityConfig.builder().build(),
      PasswordAuthenticator.create("username", "password"),
      "test-user-agent"
    )) {
      assertEquals(singletonList(Protocol.HTTP_1_1), client.getClientForTest().protocols());
    }
  }

  @Test
  void leavesRetriesToTheSdk() {
    // Like the Netty implementation: the SDK's retry orchestrator decides whether to retry a failed request.
    try (CouchbaseOkHttpClient client = newClient()) {
      assertFalse(client.getClientForTest().retryOnConnectionFailure());
    }
  }

  @Test
  void keepAliveIsTheIdleConnectionTimeout() {
    assertEquals(Duration.ofSeconds(1), keepAlive(IoConfig.DEFAULT_IDLE_HTTP_CONNECTION_TIMEOUT));
    assertEquals(Duration.ofSeconds(60), keepAlive(Duration.ofSeconds(60)));
  }

  @Test
  void zeroIdleConnectionTimeoutMeansNeverCloseIdleConnections() {
    // Like the Netty endpoints, which don't check for idle connections then.
    assertEquals(KEEP_ALIVE_FOREVER, keepAlive(Duration.ZERO));
    assertEquals(KEEP_ALIVE_FOREVER, keepAlive(Duration.ofSeconds(-1)));
  }

  @Test
  void subMillisecondKeepAliveIsRoundedUp() {
    // OkHttp works in milliseconds, and would round this down to zero, which it rejects.
    assertEquals(Duration.ofMillis(1), keepAlive(Duration.ofNanos(500)));
  }

  private static CouchbaseOkHttpClient newClient() {
    return new CouchbaseOkHttpClient(
      Duration.ofSeconds(5),
      IoConfig.create(),
      true, // native IO enabled, like the default environment
      SecurityConfig.builder().build(),
      PasswordAuthenticator.create("username", "password"),
      "test-user-agent"
    );
  }

  private static okhttp3.Response get(OkHttpClient client, TestHttpServer server) throws Exception {
    return client.newCall(CouchbaseOkHttpClient.newRequest(
      new okhttp3.Request.Builder().url("http://" + TestHttpServer.HOST_NAME + ":" + server.port() + "/"),
      mock(RequestContext.class),
      Duration.ofSeconds(5)
    )).execute();
  }

  @Test
  void ignoresTheJvmProxySettings() throws Exception {
    // Like the Netty implementation. A proxy that refuses connections would fail the request.
    ProxySelector original = ProxySelector.getDefault();
    try (ServerSocket closed = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      Proxy unusable = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(InetAddress.getLoopbackAddress(), closed.getLocalPort()));
      closed.close();
      ProxySelector.setDefault(new ProxySelector() {
        @Override
        public List<Proxy> select(URI uri) {
          return singletonList(unusable);
        }

        @Override
        public void connectFailed(URI uri, SocketAddress address, IOException e) {
        }
      });

      try (TestHttpServer server = TestHttpServer.startHttp(); CouchbaseOkHttpClient client = newClient()) {
        assertEquals(Proxy.NO_PROXY, client.getClientForTest().proxy());
        server.enqueue("hello");
        try (okhttp3.Response response = get(client.getClientForTest(), server)) {
          assertEquals("hello", response.body().string());
        }
      }
    } finally {
      ProxySelector.setDefault(original);
    }
  }

  @Test
  void doesNotAskForCompressedResponses() throws Exception {
    // Like the Netty implementation. Otherwise OkHttp would send "Accept-Encoding: gzip".
    try (TestHttpServer server = TestHttpServer.startHttp(); CouchbaseOkHttpClient client = newClient()) {
      List<String> acceptEncodings = new ArrayList<>();
      OkHttpClient recordingClient = client.getClientForTest().newBuilder()
        .addNetworkInterceptor(chain -> { // sees the request as sent
          acceptEncodings.add(chain.request().header("Accept-Encoding"));
          return chain.proceed(chain.request());
        })
        .build();

      server.enqueue("hello");
      try (okhttp3.Response response = get(recordingClient, server)) {
        assertEquals("hello", response.body().string());
      }
      assertEquals(singletonList("identity"), acceptEncodings);
    }
  }
}
