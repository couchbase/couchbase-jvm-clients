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
import com.couchbase.client.core.util.StorageSize;
import com.couchbase.client.socketoptions.ExtendedSocket;
import com.couchbase.client.socketoptions.LinuxSocketOptions;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

class CouchbaseSocketFactoryTest {
  private ServerSocket server;

  @BeforeEach
  void startServer() throws Exception {
    // Connections complete in the kernel's backlog, so nothing needs to accept them.
    server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
  }

  @AfterEach
  void stopServer() throws Exception {
    server.close();
  }

  private Socket connect(IoConfig ioConfig, boolean nativeIoEnabled) throws Exception {
    Socket socket = new CouchbaseSocketFactory(ioConfig, nativeIoEnabled).createSocket();
    socket.connect(server.getLocalSocketAddress());
    return socket;
  }

  @Test
  void setsConfiguredOptionsOnceConnected() throws Exception {
    IoConfig ioConfig = IoConfig.builder()
      .tcpKeepAliveTime(Duration.ofSeconds(30))
      .tcpKeepAliveInterval(Duration.ofMillis(4500)) // rounded up to whole seconds, like the Netty endpoints
      .tcpKeepAliveCount(3)
      .tcpUserTimeout(Duration.ofSeconds(12))
      .build();

    try (Socket socket = connect(ioConfig, true)) {
      assertTrue(socket.getTcpNoDelay());
      assertTrue(socket.getKeepAlive());

      if (LinuxSocketOptions.isAvailable()) {
        LinuxSocketOptions linuxOptions = ((ExtendedSocket) socket).linuxOptions().orElseThrow(AssertionError::new);
        assertEquals(Duration.ofSeconds(12), linuxOptions.getTcpUserTimeout());
      } else {
        assertFalse(socket instanceof ExtendedSocket);
      }

      assumeExtendedOptionsSupported(socket);
      assertEquals(30, getExtendedOption(socket, "TCP_KEEPIDLE"));
      assertEquals(5, getExtendedOption(socket, "TCP_KEEPINTERVAL"));
      assertEquals(3, getExtendedOption(socket, "TCP_KEEPCOUNT"));
    }
  }

  @Test
  void zeroMeansOsDefault() throws Exception {
    IoConfig ioConfig = IoConfig.builder()
      .tcpKeepAliveTime(Duration.ZERO)
      .tcpKeepAliveInterval(Duration.ZERO)
      .tcpKeepAliveCount(0)
      .tcpUserTimeout(Duration.ZERO)
      .build();

    try (Socket socket = connect(ioConfig, true); Socket plain = new Socket()) {
      plain.connect(server.getLocalSocketAddress());
      assertTrue(socket.getKeepAlive());
      assertFalse(socket instanceof ExtendedSocket, "shouldn't load the native library just to leave TCP_USER_TIMEOUT alone");
      assumeExtendedOptionsSupported(socket);
      for (String option : new String[]{"TCP_KEEPIDLE", "TCP_KEEPINTERVAL", "TCP_KEEPCOUNT"}) {
        assertEquals(getExtendedOption(plain, option), getExtendedOption(socket, option), option);
      }
    }
  }

  @Test
  void setsConfiguredBufferSizesBeforeConnecting() throws Exception {
    int send = 96 * 1024;
    int receive = 160 * 1024;
    CouchbaseSocketFactory factory = new CouchbaseSocketFactory(IoConfig.builder()
      .sendBuffer(StorageSize.ofBytes(send))
      .receiveBuffer(StorageSize.ofBytes(receive))
      .build(), true);
    try (Socket socket = factory.createSocket()) {
      // Before connecting: the receive buffer size affects the TCP window scale, which is agreed when connecting.
      // The OS may adjust them: Linux doubles them.
      assertTrue(socket.getSendBufferSize() == send || socket.getSendBufferSize() == 2 * send, "SO_SNDBUF=" + socket.getSendBufferSize());
      assertTrue(socket.getReceiveBufferSize() == receive || socket.getReceiveBufferSize() == 2 * receive, "SO_RCVBUF=" + socket.getReceiveBufferSize());
    }
  }

  @Test
  void keepAlivesDisabled() throws Exception {
    try (Socket socket = connect(IoConfig.builder().enableTcpKeepAlives(false).build(), true)) {
      assertFalse(socket.getKeepAlive());
      assertTrue(socket.getTcpNoDelay(), "should still set TCP_NODELAY");
    }
  }

  @Test
  void nativeIoDisabledMeansNoNativeLibrary() throws Exception {
    try (Socket socket = connect(IoConfig.builder().tcpUserTimeout(Duration.ofSeconds(20)).build(), false)) {
      assertFalse(socket instanceof ExtendedSocket);
      assertTrue(socket.getTcpNoDelay());
      assertTrue(socket.getKeepAlive());
    }
  }

  @Test
  void connectingOverloadsSetTheOptions() throws Exception {
    CouchbaseSocketFactory factory = new CouchbaseSocketFactory(IoConfig.create(), true);
    try (Socket socket = factory.createSocket(server.getInetAddress(), server.getLocalPort())) {
      assertTrue(socket.isConnected());
      assertTrue(socket.getTcpNoDelay());
      assertTrue(socket.getKeepAlive());
    }
  }

  @Test
  void clientConnectionsUseTheOptions() throws Exception {
    try (
      TestHttpServer httpServer = TestHttpServer.startHttp();
      CouchbaseOkHttpClient client = new CouchbaseOkHttpClient(
        Duration.ofSeconds(5),
        IoConfig.create(),
        true, // native IO enabled, like the default environment
        SecurityConfig.builder().build(),
        PasswordAuthenticator.create("username", "password"),
        "test-user-agent"
      )
    ) {
      List<Socket> sockets = new ArrayList<>();
      OkHttpClient recordingClient = client.getClientForTest().newBuilder()
        .addNetworkInterceptor(chain -> {
          sockets.add(chain.connection().socket());
          return chain.proceed(chain.request());
        })
        .build();

      httpServer.enqueue("hello");
      try (Response response = recordingClient.newCall(CouchbaseOkHttpClient.newRequest(
        new okhttp3.Request.Builder().url("http://" + TestHttpServer.HOST_NAME + ":" + httpServer.port() + "/"),
        mock(RequestContext.class),
        Duration.ofSeconds(5)
      )).execute()) {
        assertEquals("hello", response.body().string());
        assertTrue(sockets.get(0).getKeepAlive());
        assertTrue(sockets.get(0).getTcpNoDelay());
      }
    }
  }

  // The tests compile for Java 8, so they use the extended socket options reflectively, like the factory does:
  // with Socket's methods on Java 9+, or jdk.net.Sockets on Java 8.

  private static void assumeExtendedOptionsSupported(Socket socket) throws Exception {
    SocketOption<?> option = extendedOption("TCP_KEEPIDLE");
    assumeTrue(option != null, "this JDK doesn't have the extended keepalive options");
    Set<?> supported = isJava8()
      ? (Set<?>) Class.forName("jdk.net.Sockets").getMethod("supportedOptions", Class.class).invoke(null, Socket.class)
      : (Set<?>) Socket.class.getMethod("supportedOptions").invoke(socket);
    assumeTrue(supported.contains(option), "this platform doesn't support the extended keepalive options");
  }

  private static Object getExtendedOption(Socket socket, String name) throws Exception {
    return isJava8()
      ? Class.forName("jdk.net.Sockets").getMethod("getOption", Socket.class, SocketOption.class).invoke(null, socket, extendedOption(name))
      : Socket.class.getMethod("getOption", SocketOption.class).invoke(socket, extendedOption(name));
  }

  private static boolean isJava8() {
    try {
      Socket.class.getMethod("getOption", SocketOption.class);
      return false;
    } catch (NoSuchMethodException e) {
      return true;
    }
  }

  private static @Nullable SocketOption<?> extendedOption(String name) {
    try {
      return (SocketOption<?>) Class.forName("jdk.net.ExtendedSocketOptions").getField(name).get(null);
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }
}
