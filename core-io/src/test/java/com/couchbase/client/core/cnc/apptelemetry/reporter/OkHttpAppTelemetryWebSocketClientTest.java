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

package com.couchbase.client.core.cnc.apptelemetry.reporter;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.deps.io.netty.bootstrap.ServerBootstrap;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.MultiThreadIoEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.core.deps.io.netty.channel.nio.NioIoHandler;
import com.couchbase.client.core.deps.io.netty.channel.socket.SocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.socket.nio.NioServerSocketChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpServerCodec;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketFrame;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.service.CouchbaseOkHttpClient;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Connects {@link OkHttpAppTelemetryWebSocketClient} to a Netty WebSocket server,
 * which plays the part of the app telemetry server.
 */
class OkHttpAppTelemetryWebSocketClientTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String PATH = "/_appTelemetry";
  private static final String REPORT = "sdk_http_requests{agent=\"test\"} 1\n";

  private @Nullable CoreEnvironment env;
  private @Nullable CouchbaseOkHttpClient okHttpClient;
  private @Nullable EventLoopGroup eventLoopGroup;

  /**
   * What the client sent to the server.
   */
  private final AtomicReference<@Nullable String> authorizationHeader = new AtomicReference<>();
  private final BlockingQueue<byte[]> receivedMessages = new LinkedBlockingQueue<>();
  private final CompletableFuture<Void> serverChannelClosed = new CompletableFuture<>();

  @AfterEach
  void teardown() {
    try {
      if (okHttpClient != null) okHttpClient.close();
    } finally {
      try {
        if (eventLoopGroup != null) eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
      } finally {
        if (env != null) env.shutdown();
      }
    }
  }

  @Test
  void answersRequestsUntilServerCloses() throws Exception {
    URI uri = startServer(ch -> {
      ch.writeAndFlush(binary(0)); // GET_TELEMETRY
      ch.writeAndFlush(binary(42)); // unrecognized
      // The test closes the connection once both answers have arrived.
    });
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    AtomicBoolean connected = new AtomicBoolean();
    CompletableFuture<Void> result = connectInBackground(client, uri, () -> connected.set(true));

    assertEquals((char) 0 + REPORT, new String(nextMessage(), UTF_8));
    assertEquals("\u0001", new String(nextMessage(), UTF_8));
    assertTrue(connected.get());
    assertTrue(authorizationHeader.get().startsWith("Basic "));

    closeFromServer();
    result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // returns normally
  }

  @Test
  void sendsNoCredentialsToUserSpecifiedEndpoint() throws Exception {
    URI uri = startServer(ch -> ch.writeAndFlush(binary(0)));
    OkHttpAppTelemetryWebSocketClient client = newClient(uri);

    CompletableFuture<Void> result = connectInBackground(client, uri, () -> {
    });

    assertEquals((char) 0 + REPORT, new String(nextMessage(), UTF_8));
    assertNull(authorizationHeader.get());

    closeFromServer();
    result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  void returnsWhenServerDropsConnectionWithoutCloseFrame() throws Exception {
    URI uri = startServer(ch -> ch.writeAndFlush(binary(0)));
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    CompletableFuture<Void> result = connectInBackground(client, uri, () -> {
    });
    nextMessage(); // connected

    serverChannel.get().close();
    result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // returns normally
  }

  @Test
  void interruptClosesConnection() throws Exception {
    URI uri = startServer(ch -> ch.writeAndFlush(binary(0)));
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    CompletableFuture<Void> result = connectInBackground(client, uri, () -> {
    });
    nextMessage(); // connected

    workerThread.get().interrupt();

    Exception e = assertThrows(Exception.class, () -> result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    assertInstanceOf(InterruptedException.class, e.getCause());
    serverChannelClosed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  void connectionFailureThrows() throws Exception {
    int unusedPort;
    try (ServerSocket s = new ServerSocket(0)) {
      unusedPort = s.getLocalPort();
    }
    URI uri = URI.create("ws://127.0.0.1:" + unusedPort + PATH);
    OkHttpAppTelemetryWebSocketClient client = newClient(null);

    AtomicBoolean connected = new AtomicBoolean();
    CompletableFuture<Void> result = connectInBackground(client, uri, () -> connected.set(true));

    Exception e = assertThrows(Exception.class, () -> result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    assertInstanceOf(RuntimeException.class, e.getCause());
    assertFalse(connected.get());
  }

  private final AtomicReference<Thread> workerThread = new AtomicReference<>();
  private final AtomicReference<Channel> serverChannel = new AtomicReference<>();

  private CompletableFuture<Void> connectInBackground(AppTelemetryClient client, URI uri, Runnable onConnected) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    Thread t = new Thread(() -> {
      try {
        client.connectAndWaitForClose(uri, onConnected);
        result.complete(null);
      } catch (Throwable e) {
        result.completeExceptionally(e);
      }
    });
    workerThread.set(t);
    t.start();
    return result;
  }

  private byte[] nextMessage() throws InterruptedException {
    byte[] message = receivedMessages.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    assertNotNull(message, "timed out waiting for message from client");
    return message;
  }

  private void closeFromServer() {
    serverChannel.get().writeAndFlush(new CloseWebSocketFrame(1000, "bye"));
  }

  private static BinaryWebSocketFrame binary(int opcode) {
    return new BinaryWebSocketFrame(Unpooled.buffer(1).writeByte(opcode));
  }

  /**
   * @param userSpecifiedEndpoint if non-null, the environment's app telemetry endpoint
   */
  private OkHttpAppTelemetryWebSocketClient newClient(@Nullable URI userSpecifiedEndpoint) {
    env = CoreEnvironment.builder()
      .appTelemetryEndpoint(userSpecifiedEndpoint == null ? null : userSpecifiedEndpoint.toString())
      .build();

    okHttpClient = new CouchbaseOkHttpClient(
      Duration.ofSeconds(5),
      env.ioConfig(),
      env.ioEnvironment().nativeIoEnabled(),
      env.securityConfig(),
      PasswordAuthenticator.create("username", "password"),
      "test-user-agent"
    );

    Core core = mockCore(env);
    when(core.okHttpClient()).thenReturn(okHttpClient);

    AppTelemetryCollector collector = mock(AppTelemetryCollector.class);
    doAnswer(invocation -> {
      Consumer<? super CharSequence> sink = invocation.getArgument(0);
      sink.accept(REPORT);
      return null;
    }).when(collector).reportTo(any());

    return new OkHttpAppTelemetryWebSocketClient(core.context(), collector);
  }

  /**
   * Starts a WebSocket server, which calls the given callback once the handshake is complete.
   */
  private URI startServer(Consumer<Channel> onHandshakeComplete) {
    eventLoopGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

    Channel listener = new ServerBootstrap()
      .group(eventLoopGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          serverChannel.set(ch);
          ch.closeFuture().addListener(f -> serverChannelClosed.complete(null));
          ch.pipeline().addLast(
            new HttpServerCodec(),
            new HttpObjectAggregator(65536),
            new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder()
              .websocketPath(PATH)
              .allowExtensions(true)
              .handleCloseFrames(false) // so the server can start the closing handshake
              .sendCloseFrame(null) // so closing the channel doesn't send a close frame
              .build()),
            new SimpleChannelInboundHandler<WebSocketFrame>() {
              @Override
              public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
                  authorizationHeader.set(((WebSocketServerProtocolHandler.HandshakeComplete) evt).requestHeaders().get("Authorization"));
                  onHandshakeComplete.accept(ctx.channel());
                }
                super.userEventTriggered(ctx, evt);
              }

              @Override
              protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
                if (frame instanceof BinaryWebSocketFrame) {
                  receivedMessages.add(ByteBufUtil.getBytes(frame.content()));
                } else if (frame instanceof CloseWebSocketFrame) {
                  ctx.close(); // the client answered the server's close frame
                }
              }
            }
          );
        }
      })
      .bind(new InetSocketAddress("127.0.0.1", 0))
      .syncUninterruptibly()
      .channel();

    int port = ((InetSocketAddress) listener.localAddress()).getPort();
    return URI.create("ws://127.0.0.1:" + port + PATH);
  }
}
