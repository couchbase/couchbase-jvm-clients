/*
 * Copyright 2025 Couchbase, Inc.
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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.socket.SocketChannel;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpHeaders;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaders;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.WebSocketVersion;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import com.couchbase.client.core.deps.io.netty.handler.timeout.IdleStateHandler;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.SecurityException;
import com.couchbase.client.core.io.netty.SslHandlerFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.endpoint.BaseEndpoint.channelFrom;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

class AppTelemetryWebSocketClient {
  private static final Logger log = LoggerFactory.getLogger(AppTelemetryWebSocketClient.class);

  /**
   * Ping server if we haven't talked to it for this long.
   */
  private static final Duration pingInterval = Duration.ofMinutes(15);

  /**
   * Close channel if server sends no frames for this long.
   */
  private static final Duration idleTimeout = pingInterval
    .plus(Duration.ofMinutes(1)); // give the server this long to respond to ping


  private final AppTelemetryCollector collector;
  private final CoreContext coreContext;

  AppTelemetryWebSocketClient(
    CoreContext coreContext,
    AppTelemetryCollector collector
  ) {
    this.coreContext = requireNonNull(coreContext);
    this.collector = requireNonNull(collector);
  }

  /**
   * @throws InterruptedException if the thread was interrupted
   * @throws RuntimeException if the connection attempt failed for any other reason
   */
  void connectAndWaitForClose(URI remote, Runnable doOnSuccessfulConnection) throws InterruptedException {
    Channel channel = null;
    try {
      channel = newChannel(remote, doOnSuccessfulConnection);
      channel.closeFuture().sync();
    } finally {
      if (channel != null) channel.close();
    }
  }

  private static int getPort(URI webSocketUri) {
    int port = webSocketUri.getPort();
    if (port != -1) {
      return port;
    }

    switch (webSocketUri.getScheme()) {
      case "ws":
        return 80;
      case "wss":
        return 443;
      default:
        throw new IllegalArgumentException("Unsupported websocket scheme: " + webSocketUri.getScheme());
    }
  }

  private Channel newChannel(URI uri, Runnable doOnSuccessfulConnection) {
    String host = uri.getHost();
    int port = getPort(uri);

    HttpHeaders headers = new DefaultHttpHeaders();
    maybeAuthenticate(headers);

    AppTelemetryWebSocketHandler handler = new AppTelemetryWebSocketHandler(
      WebSocketClientHandshakerFactory.newHandshaker(
        uri,
        WebSocketVersion.V13,
        null,
        true,
        headers
      ),
      collector
    );

    EventLoopGroup group = coreContext.environment().ioEnvironment().managerEventLoopGroup().get();
    int connectTimeoutMillis = (int) coreContext.environment().timeoutConfig().connectTimeout().toMillis();

    Bootstrap b = new Bootstrap()
      .group(group)
      .channel(channelFrom(group))
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          ChannelPipeline p = ch.pipeline();

          maybeAddTlsHandler(ch, new HostAndPort(host, port));

          p.addLast(
            new HttpClientCodec(),
            new HttpObjectAggregator(8192),
            WebSocketClientCompressionHandler.INSTANCE,
            new IdleStateHandler(idleTimeout.toMillis(), pingInterval.toMillis(), 0, TimeUnit.MILLISECONDS),
            handler
          );
        }
      });

    ChannelFuture connectFuture = b.connect(host, port);

    connectFuture.addListener((ChannelFuture future) -> {
      if (future.isSuccess()) {
        log.info("App telemetry connection established for remote: {}", redactSystem(uri));
        handler.handshakeFuture().addListener(((ChannelFuture f) -> {
          if (f.isSuccess()) {
            log.info("WebSocket handshake successful for remote: {}", redactSystem(uri));
            doOnSuccessfulConnection.run();
          } else {
            log.warn("WebSocket handshake failed for remote: {}", redactSystem(uri));
            connectFuture.channel().close();
          }
        }));
      } else {
        log.warn("App telemetry connection failed for remote: {}", redactSystem(uri), future.cause());
      }
    });

    return connectFuture.channel();
  }

  /**
   * User-specified endpoints don't support authentication or TLS.
   */
  private boolean isUserSpecifiedEndpoint() {
    return coreContext.environment().appTelemetryEndpoint() != null;
  }

  private void maybeAuthenticate(HttpHeaders headers) {
    if (isUserSpecifiedEndpoint()) {
      return;
    }

    HttpRequest dummyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    coreContext.authenticator().authHttpRequest(ServiceType.MANAGER, dummyRequest);
    headers.add(dummyRequest.headers());
  }

  private void maybeAddTlsHandler(SocketChannel ch, HostAndPort remote) {
    if (isUserSpecifiedEndpoint()) {
      return;
    }

    SecurityConfig config = coreContext.environment().securityConfig();
    if (config.tlsEnabled()) {
      try {
        EndpointContext ctx = newEndpointContext(remote);
        ch.pipeline().addFirst(SslHandlerFactory.get(ch.alloc(), config, ctx));
      } catch (Exception e) {
        throw new SecurityException("Could not instantiate SSL Handler", e);
      }
    }
  }

  private EndpointContext newEndpointContext(HostAndPort remote) {
    return new EndpointContext(
      coreContext,
      remote,
      null,
      ServiceType.MANAGER,
      Optional.empty(),
      Optional.empty(),
      Optional.empty()
    );
  }

}
