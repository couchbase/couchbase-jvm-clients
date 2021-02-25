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

package com.couchbase.client.test.caves;

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CavesControlServer {

  private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();

  private volatile Channel channel;

  private volatile CavesControlServerHandler cavesControlServerHandler;
  private volatile SocketChannel childChannel;

  public CavesControlServer() {

  }

  public void start() throws Exception {

    cavesControlServerHandler = new CavesControlServerHandler();

    ServerBootstrap bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class)
      .handler(new LoggingHandler(LogLevel.DEBUG))
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          childChannel = ch;
          ch.pipeline()
            .addLast(new LoggingHandler(LogLevel.DEBUG))
            .addLast(cavesControlServerHandler);
        }
      });

    ChannelFuture f = bootstrap.bind(0).sync();
    channel = f.channel();
  }

  public void stop() throws Exception {
    if (channel != null) {
      channel.closeFuture().sync();
    }
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  public int port() {
    return ((InetSocketAddress) channel.localAddress()).getPort();
  }

  public CompletableFuture<Void> receivedHello() {
    return cavesControlServerHandler.receivedHello();
  }

  private CompletableFuture<CavesResponse> sendRequest(final Map<String, Object> payload) {
    CavesRequest request = new CavesRequest(payload);
    childChannel.writeAndFlush(request);
    return request.response();
  }

  public String startTesting(String runId, String clientName) throws Exception {
    Map<String, Object> payload = new HashMap<>();
    payload.put("type", "starttesting");
    payload.put("run", runId);
    payload.put("client", clientName);

    CavesResponse response = sendRequest(payload).get(10, TimeUnit.SECONDS);
    return (String) response.payload().get("connstr");
  }

  public Map<String, Object> endTesting(String runId) throws Exception {
    Map<String, Object> payload = new HashMap<>();
    payload.put("type", "endtesting");
    payload.put("run", runId);

    CavesResponse response = sendRequest(payload).get(10, TimeUnit.SECONDS);
    return (Map<String, Object>) response.payload().get("report");
  }

  public Map<String, Object> startTest(String runId, String testName) throws Exception {
    Map<String, Object> payload = new HashMap<>();
    payload.put("type", "starttest");
    payload.put("run", runId);
    payload.put("test", testName);

    CavesResponse response = sendRequest(payload).get(10, TimeUnit.SECONDS);
    return response.payload();
  }

  public Map<String, Object> endTest(String runId) throws Exception {
    Map<String, Object> payload = new HashMap<>();
    payload.put("type", "endtest");
    payload.put("run", runId);

    CavesResponse response = sendRequest(payload).get(10, TimeUnit.SECONDS);
    return response.payload();
  }

}
