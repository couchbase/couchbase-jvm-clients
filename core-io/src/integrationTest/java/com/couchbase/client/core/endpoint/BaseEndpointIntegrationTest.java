/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.endpoint.UnexpectedEndpointDisconnectedEvent;
import com.couchbase.client.core.deps.io.netty.bootstrap.ServerBootstrap;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.core.deps.io.netty.channel.DefaultEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.core.deps.io.netty.channel.local.LocalAddress;
import com.couchbase.client.core.deps.io.netty.channel.local.LocalServerChannel;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CoreIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the endpoint in a generic fashion that is not tied to a specific target implementation.
 */
class BaseEndpointIntegrationTest extends CoreIntegrationTest {

  private CoreEnvironment env;
  private DefaultEventLoopGroup eventLoopGroup;
  private SimpleEventBus eventBus;

  @BeforeEach
  void beforeEach() {
    eventBus = new SimpleEventBus(true);
    env = environment().eventBus(eventBus).build();
    eventLoopGroup = new DefaultEventLoopGroup();
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
    eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
  }

  /**
   * When the underlying channel closes, the endpoint must continue to reconnect until being instructed
   * to stop with an explicit disconnect command.
   */
  @Test
  void mustReconnectWhenChannelCloses() {
    LocalServerController localServerController = startLocalServer(eventLoopGroup);

    ServiceContext serviceContext = new ServiceContext(
      new CoreContext(mockCore(env), 1, env, authenticator()),
      "127.0.0.1",
      1234,
      ServiceType.KV,
      Optional.empty()
    );

    BaseEndpoint endpoint = new BaseEndpoint("127.0.0.1", 1234, eventLoopGroup,
      serviceContext, CircuitBreakerConfig.enabled(false).build(), ServiceType.QUERY, false) {

      @Override
      protected PipelineInitializer pipelineInitializer() {
        return  (endpoint, pipeline) -> { };
      }

      @Override
      protected SocketAddress remoteAddress() {
        return new LocalAddress("server");
      }
    };

    List<EndpointState> transitions = Collections.synchronizedList(new ArrayList<>());
    endpoint.states().subscribe(transitions::add);

    assertEquals(0, localServerController.connectAttempts.get());
    assertNull(localServerController.channel.get());
    endpoint.connect();

    waitUntilCondition(() -> endpoint.state() == EndpointState.CONNECTED);
    waitUntilCondition(() -> localServerController.connectAttempts.get() == 1);

    assertNotNull(localServerController.channel.get());


    localServerController.channel.get().close().awaitUninterruptibly();

    List<EndpointState> expectedTransitions = Arrays.asList(
      EndpointState.DISCONNECTED, // initial state
      EndpointState.CONNECTING, // initial connect attempt
      EndpointState.CONNECTED, // properly connected the first time
      EndpointState.DISCONNECTED, // disconnected when we kill the channel from the server side
      EndpointState.CONNECTING, // endpoint should be reconnecting now
      EndpointState.CONNECTED // finally, we are able to reconnect completely
    );

    waitUntilCondition(() -> transitions.size() == expectedTransitions.size());

    assertEquals(expectedTransitions, transitions);
    waitUntilCondition(() -> localServerController.connectAttempts.get() >= 2);

    endpoint.disconnect();
    waitUntilCondition(() -> endpoint.state() == EndpointState.DISCONNECTED);

    boolean hasDisconnectEvent = false;
    for (Event event : eventBus.publishedEvents()) {
      if (event instanceof UnexpectedEndpointDisconnectedEvent) {
        hasDisconnectEvent = true;
        break;
      }
    }
    assertTrue(hasDisconnectEvent);
  }

  private LocalServerController startLocalServer(final DefaultEventLoopGroup eventLoopGroup) {
    final LocalServerController localServerController = new LocalServerController();

    ServerBootstrap bootstrap = new ServerBootstrap()
      .group(eventLoopGroup)
      .localAddress(new LocalAddress("server"))
      .childHandler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) {
          ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) { }


            @Override
            public void channelActive(ChannelHandlerContext ctx) {
              localServerController.channel.set(ctx.channel());
              localServerController.connectAttempts.incrementAndGet();
              ctx.fireChannelActive();
            }
          });
        }
      })
      .channel(LocalServerChannel.class);

    bootstrap.bind().awaitUninterruptibly();
    return localServerController;
  }

  /**
   * Simple server controller to handle back and forth between client and server instance.
   */
  static class LocalServerController {
    final AtomicInteger connectAttempts = new AtomicInteger();
    final AtomicReference<Channel> channel = new AtomicReference<>();
  }

}
