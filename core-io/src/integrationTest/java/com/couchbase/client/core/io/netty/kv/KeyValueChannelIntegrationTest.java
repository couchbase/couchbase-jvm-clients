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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.endpoint.KeyValueEndpoint;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.NoopRequest;
import com.couchbase.client.core.msg.kv.NoopResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import com.couchbase.client.core.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.core.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.socket.SocketChannel;
import com.couchbase.client.core.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.core.cnc.SimpleEventBus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests the direct lower level communication of a full KV channel against
 * a real server socket.
 *
 * @since 2.0.0
 */
class KeyValueChannelIntegrationTest extends CoreIntegrationTest {

  private static CoreEnvironment env;
  private static EndpointContext endpointContext;
  private static NioEventLoopGroup eventLoopGroup;

  /**
   * Some tests raise warnings which are expected (i.e. auth failures), so we silence them
   * in the logs by using the simple event bus.
   */
  private static SimpleEventBus eventBus = new SimpleEventBus(true);

  @BeforeAll
  static void beforeAll() {
    TestNodeConfig node = config().nodes().get(0);
    env = environment().eventBus(eventBus).build();

    Core core = Core.create(env, authenticator(), seedNodes());
    endpointContext = new EndpointContext(
      core.context(),
      new HostAndPort(node.hostname(), node.ports().get(Services.KV)),
      null,
      ServiceType.KV,
      Optional.empty(),
      Optional.of(config().bucketname()),
      Optional.empty()
    );
    eventLoopGroup = new NioEventLoopGroup(1);
  }

  @AfterAll
  static void afterAll() {
    env.shutdown();
    eventLoopGroup.shutdownGracefully();
  }

  /**
   * This is the most simple kv test case one can do in a full-stack manner.
   *
   * <p>It connects to a kv socket, including all auth and bucket selection. It then
   * checks that the channel is opened properly and performs a NOOP and checks for a
   * successful result. Then it shuts everything down.</p>
   *
   * @throws Exception if waiting on the response fails.
   */
  @Test
  void connectNoopAndDisconnect() throws Exception {
    TestNodeConfig node = config().nodes().get(0);
    Bootstrap bootstrap = new Bootstrap()
      .remoteAddress(node.hostname(), node.ports().get(Services.KV))
      .group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          new KeyValueEndpoint.KeyValuePipelineInitializer(
            endpointContext,
            Optional.of(config().bucketname()),
            endpointContext.authenticator()
          ).init(null, ch.pipeline());
        }
      });

    Channel channel = bootstrap.connect().awaitUninterruptibly().channel();
    assertTrue(channel.isActive());
    assertTrue(channel.isOpen());

    NoopRequest request = new NoopRequest(Duration.ZERO, endpointContext, null, CollectionIdentifier.fromDefault(config().bucketname()));
    channel.writeAndFlush(request);
    NoopResponse response = request.response().get(1, TimeUnit.SECONDS);
    assertTrue(response.status().success());

    channel.close().awaitUninterruptibly();
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void failWithInvalidPasswordCredential() throws Exception {
    TestNodeConfig node = config().nodes().get(0);
    Bootstrap bootstrap = new Bootstrap()
      .remoteAddress(node.hostname(), node.ports().get(Services.KV))
      .group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          new KeyValueEndpoint.KeyValuePipelineInitializer(
            endpointContext,
            Optional.of(config().bucketname()),
            PasswordAuthenticator.create(config().adminUsername(), "djslkfsdfsoufhoshfoishgs")
          ).init(null, ch.pipeline());
        }
      });

    assertAuthenticationFailure(bootstrap, "Authentication Failure");
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.MOCKED)
  void failWithInvalidUsernameCredential() throws Exception {
    TestNodeConfig node = config().nodes().get(0);
    Bootstrap bootstrap = new Bootstrap()
      .remoteAddress(node.hostname(), node.ports().get(Services.KV))
      .group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          new KeyValueEndpoint.KeyValuePipelineInitializer(
            endpointContext,
            Optional.of(config().bucketname()),
            PasswordAuthenticator.create("vfwmf42343rew", config().adminPassword())
          ).init(null, ch.pipeline());
        }
      });

    assertAuthenticationFailure(bootstrap, "Authentication Failure");
  }

  @Test
  void failWithInvalidBucketCredential() throws Exception {
    String bucketName = "42eredwefrfe";

    TestNodeConfig node = config().nodes().get(0);
    Bootstrap bootstrap = new Bootstrap()
      .remoteAddress(node.hostname(), node.ports().get(Services.KV))
      .group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) {
          new KeyValueEndpoint.KeyValuePipelineInitializer(
            endpointContext,
            Optional.of(bucketName),
            endpointContext.authenticator()
          ).init(null, ch.pipeline());
        }
      });

    assertAuthenticationFailure(bootstrap, "Either the bucket with name \"" + bucketName + "\" is not present " +
      "or the user does not have the right privileges to access it");
  }


  /**
   * Helper method to assert authentication failure in different scenarios.
   */
  private void assertAuthenticationFailure(final Bootstrap bootstrap, final String msg) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    bootstrap.connect().addListener((ChannelFutureListener) future -> {
      try {
        assertFalse(future.isSuccess());
        Throwable ex = future.cause();
        assertTrue(ex instanceof AuthenticationFailureException);
        assertEquals(msg, ex.getMessage());
      } finally {
        latch.countDown();
      }
    });
    latch.await();
  }

}
