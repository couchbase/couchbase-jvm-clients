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
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link SelectBucketHandler}.
 *
 * @since 2.0.0
 */
class SelectBucketHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private EndpointContext endpointContext;
  private EmbeddedChannel channel;

  @BeforeEach
  void setup() {
    channel = new EmbeddedChannel();
    SimpleEventBus simpleEventBus = new SimpleEventBus(true);
    CoreEnvironment env = mock(CoreEnvironment.class);
    TimeoutConfig timeoutConfig = mock(TimeoutConfig.class);
    when(env.eventBus()).thenReturn(simpleEventBus);
    when(env.timeoutConfig()).thenReturn(timeoutConfig);
    when(timeoutConfig.connectTimeout()).thenReturn(Duration.ofMillis(10));
    CoreContext coreContext = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    endpointContext = new EndpointContext(coreContext, new HostAndPort("127.0.0.1", 1234),
      null, ServiceType.KV, Optional.empty(), Optional.empty(), Optional.empty());
  }

  @AfterEach
  void teardown() {
    channel.finishAndReleaseAll();
  }

  /**
   * This test verifies that if a downstream promise fails that the error
   * is propagated through the captured promise.
   */
  @Test
  void propagateConnectFailureFromDownstream() {
    final Exception connectException = new Exception("I failed");
    ChannelDuplexHandler failingHandler = new ChannelDuplexHandler() {
      @Override
      public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                          final SocketAddress localAddress, final ChannelPromise promise) {
        promise.setFailure(connectException);
      }
    };

    SelectBucketHandler handler = new SelectBucketHandler(endpointContext, "bucket");
    channel.pipeline().addLast(failingHandler).addLast(handler);

    ChannelFuture connect = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertEquals(connectException, connect.awaitUninterruptibly().cause());
  }

  /**
   * This test makes sure that the timer fires if the connect future is not completed
   * otherwise.
   */
  @Test
  void failConnectIfPromiseTimesOut() throws Exception {
    final Duration timeout = Duration.ofMillis(10);

    SelectBucketHandler handler = new SelectBucketHandler(endpointContext, "bucket");
    channel.pipeline().addLast(handler);

    final ChannelFuture connect = channel.connect(
      new InetSocketAddress("1.2.3.4", 1234)
    );
    channel.attr(ChannelAttributes.SERVER_FEATURE_KEY)
      .set(EnumSet.of(ServerFeature.SELECT_BUCKET));
    channel.pipeline().fireChannelActive();

    Thread.sleep(timeout.toMillis() + 5);
    channel.runScheduledPendingTasks();

    assertTrue(connect.isDone());
    assertInstanceOf(TimeoutException.class, connect.cause());
    assertEquals(
      "KV Select Bucket loading timed out after 10ms",
      connect.cause().getMessage()
    );
  }

  /**
   * Check if the properly formatted select bucket request is sent over the wire.
   */
  @Test
  void encodeAndSendSelectBucketRequest() {

  }

  /**
   * Obviously we need to make sure that if a good response arrives we keep
   * moving on through the connect pipeline.
   */
  @Test
  void completeAfterSuccessfulResponse() {

  }

  /**
   * Since we cannot proceed without selecting a bucket properly, the connect
   * attempt needs to be failed in all non-success cases.
   */
  @Test
  void failConnectForEveryOtherResponse() {

  }

}
