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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
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
import com.couchbase.client.core.error.AuthenticationFailureException;
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

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    CoreContext coreContext = new CoreContext(mockCore(), 1, env, mock(Authenticator.class));
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
   * The select bucket request and the bucket config request are sent together, in that order.
   */
  @Test
  void encodeAndSendSelectBucketRequest() {
    activate();

    ByteBuf selectBucket = channel.readOutbound();
    ByteBuf getConfig = channel.readOutbound();
    try {
      assertEquals(MemcacheProtocol.Opcode.SELECT_BUCKET.opcode(), MemcacheProtocol.opcode(selectBucket));
      assertEquals("bucket", MemcacheProtocol.keyAsString(selectBucket));
      assertEquals(MemcacheProtocol.Opcode.GET_CONFIG.opcode(), MemcacheProtocol.opcode(getConfig));
      assertNotEquals(MemcacheProtocol.opaque(selectBucket), MemcacheProtocol.opaque(getConfig));
    } finally {
      selectBucket.release();
      getConfig.release();
    }
  }

  /**
   * The connect completes only once both responses arrived, and the bucket UUID is stored on the channel.
   */
  @Test
  void completeAfterSuccessfulResponse() {
    ChannelFuture connect = activate();
    int[] opaques = readOpaques();

    channel.writeInbound(response(MemcacheProtocol.Opcode.SELECT_BUCKET, SUCCESS, opaques[0], ""));
    assertFalse(connect.isDone(), "Should wait for the bucket config before completing");

    channel.writeInbound(response(MemcacheProtocol.Opcode.GET_CONFIG, SUCCESS, opaques[1],
      "{\"rev\":1,\"name\":\"bucket\",\"uuid\":\"abc123\"}"));
    assertTrue(connect.isSuccess());
    assertEquals("abc123", channel.attr(ChannelAttributes.BUCKET_UUID_KEY).get());
  }

  /**
   * If the server can't provide the config, the connection still works, without a known bucket UUID.
   */
  @Test
  void completeWithoutUuidIfConfigUnavailable() {
    ChannelFuture connect = activate();
    int[] opaques = readOpaques();

    channel.writeInbound(response(MemcacheProtocol.Opcode.GET_CONFIG, MemcacheProtocol.Status.NOT_FOUND.status(), opaques[1], ""));
    channel.writeInbound(response(MemcacheProtocol.Opcode.SELECT_BUCKET, SUCCESS, opaques[0], ""));

    assertTrue(connect.isSuccess());
    assertNull(channel.attr(ChannelAttributes.BUCKET_UUID_KEY).get());
  }

  /**
   * Since we cannot proceed without selecting a bucket properly, the connect
   * attempt needs to be failed in all non-success cases.
   */
  @Test
  void failConnectForEveryOtherResponse() {
    ChannelFuture connect = activate();
    int[] opaques = readOpaques();

    channel.writeInbound(response(MemcacheProtocol.Opcode.SELECT_BUCKET, MemcacheProtocol.Status.ACCESS_ERROR.status(), opaques[0], ""));
    channel.writeInbound(response(MemcacheProtocol.Opcode.GET_CONFIG, SUCCESS, opaques[1], "{\"uuid\":\"abc123\"}"));

    assertTrue(connect.isDone());
    assertInstanceOf(AuthenticationFailureException.class, connect.cause());
  }

  private static final short SUCCESS = MemcacheProtocol.Status.SUCCESS.status();

  private ChannelFuture activate() {
    channel.pipeline().addLast(new SelectBucketHandler(endpointContext, "bucket"));
    ChannelFuture connect = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    channel.pipeline().fireChannelActive();
    return connect;
  }

  /**
   * Reads the two handshake requests, and returns their opaques: select bucket first, then get config.
   */
  private int[] readOpaques() {
    ByteBuf selectBucket = channel.readOutbound();
    ByteBuf getConfig = channel.readOutbound();
    try {
      return new int[]{MemcacheProtocol.opaque(selectBucket), MemcacheProtocol.opaque(getConfig)};
    } finally {
      selectBucket.release();
      getConfig.release();
    }
  }

  private static ByteBuf response(MemcacheProtocol.Opcode opcode, short status, int opaque, String body) {
    return MemcacheProtocol.response(
      UnpooledByteBufAllocator.DEFAULT,
      opcode,
      (byte) 0,
      status,
      opaque,
      0,
      Unpooled.EMPTY_BUFFER,
      Unpooled.EMPTY_BUFFER,
      Unpooled.copiedBuffer(body, UTF_8)
    );
  }

}
