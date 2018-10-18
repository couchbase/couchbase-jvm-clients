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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.io.SelectBucketDisabledEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.utils.SimpleEventBus;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

  private CoreContext coreContext;
  private EmbeddedChannel channel;
  private SimpleEventBus simpleEventBus;

  @BeforeEach
  void setup() {
    channel = new EmbeddedChannel();
    simpleEventBus = new SimpleEventBus();
    CoreEnvironment env = mock(CoreEnvironment.class);
    IoEnvironment ioEnv = mock(IoEnvironment.class);
    when(env.eventBus()).thenReturn(simpleEventBus);
    when(env.ioEnvironment()).thenReturn(ioEnv);
    when(ioEnv.connectTimeout()).thenReturn(Duration.ofMillis(10));
    coreContext = new CoreContext(1, env);
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

    SelectBucketHandler handler = new SelectBucketHandler(coreContext, "bucket");
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

    SelectBucketHandler handler = new SelectBucketHandler(coreContext, "bucket");
    channel.pipeline().addLast(handler);

    final ChannelFuture connect = channel.connect(
      new InetSocketAddress("1.2.3.4", 1234)
    );
    channel.attr(ChannelAttributes.SERVER_FEATURE_KEY)
      .set(Collections.singletonList(ServerFeature.SELECT_BUCKET));
    channel.pipeline().fireChannelActive();

    Thread.sleep(timeout.toMillis() + 5);
    channel.runScheduledPendingTasks();

    assertTrue(connect.isDone());
    assertTrue(connect.cause() instanceof TimeoutException);
    assertEquals(
      "KV Select Bucket loading timed out after 10ms",
      connect.cause().getMessage()
    );
  }

  /**
   * If the SELECT_BUCKET has not been negotiated, the handler should remove itself
   * immediately.
   */
  @Test
  void completeImmediatelyIfNotNegotiated() {
    SelectBucketHandler handler = new SelectBucketHandler(coreContext, "bucket");
    channel.pipeline().addLast(handler);

    final ChannelFuture connect = channel.connect(
      new InetSocketAddress("1.2.3.4", 1234)
    );
    assertFalse(connect.isDone());
    assertNotNull(channel.pipeline().get(SelectBucketHandler.class));

    channel.pipeline().fireChannelActive();
    assertTrue(connect.isSuccess());
    assertNull(channel.pipeline().get(SelectBucketHandler.class));

    assertEquals(1, simpleEventBus.publishedEvents().size());
    SelectBucketDisabledEvent event =
      (SelectBucketDisabledEvent) simpleEventBus.publishedEvents().get(0);

    assertEquals(Event.Severity.DEBUG, event.severity());
    assertEquals(
      "Select Bucket disabled/not negotiated during HELLO for bucket \"bucket\"",
      event.description()
    );
    assertEquals("bucket", event.bucket());
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
