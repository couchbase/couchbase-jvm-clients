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
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.io.ErrorMapLoadedEvent;
import com.couchbase.client.core.cnc.events.io.ErrorMapLoadingFailedEvent;
import com.couchbase.client.core.cnc.events.io.ErrorMapUndecodableEvent;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.util.SimpleEventBus;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.verifyRequest;
import static com.couchbase.client.test.Util.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link ErrorMapLoadingHandler}.
 */
class ErrorMapLoadingHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private EndpointContext endpointContext;
  private EmbeddedChannel channel;
  private SimpleEventBus simpleEventBus;

  @BeforeEach
  void setup() {
    channel = new EmbeddedChannel();
    simpleEventBus = new SimpleEventBus(true);
    CoreEnvironment env = mock(CoreEnvironment.class);
    TimeoutConfig timeoutConfig = mock(TimeoutConfig.class);
    when(env.eventBus()).thenReturn(simpleEventBus);
    when(env.timeoutConfig()).thenReturn(timeoutConfig);
    when(timeoutConfig.connectTimeout()).thenReturn(Duration.ofMillis(1000));
    CoreContext coreContext = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    endpointContext = new EndpointContext(coreContext, "127.0.0.1", 1234,
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

    ErrorMapLoadingHandler handler = new ErrorMapLoadingHandler(endpointContext);
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
    channel = new EmbeddedChannel();
    simpleEventBus = new SimpleEventBus(true);
    CoreEnvironment env = mock(CoreEnvironment.class);
    TimeoutConfig timeoutConfig = mock(TimeoutConfig.class);
    when(env.eventBus()).thenReturn(simpleEventBus);
    when(env.timeoutConfig()).thenReturn(timeoutConfig);
    when(timeoutConfig.connectTimeout()).thenReturn(Duration.ofMillis(100));
    CoreContext coreContext = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    EndpointContext endpointContext = new EndpointContext(coreContext,
      "127.0.0.1", 1234, null, ServiceType.KV,
      Optional.empty(), Optional.empty(), Optional.empty());

    ErrorMapLoadingHandler handler = new ErrorMapLoadingHandler(endpointContext);

    channel.pipeline().addLast(handler);

    final ChannelFuture connect = channel.connect(
      new InetSocketAddress("1.2.3.4", 1234)
    );
    channel.pipeline().fireChannelActive();

    Thread.sleep(Duration.ofMillis(100).toMillis() + 5);
    channel.runScheduledPendingTasks();

    assertTrue(connect.isDone());
    assertTrue(connect.cause() instanceof TimeoutException);
    assertEquals("KV Error Map loading timed out after 100ms", connect.cause().getMessage());
  }

  /**
   * This test verifies that the client sends the proper KV command to the server
   * when requesting the KV error map.
   */
  @Test
  void encodeAndSendErrorMapRequest() {
    ErrorMapLoadingHandler handler = new ErrorMapLoadingHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(ErrorMapLoadingHandler.class));
    channel.connect(new InetSocketAddress("1.2.3.4", 1234));

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.ERROR_MAP.opcode(), false, false, true);

    // sanity check the body payload
    assertTrue(ProtocolVerifier.body(writtenRequest).isPresent());
    ByteBuf body = ProtocolVerifier.body(writtenRequest).get();
    assertEquals(2, body.readableBytes());
    assertEquals((short) 1, body.readShort()); // we are checking for version 1 here

    ReferenceCountUtil.release(writtenRequest);
  }

  /**
   * Verify that when a good error map comes back, we parse and return it properly.
   */
  @Test
  void decodeSuccessfulErrorMap() {
    ErrorMapLoadingHandler handler = new ErrorMapLoadingHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(ErrorMapLoadingHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.ERROR_MAP.opcode(), false, false, true);
    assertNotNull(channel.pipeline().get(ErrorMapLoadingHandler.class));
    ReferenceCountUtil.release(writtenRequest);

    ByteBuf response = decodeHexDump(readResource(
      "success_errormap_response.txt",
      ErrorMapLoadingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    assertEquals(1, simpleEventBus.publishedEvents().size());
    ErrorMapLoadedEvent event =
      (ErrorMapLoadedEvent) simpleEventBus.publishedEvents().get(0);

    assertEquals(Event.Severity.DEBUG, event.severity());
    Optional<ErrorMap> maybeMap = event.errorMap();
    assertTrue(maybeMap.isPresent());
    assertNotNull(maybeMap.get());

    ErrorMap errorMap = channel.attr(ChannelAttributes.ERROR_MAP_KEY).get();
    assertEquals(errorMap, maybeMap.get());
  }

  /**
   * Make sure that when the server returns a non-successful response we still handle
   * it and not crash.
   */
  @Test
  void decodeUnsuccessfulResponse() {
    ErrorMapLoadingHandler handler = new ErrorMapLoadingHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(ErrorMapLoadingHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.ERROR_MAP.opcode(), false, false, true);
    assertNotNull(channel.pipeline().get(ErrorMapLoadingHandler.class));
    ReferenceCountUtil.release(writtenRequest);

    ByteBuf response = decodeHexDump(readResource(
      "error_errormap_response.txt",
      ErrorMapLoadingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    assertEquals(1, simpleEventBus.publishedEvents().size());
    ErrorMapLoadingFailedEvent event =
      (ErrorMapLoadingFailedEvent) simpleEventBus.publishedEvents().get(0);

    assertEquals(Event.Severity.WARN, event.severity());
    assertEquals("KV Error Map Negotiation failed (Status 0x1)", event.description());
    assertNull(channel.attr(ChannelAttributes.ERROR_MAP_KEY).get());
  }

  /**
   * This seems to be a real edge case, but when the server returns a successful
   * response but with no map, we should still handle it gracefully.
   */
  @Test
  void decodeSuccessfulResponseWithEmptyMap() {
    ErrorMapLoadingHandler handler = new ErrorMapLoadingHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(ErrorMapLoadingHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.ERROR_MAP.opcode(), false, false, true);
    assertNotNull(channel.pipeline().get(ErrorMapLoadingHandler.class));
    ReferenceCountUtil.release(writtenRequest);

    ByteBuf response = decodeHexDump(readResource(
      "success_empty_errormap_response.txt",
      ErrorMapLoadingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    assertEquals(2, simpleEventBus.publishedEvents().size());

    ErrorMapUndecodableEvent undecodableEvent =
      (ErrorMapUndecodableEvent) simpleEventBus.publishedEvents().get(0);
    assertEquals(
      "KV Error Map loaded but undecodable. "
        + "Message: \"No content in response\", Content: \"\"",
      undecodableEvent.description()
    );

    ErrorMapLoadedEvent event =
      (ErrorMapLoadedEvent) simpleEventBus.publishedEvents().get(1);

    assertEquals(Event.Severity.DEBUG, event.severity());
    Optional<ErrorMap> maybeMap = event.errorMap();
    assertFalse(maybeMap.isPresent());
  }

}
