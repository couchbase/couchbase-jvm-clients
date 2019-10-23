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

import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.verifyRequest;
import static com.couchbase.client.test.Util.readResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.io.FeaturesNegotiatedEvent;
import com.couchbase.client.core.cnc.events.io.FeaturesNegotiationFailedEvent;
import com.couchbase.client.core.cnc.events.io.UnsolicitedFeaturesReturnedEvent;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.*;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Verifies the functionality of the {@link FeatureNegotiatingHandler}.
 *
 * @author Michael Nitschinger
 * @since 2.0.0
 */
class FeatureNegotiatingHandlerTest {

  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private EndpointContext endpointContext;
  private EmbeddedChannel channel;
  private SimpleEventBus simpleEventBus;
  private TimeoutConfig timeoutConfig;

  @BeforeEach
  void setup() {
    channel = new EmbeddedChannel();
    simpleEventBus = new SimpleEventBus(true);
    CoreEnvironment env = mock(CoreEnvironment.class);
    timeoutConfig = mock(TimeoutConfig.class);
    when(env.eventBus()).thenReturn(simpleEventBus);
    when(env.timeoutConfig()).thenReturn(timeoutConfig);
    when(env.userAgent()).thenReturn(new UserAgent("some", Optional.empty(), Optional.empty(), Optional.empty()));
    when(timeoutConfig.connectTimeout()).thenReturn(Duration.ofMillis(1000));
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

    FeatureNegotiatingHandler handler = new FeatureNegotiatingHandler(
      endpointContext,
      Collections.singleton(ServerFeature.TRACING)
    );
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
    Duration timeout = Duration.ofMillis(100);
    when(timeoutConfig.connectTimeout()).thenReturn(timeout);

    FeatureNegotiatingHandler handler = new FeatureNegotiatingHandler(
      endpointContext,
      Collections.singleton(ServerFeature.TRACING)
    );
    channel.pipeline().addLast(handler);

    final ChannelFuture connect = channel.connect(
      new InetSocketAddress("1.2.3.4", 1234)
    );
    channel.pipeline().fireChannelActive();

    Thread.sleep(timeout.toMillis() + 5);
    channel.runScheduledPendingTasks();

    assertTrue(connect.isDone());
    assertTrue(connect.cause() instanceof TimeoutException);
    assertEquals("KV Feature Negotiation timed out after 100ms", connect.cause().getMessage());
  }

  /**
   * This test makes sure that if no server features need to be negotiated, we are not even
   * sending a request and completing the connect phase immediately.
   */
  @Test
  void connectInstantlyIfNoFeaturesNeeded() {
    FeatureNegotiatingHandler handler = new FeatureNegotiatingHandler(
      endpointContext,
      Collections.emptySet()
    );
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(FeatureNegotiatingHandler.class));
    ChannelFuture connect = channel.connect(new InetSocketAddress("1.2.3.4", 1234));

    channel.runPendingTasks();
    assertTrue(connect.isSuccess());
    assertNull(channel.pipeline().get(FeatureNegotiatingHandler.class));
  }

  /**
   * This test verifies that the sent hello request looks like it should.
   *
   * @param enabledFeatures parameterized input in the enabled features.
   */
  @ParameterizedTest
  @MethodSource("featureProvider")
  void encodeAndSendHelloRequest(Set<ServerFeature> enabledFeatures) {
    FeatureNegotiatingHandler handler = new FeatureNegotiatingHandler(
      endpointContext,
      enabledFeatures
    );
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(FeatureNegotiatingHandler.class));
    channel.connect(new InetSocketAddress("1.2.3.4", 1234));

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.HELLO.opcode(), true, false, true);

    // sanity check json block
    assertTrue(ProtocolVerifier.key(writtenRequest).isPresent());
    String json = ProtocolVerifier.key(writtenRequest).get().toString(UTF_8);
    assertEquals(
      "{\"a\":\"some/0.0.0\",\"i\":\"0000000000000001/0000000000000001\"}",
      json
    );

    // sanity check enabled features
    assertTrue(ProtocolVerifier.body(writtenRequest).isPresent());
    ByteBuf features = ProtocolVerifier.body(writtenRequest).get();
    assertEquals(enabledFeatures.size() * 2, features.readableBytes());

    ReferenceCountUtil.release(writtenRequest);
  }

  /**
   * Provider used for {@link #encodeAndSendHelloRequest(Set)} to test a couple of different
   * combinations.
   *
   * @return a stream of server features to test.
   */
  private static Stream<Set<ServerFeature>> featureProvider() {
    return Stream.of(
      EnumSet.of(ServerFeature.DUPLEX, ServerFeature.COLLECTIONS),
      EnumSet.of(ServerFeature.JSON),
      EnumSet.of(ServerFeature.SNAPPY, ServerFeature.SELECT_BUCKET, ServerFeature.TCPNODELAY)
    );
  }

  /**
   * This test verifies that a successful hello response is properly handled.
   */
  @Test
  void decodeAndPropagateSuccessHelloResponse() {
    Set<ServerFeature> toNegotiate = EnumSet.of(ServerFeature.TCPNODELAY,
      ServerFeature.XATTR, ServerFeature.XERROR, ServerFeature.SELECT_BUCKET,
      ServerFeature.SNAPPY, ServerFeature.TRACING);
    FeatureNegotiatingHandler handler = new FeatureNegotiatingHandler(
      endpointContext,
      toNegotiate
    );
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(FeatureNegotiatingHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.HELLO.opcode(), true, false, true);
    assertNotNull(channel.pipeline().get(FeatureNegotiatingHandler.class));

    ByteBuf response = decodeHexDump(readResource(
      "success_hello_response.txt",
      FeatureNegotiatingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    assertEquals(1, simpleEventBus.publishedEvents().size());
    FeaturesNegotiatedEvent event =
      (FeaturesNegotiatedEvent) simpleEventBus.publishedEvents().get(0);
    assertEquals(
      "Negotiated [TCPNODELAY, XATTR, XERROR, SELECT_BUCKET, SNAPPY, TRACING]",
      event.description()
    );
    assertEquals(Event.Severity.DEBUG, event.severity());

    List<ServerFeature> serverFeatures = channel.attr(ChannelAttributes.SERVER_FEATURE_KEY).get();
    assertEquals(toNegotiate, new HashSet<>(serverFeatures));
    assertNull(channel.pipeline().get(FeatureNegotiatingHandler.class));

    ReferenceCountUtil.release(writtenRequest);
  }

  /**
   * This test checks that a non-successful response is properly handled.
   */
  @Test
  void decodeNonSuccessfulHelloResponse() {
    Set<ServerFeature> toNegotiate = EnumSet.of(ServerFeature.TCPNODELAY,
      ServerFeature.XATTR, ServerFeature.XERROR, ServerFeature.SELECT_BUCKET,
      ServerFeature.SNAPPY, ServerFeature.TRACING);
    FeatureNegotiatingHandler handler = new FeatureNegotiatingHandler(
      endpointContext,
      toNegotiate
    );
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(FeatureNegotiatingHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.HELLO.opcode(), true, false, true);
    assertNotNull(channel.pipeline().get(FeatureNegotiatingHandler.class));

    ByteBuf response = decodeHexDump(readResource(
      "error_hello_response.txt",
      FeatureNegotiatingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    assertEquals(2, simpleEventBus.publishedEvents().size());
    FeaturesNegotiationFailedEvent failureEvent =
      (FeaturesNegotiationFailedEvent) simpleEventBus.publishedEvents().get(0);
    assertEquals(Event.Severity.WARN, failureEvent.severity());
    assertEquals("HELLO Negotiation failed (Status 0x1)", failureEvent.description());

    FeaturesNegotiatedEvent event =
      (FeaturesNegotiatedEvent) simpleEventBus.publishedEvents().get(1);
    assertEquals(
      "Negotiated []",
      event.description()
    );
    assertEquals(Event.Severity.DEBUG, event.severity());

    List<ServerFeature> serverFeatures = channel.attr(ChannelAttributes.SERVER_FEATURE_KEY).get();
    assertTrue(serverFeatures.isEmpty());
    assertNull(channel.pipeline().get(FeatureNegotiatingHandler.class));

    ReferenceCountUtil.release(writtenRequest);
  }

  /**
   * Should the server return non-asked-for features, ignore them.
   */
  @Test
  void decodeAndIgnoreNonAskedForFeaturesInResponse() {
    Set<ServerFeature> toNegotiate = EnumSet.of(ServerFeature.SNAPPY, ServerFeature.TRACING);
    FeatureNegotiatingHandler handler = new FeatureNegotiatingHandler(
      endpointContext,
      toNegotiate
    );
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(FeatureNegotiatingHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.HELLO.opcode(), true, false, true);
    assertNotNull(channel.pipeline().get(FeatureNegotiatingHandler.class));

    ByteBuf response = decodeHexDump(readResource(
      "success_hello_response.txt",
      FeatureNegotiatingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    assertEquals(2, simpleEventBus.publishedEvents().size());

    UnsolicitedFeaturesReturnedEvent unsolicitedEvent =
      (UnsolicitedFeaturesReturnedEvent) simpleEventBus.publishedEvents().get(0);
    assertEquals(
      "Received unsolicited features during HELLO [TCPNODELAY, XATTR, XERROR, SELECT_BUCKET]",
      unsolicitedEvent.description()
    );
    assertEquals(Event.Severity.WARN, unsolicitedEvent.severity());

    FeaturesNegotiatedEvent event =
      (FeaturesNegotiatedEvent) simpleEventBus.publishedEvents().get(1);
    assertEquals(
      "Negotiated [SNAPPY, TRACING]",
      event.description()
    );

    List<ServerFeature> serverFeatures = channel.attr(ChannelAttributes.SERVER_FEATURE_KEY).get();
    assertEquals(toNegotiate, new HashSet<>(serverFeatures));
    assertNull(channel.pipeline().get(FeatureNegotiatingHandler.class));

    ReferenceCountUtil.release(writtenRequest);
  }

}
