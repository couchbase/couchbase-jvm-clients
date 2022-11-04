/*
 * Copyright (c) 2020 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.events.io.SaslMechanismsListedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownSaslMechanismDetectedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.verifyRequest;
import static com.couchbase.client.test.Util.readResource;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link SaslListMechanismsHandler}.
 */
public class SaslListMechanismsHandlerTest extends AbstractKeyValueEmbeddedChannelTest {

  private EndpointContext endpointContext;

  @BeforeEach
  @Override
  protected void beforeEach() {
    super.beforeEach();

    CoreEnvironment env = mock(CoreEnvironment.class);
    TimeoutConfig timeoutConfig = mock(TimeoutConfig.class);
    when(env.eventBus()).thenReturn(eventBus);
    when(env.timeoutConfig()).thenReturn(timeoutConfig);
    when(timeoutConfig.connectTimeout()).thenReturn(Duration.ofMillis(1000));
    CoreContext coreContext = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    endpointContext = new EndpointContext(coreContext, new HostAndPort("127.0.0.1", 1234),
      null, ServiceType.KV, Optional.empty(), Optional.empty(), Optional.empty());
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

    SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);
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
    eventBus = new SimpleEventBus(true);
    CoreEnvironment env = mock(CoreEnvironment.class);
    TimeoutConfig timeoutConfig = mock(TimeoutConfig.class);
    when(env.eventBus()).thenReturn(eventBus);
    when(env.timeoutConfig()).thenReturn(timeoutConfig);
    when(timeoutConfig.connectTimeout()).thenReturn(Duration.ofMillis(100));
    CoreContext coreContext = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    EndpointContext endpointContext = new EndpointContext(coreContext,
      new HostAndPort("127.0.0.1", 1234), null, ServiceType.KV,
      Optional.empty(), Optional.empty(), Optional.empty());

    SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);

    channel.pipeline().addLast(handler);

    final ChannelFuture connect = channel.connect(
      new InetSocketAddress("1.2.3.4", 1234)
    );
    channel.pipeline().fireChannelActive();

    Thread.sleep(Duration.ofMillis(100).toMillis() + 5);
    channel.runScheduledPendingTasks();

    assertTrue(connect.isDone());
    assertInstanceOf(TimeoutException.class, connect.cause());
    assertEquals("SASL Mechanism listing timed out after 100ms", connect.cause().getMessage());
  }

  /**
   * This test verifies that the client sends the proper KV command to the server
   * when requesting the SASL mechanisms.
   */
  @Test
  void encodeAndSendListMechsRequest() {
    SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(SaslListMechanismsHandler.class));
    channel.connect(new InetSocketAddress("1.2.3.4", 1234));

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.SASL_LIST_MECHS.opcode(), false, false, false);

    ReferenceCountUtil.release(writtenRequest);
  }

  @Test
  void decodesSuccessfulSaslMechsList() {
    SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(SaslListMechanismsHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.SASL_LIST_MECHS.opcode(), false, false, false);
    assertNotNull(channel.pipeline().get(SaslListMechanismsHandler.class));
    ReferenceCountUtil.release(writtenRequest);

    ByteBuf response = decodeHexDump(readResource(
      "success_sasl_list_mechs_response.txt",
      ErrorMapLoadingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    Set<SaslMechanism> saslMechanisms = channel.attr(ChannelAttributes.SASL_MECHS_KEY).get();
    assertEquals(saslMechanisms, EnumSet.of(SaslMechanism.PLAIN, SaslMechanism.SCRAM_SHA1, SaslMechanism.SCRAM_SHA256, SaslMechanism.SCRAM_SHA512));

    assertInstanceOf(SaslMechanismsListedEvent.class, eventBus.publishedEvents().get(0));
  }

  @Test
  void ignoresUnknownSaslMech() {
    SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(SaslListMechanismsHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.SASL_LIST_MECHS.opcode(), false, false, false);
    assertNotNull(channel.pipeline().get(SaslListMechanismsHandler.class));
    ReferenceCountUtil.release(writtenRequest);

    ByteBuf response = decodeHexDump(readResource(
      "success_sasl_list_mechs_unknown_mech_response.txt",
      ErrorMapLoadingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertTrue(connectFuture.isSuccess());

    Set<SaslMechanism> saslMechanisms = channel.attr(ChannelAttributes.SASL_MECHS_KEY).get();
    assertEquals(saslMechanisms, EnumSet.of(SaslMechanism.SCRAM_SHA1, SaslMechanism.SCRAM_SHA256, SaslMechanism.SCRAM_SHA512));

    UnknownSaslMechanismDetectedEvent event = (UnknownSaslMechanismDetectedEvent) eventBus.publishedEvents().get(0);
    assertEquals("FLAUN", event.mechanism());
  }

  @Test
  void failConnectIfSaslMechsListEmpty() {
    SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(SaslListMechanismsHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.SASL_LIST_MECHS.opcode(), false, false, false);
    assertNotNull(channel.pipeline().get(SaslListMechanismsHandler.class));
    ReferenceCountUtil.release(writtenRequest);

    ByteBuf response = decodeHexDump(readResource(
      "success_sasl_list_mechs_empty_response.txt",
      ErrorMapLoadingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertFalse(connectFuture.isSuccess());
    AuthenticationFailureException cause = (AuthenticationFailureException) connectFuture.cause();
    assertTrue(cause.getMessage().contains("Received empty mechanism list from server"));
  }

  @Test
  void failConnectIfStatusNotSuccess() {
    SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);
    channel.pipeline().addLast(handler);

    assertEquals(handler, channel.pipeline().get(SaslListMechanismsHandler.class));
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("1.2.3.4", 1234));
    assertFalse(connectFuture.isDone());

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    ByteBuf writtenRequest = channel.readOutbound();
    verifyRequest(writtenRequest, MemcacheProtocol.Opcode.SASL_LIST_MECHS.opcode(), false, false, false);
    assertNotNull(channel.pipeline().get(SaslListMechanismsHandler.class));
    ReferenceCountUtil.release(writtenRequest);

    ByteBuf response = decodeHexDump(readResource(
      "error_sasl_list_mechs_response.txt",
      ErrorMapLoadingHandlerTest.class
    ));
    channel.writeInbound(response);
    channel.runPendingTasks();

    assertFalse(connectFuture.isSuccess());
    AuthenticationFailureException cause = (AuthenticationFailureException) connectFuture.cause();
    assertTrue(cause.getMessage().contains("Received non-success status from server"));
  }

  /**
   * This test makes sure that after the initial request is sent, the channel active signal is
   * propagated so that we do not regress bootstrap pipelining functionality.
   */
  @Test
  void propagatesChannelActiveAfterSendingInitialRequest() {
    final SaslListMechanismsHandler handler = new SaslListMechanismsHandler(endpointContext);
    final AtomicBoolean channelActiveFired = new AtomicBoolean();

    channel.pipeline().addLast(handler).addLast(new SimpleChannelInboundHandler<ByteBuf>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) { }

      @Override
      public void channelActive(ChannelHandlerContext ctx) {
        channelActiveFired.set(true);
      }
    });

    assertEquals(handler, channel.pipeline().get(SaslListMechanismsHandler.class));
    channel.connect(new InetSocketAddress("1.2.3.4", 1234));

    channel.pipeline().fireChannelActive();
    channel.runPendingTasks();
    waitUntilCondition(channelActiveFired::get);
  }


}
