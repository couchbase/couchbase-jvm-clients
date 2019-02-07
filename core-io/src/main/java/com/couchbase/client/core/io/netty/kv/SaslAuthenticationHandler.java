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

import com.couchbase.client.core.cnc.events.io.SaslAuthenticationCompletedEvent;
import com.couchbase.client.core.cnc.events.io.SaslAuthenticationFailedEvent;
import com.couchbase.client.core.cnc.events.io.SaslMechanismsSelectedEvent;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.error.AuthenticationException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.kv.sasl.CouchbaseSaslClientFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.opcode;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.successful;

/**
 * This handler is responsible for perform SASL authentication against the KV engine.
 *
 * <p>SASL is a complicated back-and-forth protocol which involves potentially many steps
 * depending on the mechanism used. Couchbase supports a variety of protocols depending
 * on the version, so the first step is to actually ask the server for the types
 * of procotols it supports. Once the client has this figured out, it initializes the
 * SASL client and starts the "back and forth" challenge response protocol. All of this
 * opaque payload is framed over the memcache binary protocol as usual.</p>
 *
 * <p>Through configuration it is possible to change some defaults, for example limit the
 * types of protocols accepted.</p>
 *
 * @since 2.0.0
 */
public class SaslAuthenticationHandler extends ChannelDuplexHandler implements CallbackHandler {

  /**
   * Status indicating an authentication error.
   */
  private static final short STATUS_AUTH_ERROR = 0x20;

  /**
   * Status indicating to continue the authentication process.
   */
  private static final short STATUS_AUTH_CONTINUE = 0x21;

  /**
   * Holds the timeout for the full feature negotiation phase.
   */
  private final Duration timeout;
  private final String username;
  private final String password;
  private final Set<SaslMechanism> allowedMechanisms;
  private final EndpointContext endpointContext;

  /**
   * Once connected, holds the io context for more debug information.
   */
  private IoContext ioContext;

  /**
   * The JVM {@link SaslClient} that handles the actual authentication process.
   */
  private SaslClient saslClient;

  /**
   * Holds the intercepted promise from up the pipeline which is either
   * completed or failed depending on the downstream components or the
   * result of the SASL auth process.
   */
  private ChannelPromise interceptedConnectPromise;

  public SaslAuthenticationHandler(final EndpointContext endpointContext, final String username,
                                   final String password) {
    this.endpointContext = endpointContext;
    this.username = username;
    this.password = password;
    this.allowedMechanisms = endpointContext.environment().ioConfig().allowedSaslMechanisms();
    this.timeout = endpointContext.environment().timeoutConfig().connectTimeout();
  }

  /**
   * Intercepts the connect process inside the pipeline to only propagate either
   * success or failure if the hello process is completed either way.
   *
   * @param ctx           the {@link ChannelHandlerContext} for which the connect operation is made.
   * @param remoteAddress the {@link SocketAddress} to which it should connect.
   * @param localAddress  the {@link SocketAddress} which is used as source on connect.
   * @param promise       the {@link ChannelPromise} to notify once the operation completes.
   */
  @Override
  public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                      final SocketAddress localAddress, final ChannelPromise promise) {
      interceptedConnectPromise = promise;
      ChannelPromise downstream = ctx.newPromise();
      downstream.addListener(f -> {
        if (!f.isSuccess() && !interceptedConnectPromise.isDone()) {
          ConnectTimings.record(ctx.channel(), this.getClass());
          interceptedConnectPromise.tryFailure(f.cause());
        }
      });
      ctx.connect(remoteAddress, localAddress, downstream);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );

    ctx.executor().schedule(() -> {
      if (!interceptedConnectPromise.isDone()) {
        ConnectTimings.stop(ctx.channel(), this.getClass(), true);
        interceptedConnectPromise.tryFailure(
          new TimeoutException("KV SASL Negotiation timed out after "
            + timeout.toMillis() + "ms")
        );
      }
    }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    ConnectTimings.start(ctx.channel(), this.getClass());
    ctx.writeAndFlush(buildListMechanismsRequest(ctx));
  }

  /**
   * Helper method to build the initial SASL list mechanisms request.
   *
   * @param ctx the channel handler context to use.
   * @return the encoded representation of the request.
   */
  private ByteBuf buildListMechanismsRequest(final ChannelHandlerContext ctx) {
    return MemcacheProtocol.request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.SASL_LIST_MECHS,
      noDatatype(),
      noPartition(),
      Utils.opaque(ctx.channel(), true),
      noCas(),
      noExtras(),
      noKey(),
      noBody()
    );
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof ByteBuf) {
      ByteBuf response = (ByteBuf) msg;
      if (successful(response) || status(response) == STATUS_AUTH_CONTINUE) {
        byte opcode = opcode(response);
        try {
          if (MemcacheProtocol.Opcode.SASL_LIST_MECHS.opcode() == opcode) {
            handleListMechsResponse(ctx, (ByteBuf) msg);
          } else if (MemcacheProtocol.Opcode.SASL_AUTH.opcode() == opcode) {
            handleAuthResponse(ctx, (ByteBuf) msg);
          } else if (MemcacheProtocol.Opcode.SASL_STEP.opcode() == opcode) {
            completeAuth(ctx);
          }
        } catch (Exception ex) {
          failConnect(ctx, "Unexpected error during SASL auth", response, ex);
        }
      } else if (STATUS_AUTH_ERROR == status(response)) {
        failConnect(ctx, "Authentication Failure", null, null);
      } else {
        failConnect(
          ctx,
          "Unexpected Status 0x" + Integer.toHexString(status(response)) + " during SASL auth",
          response,
          null
        );
      }
    } else {
      failConnect(
        ctx,
        "Unexpected response type on channel read, this is a bug - please report. " + msg,
        null,
        null
      );
    }

    ReferenceCountUtil.release(msg);
  }

  /**
   * Handles a SASL list mechanisms responds and selects the proper algorithm.
   *
   * <p>Once selected the sasl client is constructed and then the first SASL AUTH message
   * is created and sent over the wire.</p>
   *
   * @param ctx the channel context.
   * @param response the response from the list mechs request.
   */
  private void handleListMechsResponse(final ChannelHandlerContext ctx, final ByteBuf response) {
    String[] serverMechanisms = body(response)
      .orElse(Unpooled.EMPTY_BUFFER)
      .toString(CharsetUtil.UTF_8)
      .split(" ");

    Set<SaslMechanism> usedMechs = allowedMechanisms
      .stream()
      .filter(m -> Arrays.asList(serverMechanisms).contains(m.mech()))
      .collect(Collectors.toSet());

    try {
      saslClient = createSaslClient(usedMechs);

      endpointContext.environment().eventBus().publish(new SaslMechanismsSelectedEvent(
        ioContext,
        Stream.of(serverMechanisms).map(SaslMechanism::from).collect(Collectors.toSet()),
        allowedMechanisms,
        SaslMechanism.from(saslClient.getMechanismName())
      ));

      ctx.writeAndFlush(buildAuthRequest(ctx));
    } catch (SaslException e) {
      failConnect(ctx,
        "SASL Client could not be constructed. Server Mechanisms: "
          + Arrays.toString(serverMechanisms),
        response,
        e
      );
    }
  }

  /**
   * Helper method to build the SASL auth request.
   *
   * @param ctx the channel context.
   * @return the created auth request.
   * @throws SaslException if something went wrong during challenge evaluation.
   */
  private ByteBuf buildAuthRequest(final ChannelHandlerContext ctx) throws SaslException {
    byte[] payload = saslClient.hasInitialResponse()
      ? saslClient.evaluateChallenge(new byte[]{})
      : null;
    ByteBuf body = payload != null
      ? ctx.alloc().buffer().writeBytes(payload)
      : Unpooled.EMPTY_BUFFER;
    ByteBuf key = Unpooled.copiedBuffer(saslClient.getMechanismName(), CharsetUtil.UTF_8);
    ByteBuf request = request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.SASL_AUTH,
      noDatatype(),
      noPartition(),
      Utils.opaque(ctx.channel(), true),
      noCas(),
      noExtras(),
      key,
      body
    );
    key.release();
    body.release();
    return request;
  }

  /**
   * Helper method to construct the {@link SaslClient}.
   *
   * @param selected the mechanisms that got selected.
   * @return a new sasl client
   * @throws SaslException if something went wrong during the creation.
   */
  private SaslClient createSaslClient(final Set<SaslMechanism> selected) throws SaslException {
    return new CouchbaseSaslClientFactory().createSaslClient(
      selected.stream().map(SaslMechanism::mech).toArray(String[]::new),
      null,
      "couchbase",
      ioContext.remoteSocket().toString(),
      null,
      this
    );
  }

  /**
   * Handle a SASL AUTH response and start the next SASL step.
   *
   * @param ctx the channel context.
   * @param response the response from the server from our AUTH request.
   */
  private void handleAuthResponse(final ChannelHandlerContext ctx, final ByteBuf response) {
    if (saslClient.isComplete()) {
      completeAuth(ctx);
      return;
    }

    ByteBuf responseBody = body(response).orElse(Unpooled.EMPTY_BUFFER);
    byte[] payload = new byte[responseBody.readableBytes()];
    responseBody.readBytes(payload);

    try {
      byte[] evaluatedBytes = saslClient.evaluateChallenge(payload);
      if (evaluatedBytes != null && evaluatedBytes.length > 0) {
        ctx.writeAndFlush(buildStepRequest(ctx, evaluatedBytes));
      } else {
        throw new SaslException("Evaluation returned empty payload, this is unexpected!");
      }
    } catch (SaslException e) {
      failConnect(ctx, "Failure while evaluating SASL Auth Response.", response, e);
    }
  }

  /**
   * Helper method to build the SASL step request based on the evaluated challenge.
   *
   * @param ctx the channel context
   * @param evaluatedBytes the evaluated challenge.
   * @return the full SASL step request.
   */
  private ByteBuf buildStepRequest(final ChannelHandlerContext ctx, final byte[] evaluatedBytes) {
    final String mech = saslClient.getMechanismName();

    ByteBuf body;
    if (mech.equalsIgnoreCase(SaslMechanism.CRAM_MD5.mech())
      || mech.equalsIgnoreCase(SaslMechanism.PLAIN.mech())) {
      String[] evaluated = new String(evaluatedBytes, CharsetUtil.UTF_8).split(" ");
      body = Unpooled.copiedBuffer(username + "\0" + evaluated[1], CharsetUtil.UTF_8);
    } else {
      body = Unpooled.wrappedBuffer(evaluatedBytes);
    }

    ByteBuf key = Unpooled.copiedBuffer(mech, CharsetUtil.UTF_8);
    ByteBuf request =request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.SASL_STEP,
      noDatatype(),
      noPartition(),
      Utils.opaque(ctx.channel(), true),
      noCas(),
      noExtras(),
      key,
      body
    );
    key.release();
    body.release();
    return request;
  }

  /**
   * Helper method to complete the SASL auth successfully.
   */
  private void completeAuth(final ChannelHandlerContext ctx) {
    Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);
    endpointContext.environment().eventBus().publish(
      new SaslAuthenticationCompletedEvent(latency.orElse(Duration.ZERO), ioContext)
    );
    interceptedConnectPromise.trySuccess();
    ctx.pipeline().remove(this);
    ctx.fireChannelActive();
  }

  /**
   * Refactored method which is called from many places to fail the connection
   * process because of an issue during SASL auth.
   *
   * <p>Usually errors during auth are very problematic and as a result we cannot continue
   * with this channel connect attempt.</p>
   *
   * @param ctx the channel context.
   *
   */
  private void failConnect(final ChannelHandlerContext ctx, final String message,
                           final ByteBuf lastPacket, final Throwable cause) {
    Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);

    byte[] packetCopy = new byte[] {};
    if (lastPacket != null) {
      int ridx = lastPacket.readerIndex();
      lastPacket.readerIndex(lastPacket.writerIndex());
      packetCopy = new byte[lastPacket.readableBytes()];
      lastPacket.readBytes(packetCopy);
      lastPacket.readerIndex(ridx);
    }

    endpointContext.environment().eventBus().publish(new SaslAuthenticationFailedEvent(
      latency.orElse(Duration.ZERO),
      ioContext,
      message,
      packetCopy
    ));
    interceptedConnectPromise.tryFailure(new AuthenticationException(message, cause));
  }

  /**
   * This SASL callback handler is used to call certain callbacks during the
   * authentication phases (to set the name and password if required).
   *
   * @param callbacks the callbacks to handle.
   */
  @Override
  public void handle(final Callback[] callbacks) throws UnsupportedCallbackException {
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        ((NameCallback) callback).setName(username);
      } else if (callback instanceof PasswordCallback) {
        ((PasswordCallback) callback).setPassword(password.toCharArray());
      } else {
        throw new UnsupportedCallbackException(callback, "Unexpected/Unsupported Callback");
      }
    }
  }

}
