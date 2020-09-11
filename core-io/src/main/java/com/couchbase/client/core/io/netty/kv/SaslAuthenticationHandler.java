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
import com.couchbase.client.core.cnc.events.io.SaslAuthenticationRestartedEvent;
import com.couchbase.client.core.cnc.events.io.SaslMechanismsSelectedEvent;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.context.KeyValueIoErrorContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.kv.sasl.CouchbaseSaslClientFactory;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;
import com.couchbase.client.core.util.Bytes;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.opcode;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.successful;
import static java.nio.charset.StandardCharsets.UTF_8;

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

  /**
   * Stores the number of roundtrips the chosen algorithm still has to go through.
   */
  private int roundtripsToGo;

  public SaslAuthenticationHandler(final EndpointContext endpointContext, final String username,
                                   final String password, final Set<SaslMechanism> allowedSaslMechanisms) {
    this.endpointContext = endpointContext;
    this.username = username;
    this.password = password;
    this.allowedMechanisms = allowedSaslMechanisms;
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
    startAuthSequence(ctx, allowedMechanisms);
  }

  /**
   * Starts the SASL auth sequence with the set of mechanisms that are valid for this specific run.
   *
   * @param ctx the channel handler context
   * @param usedMechanisms the mechanisms that can be used during this run
   */
  private void startAuthSequence(final ChannelHandlerContext ctx, final Set<SaslMechanism> usedMechanisms) {
    try {
      saslClient = createSaslClient(usedMechanisms);

      SaslMechanism selectedMechanism = SaslMechanism.from(saslClient.getMechanismName());
      roundtripsToGo = selectedMechanism.roundtrips();

      endpointContext.environment().eventBus().publish(new SaslMechanismsSelectedEvent(
        ioContext,
        usedMechanisms,
        selectedMechanism
      ));

      ctx.writeAndFlush(buildAuthRequest(ctx));
      maybePropagateChannelActive(ctx);
    } catch (SaslException e) {
      failConnect(ctx,
        "SASL Client could not be constructed",
        null,
        e,
        (short) 0
      );
    }
  }

  /**
   * Check if the number of roundtrips allow propagating the channel active, enabling pipelining from higher
   * levels.
   *
   * @param ctx the channel handler context.
   */
  private void maybePropagateChannelActive(final ChannelHandlerContext ctx) {
    if (roundtripsToGo == 1) {
      ctx.fireChannelActive();
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof ByteBuf) {
      ByteBuf response = (ByteBuf) msg;
      roundtripsToGo--;
      if (successful(response) || status(response) == STATUS_AUTH_CONTINUE) {
        byte opcode = opcode(response);
        try {
          if (MemcacheProtocol.Opcode.SASL_AUTH.opcode() == opcode) {
            handleAuthResponse(ctx, (ByteBuf) msg);
          } else if (MemcacheProtocol.Opcode.SASL_STEP.opcode() == opcode) {
            completeAuth(ctx);
          }
        } catch (Exception ex) {
          failConnect(ctx, "Unexpected error during SASL auth", response, ex, status(response));
        }
      } else if (STATUS_AUTH_ERROR == status(response)) {
        maybeFailConnect(ctx, "Authentication Failure - Potential causes: invalid credentials or if " +
          "LDAP is enabled ensure PLAIN SASL mechanism is exclusively used on the PasswordAuthenticator (insecure) or " +
          "TLS is used (recommended)", response, null, status(response));
      } else {
        failConnect(
          ctx,
          "Unexpected Status 0x" + Integer.toHexString(status(response)) + " during SASL auth",
          response,
          null,
          status(response)
        );
      }
    } else {
      failConnect(
        ctx,
        "Unexpected response type on channel read, this is a bug - please report. " + msg,
        null,
        null,
        (short) 0
      );
    }

    ReferenceCountUtil.release(msg);
  }

  /**
   * Check if we need to do an auth retry with different mechs instead of giving up immediately.
   * <p>
   * The method performs the logic roughly as follows: we know that the current authentication attempt failed, but
   * because we are pipelining the original auth request with our allowed list it could be that they do not overlap
   * (i.e. only PLAIN is allowed because the server has LDAP enabled but we do not negotiate it by default), so give
   * it another chance to run with the updated and merged mechs list. If it still fails the next time it will terminate
   * eventually.
   * <p>
   * Most of the time though it will fail immediately since the mechs are aligned and the user just entered the
   * wrong credentials.
   */
  private void maybeFailConnect(final ChannelHandlerContext ctx, final String message, final ByteBuf lastPacket,
                                final Throwable cause, final short status) {
    final SaslMechanism currentlyUsedMech = SaslMechanism.from(saslClient.getMechanismName());
    final Set<SaslMechanism> negotiatedMechs = ctx.channel().attr(ChannelAttributes.SASL_MECHS_KEY).get();

    if (negotiatedMechs.contains(currentlyUsedMech)) {
      failConnect(ctx, message, lastPacket, cause, status);
    } else {
      Set<SaslMechanism> mergedMechs = allowedMechanisms
        .stream()
        .filter(negotiatedMechs::contains)
        .collect(Collectors.toSet());

      if (mergedMechs.isEmpty()) {
        failConnect(ctx, "Could not negotiate SASL mechanism with server. If you are using LDAP you must either" +
           "connect via TLS (recommended), or ONLY enable PLAIN in the allowed SASL mechanisms list on the PasswordAuthenticator" +
           "(this is insecure and will present the user credentials in plain-text over the wire).",
           lastPacket, cause, status);
      } else {
        ioContext.environment().eventBus().publish(new SaslAuthenticationRestartedEvent(ioContext));
        startAuthSequence(ctx, mergedMechs);
      }
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
      ? saslClient.evaluateChallenge(Bytes.EMPTY_BYTE_ARRAY)
      : null;
    ByteBuf body = payload != null
      ? ctx.alloc().buffer().writeBytes(payload)
      : Unpooled.EMPTY_BUFFER;
    ByteBuf key = Unpooled.copiedBuffer(saslClient.getMechanismName(), UTF_8);
    ByteBuf request = request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.SASL_AUTH,
      noDatatype(),
      noPartition(),
      BaseKeyValueRequest.nextOpaque(),
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
        maybePropagateChannelActive(ctx);
      } else {
        throw new SaslException("Evaluation returned empty payload, this is unexpected!");
      }
    } catch (SaslException e) {
      failConnect(ctx, "Failure while evaluating SASL Auth Response.", response, e, MemcacheProtocol.status(response));
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
    if (mech.equalsIgnoreCase(SaslMechanism.PLAIN.mech())) {
      String[] evaluated = new String(evaluatedBytes, UTF_8).split(" ");
      body = Unpooled.copiedBuffer(username + "\0" + evaluated[1], UTF_8);
    } else {
      body = Unpooled.wrappedBuffer(evaluatedBytes);
    }

    ByteBuf key = Unpooled.copiedBuffer(mech, UTF_8);
    ByteBuf request =request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.SASL_STEP,
      noDatatype(),
      noPartition(),
      BaseKeyValueRequest.nextOpaque(),
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
                           final ByteBuf lastPacket, final Throwable cause, final short status) {
    Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);

    byte[] packetCopy = Bytes.EMPTY_BYTE_ARRAY;
    Map<String, Object> serverContext = null;

    if (lastPacket != null) {
      if (MemcacheProtocol.verifyResponse(lastPacket)) {
        // This is a proper response, try to extract server context
        Optional<ByteBuf> body = MemcacheProtocol.body(lastPacket);
        if (body.isPresent()) {
          byte[] content = new byte[body.get().readableBytes()];
          body.get().readBytes(content);

          try {
            serverContext = Mapper.decodeInto(content, new TypeReference<Map<String, Object>>() {});
          } catch (Exception ex) {
            // Ignore, no displayable content
          }
        }
      } else {
        // This is not a proper memcache response, store the raw packet for debugging purposes
        int ridx = lastPacket.readerIndex();
        lastPacket.readerIndex(lastPacket.writerIndex());
        packetCopy = new byte[lastPacket.readableBytes()];
        lastPacket.readBytes(packetCopy);
        lastPacket.readerIndex(ridx);
      }
    }

    KeyValueIoErrorContext errorContext = new KeyValueIoErrorContext(
      MemcacheProtocol.decodeStatus(status), endpointContext, serverContext
    );

    endpointContext.environment().eventBus().publish(new SaslAuthenticationFailedEvent(
      latency.orElse(Duration.ZERO),
      errorContext,
      message,
      packetCopy
    ));
    interceptedConnectPromise.tryFailure(new AuthenticationFailureException(
      message,
      errorContext,
      cause
    ));
    ctx.pipeline().remove(this);
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
