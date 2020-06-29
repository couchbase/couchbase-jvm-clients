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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.io.SaslMechanismsListingFailedEvent;
import com.couchbase.client.core.cnc.events.io.SaslMechanismsListedEvent;
import com.couchbase.client.core.cnc.events.io.UnknownSaslMechanismDetectedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.env.SaslMechanism;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.KeyValueIoErrorContext;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.successful;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link SaslListMechanismsHandler} asks the server KV engine which SASL mechanism it supports.
 * <p>
 * Note that this handler only performs the listing and is separate from other SASL handlers in the pipeline
 * which then actually decide what to do with the list. When negotiated, for pipelining reasons, the list
 * of mechanisms is stored in the pipeline so that it can be consumed from other handlers.
 */
@Stability.Internal
public class SaslListMechanismsHandler extends ChannelDuplexHandler {

  /**
   * Holds the timeout for the full sasl list mechs phase.
   */
  private final Duration timeout;

  /**
   * Holds the core context as reference to event bus and more.
   */
  private final EndpointContext endpointContext;

  /**
   * Once connected, holds the io context for more debug information.
   */
  private IoContext ioContext;

  /**
   * Holds the intercepted promise from up the pipeline which is either
   * completed or failed depending on the downstream components or the
   * result of the negotiation.
   */
  private ChannelPromise interceptedConnectPromise;

  /**
   * Creates a new {@link SaslListMechanismsHandler}.
   *
   * @param endpointContext the core context used to refer to values like the core id.
   */
  public SaslListMechanismsHandler(final EndpointContext endpointContext) {
    this.endpointContext = endpointContext;
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

  /**
   * As soon as the channel is active start sending the request but also schedule
   * a timeout properly.
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel active operation is made.
   */
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
          new TimeoutException("SASL Mechanism listing timed out after "
            + timeout.toMillis() + "ms")
        );
      }
    }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    ConnectTimings.start(ctx.channel(), this.getClass());
    ctx.writeAndFlush(buildListMechanismsRequest(ctx));

    // Fire the channel active immediately so the upper handler in the pipeline gets a chance to
    // pipeline its request before the response of this one arrives. This helps speeding up the
    // bootstrap sequence.
    ctx.fireChannelActive();
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
      BaseKeyValueRequest.nextOpaque(),
      noCas(),
      noExtras(),
      noKey(),
      noBody()
    );
  }

  /**
   * As soon as we get a response, turn it into a list of SASL mechanisms the server supports.
   * <p>
   * If the server responds with an empty list this is an issue and as a result we need to fail the connection
   * immediately.
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel read operation is made.
   * @param msg the incoming msg that needs to be parsed.
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof ByteBuf) {
      Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);

      if (successful((ByteBuf) msg)) {
        String[] rawMechansisms = body((ByteBuf) msg)
          .orElse(Unpooled.EMPTY_BUFFER)
          .toString(UTF_8)
          .split(" ");

        boolean isEmpty = rawMechansisms.length == 1 && rawMechansisms[0].isEmpty();
        if (rawMechansisms.length > 0 && !isEmpty) {
          final Set<SaslMechanism> serverMechanisms = decodeMechanisms(rawMechansisms);
          ioContext.environment().eventBus().publish(
            new SaslMechanismsListedEvent(ioContext, serverMechanisms, latency.orElse(Duration.ZERO))
          );
          ctx.channel().attr(ChannelAttributes.SASL_MECHS_KEY).set(serverMechanisms);

          interceptedConnectPromise.trySuccess();
          ctx.pipeline().remove(this);
        } else {
          failConnection("Received empty mechanism list from server", status((ByteBuf) msg), latency);
        }
      } else {
        failConnection("Received non-success status from server", status((ByteBuf) msg), latency);
      }
    } else {
      interceptedConnectPromise.tryFailure(new CouchbaseException("Unexpected response "
        + "type on channel read, this is a bug - please report. " + msg));
    }

    ReferenceCountUtil.release(msg);
  }

  /**
   * Helper method to fail the connection due to an unsuccessful sasl list mechs event.
   *
   * @param message the message that should be part of the error
   * @param status the status code from the memcached response
   * @param duration the duration how long it took overall
   */
  private void failConnection(final String message, final short status, final Optional<Duration> duration) {
    KeyValueIoErrorContext errorContext = new KeyValueIoErrorContext(
      MemcacheProtocol.decodeStatus(status), endpointContext, null
    );
    ioContext.environment().eventBus().publish(
      new SaslMechanismsListingFailedEvent(duration.orElse(Duration.ZERO), errorContext, message)
    );
    interceptedConnectPromise.tryFailure(new AuthenticationFailureException(message, errorContext, null));
  }

  /**
   * Decodes the raw mechanisms and discards those which are unknown (but raises an event so they are not lost).
   *
   * @param encoded the encoded string mechanisms as input
   * @return the decoded enums as output
   */
  private Set<SaslMechanism> decodeMechanisms(final String[] encoded) {
    return Arrays
      .stream(encoded)
      .map(e -> {
        SaslMechanism mech = SaslMechanism.from(e);
        if (mech == null) {
          ioContext.environment().eventBus().publish(new UnknownSaslMechanismDetectedEvent(ioContext, e));
        }
        return mech;
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toSet());
  }

  /**
   * If there is an exception raised while we are waiting for our connect phase to complete, the error
   * should be propagated as a cause up the pipeline.
   *
   * <p>One reason for example could be TLS problems that need to be surfaced up the stack properly.</p>
   *
   * @param ctx the channel handler context.
   * @param cause the cause of the problem.
   */
  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    if (!interceptedConnectPromise.isDone()) {
      interceptedConnectPromise.tryFailure(cause);
    }
    ctx.fireExceptionCaught(cause);
  }

}
