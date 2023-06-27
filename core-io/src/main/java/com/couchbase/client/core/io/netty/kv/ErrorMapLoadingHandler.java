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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.io.ErrorMapLoadedEvent;
import com.couchbase.client.core.cnc.events.io.ErrorMapLoadingFailedEvent;
import com.couchbase.client.core.cnc.events.io.ErrorMapUndecodableEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.bodyAsBytes;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.status;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.successful;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This handler tries to load the KV Error Map in a best effort manner.
 *
 * <p>We are trying to grab a KV error map from kv_engine, but if the server does
 * not respond with a successful message and the configuration we keep moving on
 * without it. The client has reasonable defaults in place and can operate without
 * it. Note that there will still be a warning event generated if this is the case,
 * since it is definitely not expected.</p>
 *
 * @since 2.0.0
 */
@Stability.Internal
public class ErrorMapLoadingHandler extends ChannelDuplexHandler {

  /**
   * Right now we are at version 2 for the error map, so that's what we
   * negotiate as part of the process. Note that v1 and v2 are identical, the
   * version bump was needed since other SDKs would not consume "new" properties
   * as gracefully.
   */
  private static final short MAP_VERSION = 2;

  /**
   * Holds the core context as reference to event bus and more.
   */
  private final EndpointContext endpointContext;

  /**
   * Holds the timeout for the full error map loading phase.
   */
  private final Duration timeout;

  /**
   * Once connected, holds the io context for more debug information.
   */
  private IoContext ioContext;

  /**
   * Holds the intercepted promise from up the pipeline which is either
   * completed or failed depending on the downstream components or the
   * result of the error map negotiation.
   */
  private ChannelPromise interceptedConnectPromise;

  /**
   * Creates a new {@link ErrorMapLoadingHandler}.
   *
   * @param endpointContext the core context used to refer to values like the core id.
   */
  public ErrorMapLoadingHandler(final EndpointContext endpointContext) {
    this.endpointContext = endpointContext;
    this.timeout = endpointContext.environment().timeoutConfig().connectTimeout();
  }

  /**
   * Intercepts the connect process inside the pipeline to only propagate either
   * success or failure if the error map loading process is completed either way.
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
          new TimeoutException("KV Error Map loading timed out after "
            + timeout.toMillis() + "ms")
        );
      }
    }, timeout.toNanos(), TimeUnit.NANOSECONDS);
    ConnectTimings.start(ctx.channel(), this.getClass());
    ctx.writeAndFlush(buildErrorMapRequest(ctx));

    // Fire the channel active immediately so the upper handler in the pipeline gets a chance to
    // pipeline its request before the response of this one arrives. This helps speeding up the
    // bootstrap sequence.
    ctx.fireChannelActive();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    Optional<Duration> latency = ConnectTimings.stop(ctx.channel(), this.getClass(), false);

    if (msg instanceof ByteBuf) {
      if (successful((ByteBuf) msg)) {
        Optional<ErrorMap> loadedMap = extractErrorMap((ByteBuf) msg);
        loadedMap.ifPresent(errorMap -> ctx.channel().attr(ChannelAttributes.ERROR_MAP_KEY).set(errorMap));
        endpointContext.environment().eventBus().publish(
          new ErrorMapLoadedEvent(ioContext, latency.orElse(Duration.ZERO), loadedMap)
        );
      } else {
        endpointContext.environment().eventBus().publish(
          new ErrorMapLoadingFailedEvent(
            ioContext,
            latency.orElse(Duration.ZERO),
            status((ByteBuf) msg))
        );
      }
      interceptedConnectPromise.trySuccess();
      ctx.pipeline().remove(this);
    } else {
      interceptedConnectPromise.tryFailure(new CouchbaseException("Unexpected response "
        + "type on channel read, this is a bug - please report. " + msg));
    }

    ReferenceCountUtil.release(msg);
  }

  /**
   * Helper method to extract the error map from a successful response.
   *
   * @param msg the response to work with.
   * @return the parsed error map or none if an error happened.
   */
  private Optional<ErrorMap> extractErrorMap(final ByteBuf msg) {
    byte[] input = bodyAsBytes(msg);
    if (input.length != 0) {
      try {
        return Optional.of(ErrorMap.fromJson(input));
      } catch (IOException e) {
        endpointContext.environment().eventBus().publish(new ErrorMapUndecodableEvent(
          ioContext, e.getMessage(), new String(input, UTF_8)
        ));
        return Optional.empty();
      }
    } else {
      endpointContext.environment().eventBus().publish(new ErrorMapUndecodableEvent(
        ioContext, "No content in response", ""
      ));
      return Optional.empty();
    }
  }

  /**
   * Helper method to build the error map fetch request.
   *
   * @param ctx the {@link ChannelHandlerContext} for which the channel active operation is made.
   * @return the created request as a {@link ByteBuf}.
   */
  private ByteBuf buildErrorMapRequest(final ChannelHandlerContext ctx) {
    ByteBuf body = ctx.alloc().buffer(Short.BYTES).writeShort(MAP_VERSION);
    ByteBuf request = MemcacheProtocol.request(
      ctx.alloc(),
      MemcacheProtocol.Opcode.ERROR_MAP,
      noDatatype(),
      noPartition(),
      BaseKeyValueRequest.nextOpaque(),
      noCas(),
      noExtras(),
      noKey(),
      body);
    body.release();
    return request;
  }

}
