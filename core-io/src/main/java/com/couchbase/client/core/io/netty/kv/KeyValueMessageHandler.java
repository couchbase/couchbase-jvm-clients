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
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.Compressible;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import java.util.List;

/**
 * This handler is responsible for encoding KV requests and completing them once
 * a response arrives.
 *
 * @since 2.0.0
 */
public class KeyValueMessageHandler extends ChannelDuplexHandler {

  /**
   * Stores the current opaque value.
   */
  private int opaque;

  /**
   * If compression is enabled and should be used.
   */
  private boolean compressionEnabled;

  /**
   * Stores the {@link CoreContext} for use.
   */
  private final CoreContext coreContext;

  /**
   * Holds all outstanding requests based on their opaque.
   */
  private final IntObjectMap<KeyValueRequest> writtenRequests;

  /**
   * Holds the start timestamps for the outstanding dispatched requests.
   */
  private final IntObjectMap<Long> writtenRequestDispatchTimings;

  /**
   * The compression config used for this handler.
   */
  private final CompressionConfig compressionConfig;

  /**
   * The event bus used to signal events.
   */
  private final EventBus eventBus;

  /**
   * Creates a new {@link KeyValueMessageHandler}.
   *
   * @param coreContext the parent core context.
   */
  public KeyValueMessageHandler(final CoreContext coreContext) {
    this.coreContext = coreContext;
    this.writtenRequests = new IntObjectHashMap<>();
    this.writtenRequestDispatchTimings = new IntObjectHashMap<>();
    this.compressionConfig = coreContext.environment().ioEnvironment().compressionConfig();
    this.eventBus = coreContext.environment().eventBus();
  }

  /**
   * Actions to be performed when the channel becomes active.
   *
   * <p>Since the opaque is incremented in the handler below during bootstrap but now is
   * only modified in this handler, cache the reference since the attribute lookup is
   * more costly.</p>
   *
   * @param ctx the channel context.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    opaque = Utils.opaque(ctx.channel(), false);

    List<ServerFeature> features = ctx.channel().attr(ChannelAttributes.SERVER_FEATURE_KEY).get();
    compressionEnabled = features != null && features.contains(ServerFeature.SNAPPY);

    ctx.fireChannelActive();
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof KeyValueRequest) {
      KeyValueRequest request = (KeyValueRequest) msg;

      int nextOpaque = ++opaque;
      handleSameOpaqueRequest(writtenRequests.put(nextOpaque, request));

      if (compressionEnabled && request instanceof Compressible) {
        ctx.write(((Compressible) request).encode(ctx.alloc(), nextOpaque, compressionConfig));
      } else {
        ctx.write(request.encode(ctx.alloc(), nextOpaque));
      }
      writtenRequestDispatchTimings.put(nextOpaque, (Long) System.nanoTime());
    } else {
      // todo: terminate this channel and raise an event, this is not supposed to happen
    }
  }

  private void handleSameOpaqueRequest(final KeyValueRequest requestWithSameOpaque) {
    if (requestWithSameOpaque == null) {
      return;
    }

    // TODO: figure out what to do if there was already one request with the
    // TODO: same opaque.. likely the new one, before sending, needs to be
    // TODO: assigned a new opaque!
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof ByteBuf) {
      decode(ctx, (ByteBuf) msg);
    } else {
      // todo: ERROR!! something weird came back...
    }
  }

  /**
   * Main method to start dispatching the decode.
   *
   * @param ctx
   * @param response
   */
  private void decode(final ChannelHandlerContext ctx, final ByteBuf response) {
    int opaque = MemcacheProtocol.opaque(response);
    KeyValueRequest request = writtenRequests.remove(opaque);
    if (request == null) {
      // todo: this is a problem! no request found with the opaque for a given
      // todo: response.. server error? ignore the request and release its resources
      // todo: but raise event if this happens and keep going...
    }

    long start = writtenRequestDispatchTimings.remove(opaque);
    request.context().dispatchLatency(System.nanoTime() - start);

    Response decoded = request.decode(response);
    request.succeed(decoded);

    ReferenceCountUtil.release(response);
  }

}
