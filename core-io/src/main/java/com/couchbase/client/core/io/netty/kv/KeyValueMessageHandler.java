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
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import java.util.Map;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;

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
   * Stores the {@link CoreContext} for use.
   */
  private final CoreContext coreContext;

  /**
   * Holds all outstanding requests based on their opaque.
   */
  private final IntObjectMap<KeyValueRequest> writtenRequests;

  public KeyValueMessageHandler(final CoreContext coreContext) {
    this.coreContext = coreContext;
    this.writtenRequests = new IntObjectHashMap<>();
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
    ctx.fireChannelActive();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (msg instanceof KeyValueRequest) {
      KeyValueRequest request = (KeyValueRequest) msg;

      int nextOpaque = ++opaque;
      handleSameOpaqueRequest(writtenRequests.put(nextOpaque, request));
      ctx.write(request.encode(ctx.alloc(), nextOpaque));
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
    Response decoded = request.decode(response);
    request.succeed(decoded);

    ReferenceCountUtil.release(response);
  }

}
