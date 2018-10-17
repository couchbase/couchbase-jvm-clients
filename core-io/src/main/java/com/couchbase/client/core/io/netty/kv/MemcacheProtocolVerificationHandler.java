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
import com.couchbase.client.core.cnc.events.io.InvalidPacketDetectedEvent;
import com.couchbase.client.core.io.IoContext;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.verifyRequest;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.verifyResponse;

/**
 * This handler makes sure that the KV protocol packets passed around
 * are not malformed.
 *
 * <p>This handler is purely here for defense in-depth against malformed
 * packets. For performance reasons, it only checks the basic structure
 * and length fields and cannot do any deep-packet inspection.</p>
 */
public class MemcacheProtocolVerificationHandler extends ChannelDuplexHandler {

  /**
   * Holds the core context as reference to event bus and more.
   */
  private final CoreContext coreContext;

  /**
   * Creates a new {@link MemcacheProtocolVerificationHandler}.
   *
   * @param coreContext the core context used to refer to values like the core id.
   */
  MemcacheProtocolVerificationHandler(final CoreContext coreContext) {
    this.coreContext = coreContext;
  }

  /**
   * Makes sure that outgoing requests are valid.
   *
   * @param ctx the context to use.
   * @param request the request to check.
   * @param promise the write promise.
   */
  @Override
  public void write(final ChannelHandlerContext ctx, final Object request,
                    final ChannelPromise promise) {
    if (request instanceof ByteBuf && !verifyRequest((ByteBuf) request)) {
      handleEventAndCloseChannel(ctx, (ByteBuf) request);
      return;
    }
    ctx.write(request, promise);
  }

  /**
   * Makes sure that incoming responses are valid.
   *
   * @param ctx the context to use.
   * @param response the response to check.
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object response) {
    if (response instanceof ByteBuf && !verifyResponse((ByteBuf) response)) {
      handleEventAndCloseChannel(ctx, (ByteBuf) response);
      return;
    }
    ctx.fireChannelRead(response);
  }

  /**
   * Helper method to send the event into the event bus and then close the
   * channel.
   *
   * @param ctx the context to use.
   * @param msg the msg to export.
   */
  private void handleEventAndCloseChannel(final ChannelHandlerContext ctx, final ByteBuf msg) {
    final IoContext ioContext = new IoContext(
      coreContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress()
    );

    byte[] packet = new byte[msg.readableBytes()];
    msg.readBytes(packet);
    ReferenceCountUtil.release(msg);

    coreContext.environment().eventBus().publish(new InvalidPacketDetectedEvent(
      ioContext,
      packet
    ));
    ctx.close();
  }

}
