/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.IoContext;

/**
 * Similar to the netty LoggingHandler, but it dumps the traffic into the event bus for later
 * consumption instead of logging it right away.
 *
 * @since 2.0.0
 */
public class TrafficCaptureHandler extends ChannelDuplexHandler {

  private final EndpointContext endpointContext;
  private final EventBus eventBus;
  private IoContext ioContext;

  public TrafficCaptureHandler(EndpointContext endpointContext) {
    this.endpointContext = endpointContext;
    this.eventBus = endpointContext.environment().eventBus();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    ioContext = new IoContext(
      endpointContext,
      ctx.channel().localAddress(),
      ctx.channel().remoteAddress(),
      endpointContext.bucket()
    );

    ctx.fireChannelActive();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    if (msg instanceof ByteBuf) {
      eventBus.publish(new ReadTrafficCapturedEvent(ioContext, ByteBufUtil.prettyHexDump((ByteBuf) msg)));
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof ByteBuf) {
      eventBus.publish(new WriteTrafficCapturedEvent(ioContext, ByteBufUtil.prettyHexDump((ByteBuf) msg)));
    }
    ctx.write(msg, promise);
  }

}
