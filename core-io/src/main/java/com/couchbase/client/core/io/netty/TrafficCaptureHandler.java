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
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.TrafficCaptureFormat;
import com.couchbase.client.core.service.ServiceType;


/**
 * Similar to the netty LoggingHandler, but it dumps the traffic into the event bus for later
 * consumption instead of logging it right away.
 * <p>
 * Describes the traffic with {@link TrafficCaptureFormat}, which masks credentials.
 *
 * @since 2.0.0
 */
public class TrafficCaptureHandler extends ChannelDuplexHandler {

  private enum Direction {
    INBOUND,
    OUTBOUND,
  }

  private final EndpointContext endpointContext;
  private final EventBus eventBus;
  private IoContext ioContext;

  public TrafficCaptureHandler(final EndpointContext endpointContext) {
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
      eventBus.publish(new ReadTrafficCapturedEvent(ioContext, byteBufToString((ByteBuf) msg, Direction.INBOUND)));
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof ByteBuf) {
      eventBus.publish(new WriteTrafficCapturedEvent(ioContext, byteBufToString((ByteBuf) msg, Direction.OUTBOUND)));
    }
    ctx.write(msg, promise);
  }

  /**
   * Describes the buffer's readable bytes: a KV packet (the KV pipeline frames packets before this handler sees
   * them), or part of an HTTP request or response. Doesn't change the buffer.
   */
  private String byteBufToString(final ByteBuf msg, final Direction direction) {
    byte[] bytes = ByteBufUtil.getBytes(msg);
    if (endpointContext.serviceType() == ServiceType.KV) {
      KeyValuePacket packet;
      try {
        packet = KeyValuePacket.wrap(bytes, 0, bytes.length);
      } catch (IllegalArgumentException notOnePacket) {
        // unexpected, since traffic capture happens after frame decoding
        return TrafficCaptureFormat.hexDump(bytes);
      }
      return TrafficCaptureFormat.memcacheToString(packet);
    } else if (direction == Direction.OUTBOUND) {
      return TrafficCaptureFormat.httpRequestToString(bytes);
    } else {
      return TrafficCaptureFormat.hexDump(bytes);
    }
  }
}
