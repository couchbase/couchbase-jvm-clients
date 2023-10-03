/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.cnc.events.io.UnknownServerPushRequestReceivedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInboundHandlerAdapter;
import com.couchbase.client.core.endpoint.EndpointContext;

import static java.util.Objects.requireNonNull;

public class ServerPushHandler extends ChannelInboundHandlerAdapter {
  private final EndpointContext endpointContext;

  public ServerPushHandler(EndpointContext endpointContext) {
    this.endpointContext = requireNonNull(endpointContext);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      if (MemcacheProtocol.magic(buf) == MemcacheProtocol.Magic.SERVER_PUSH_REQUEST.magic()) {
        try {
          byte opcodeByte = MemcacheProtocol.opcode(buf);
          MemcacheProtocol.ServerPushOpcode opcode = MemcacheProtocol.ServerPushOpcode.of(opcodeByte);

          if (opcode == MemcacheProtocol.ServerPushOpcode.CLUSTERMAP_CHANGE_NOTIFICATION) {
            this.endpointContext.core().configurationProvider().signalConfigChanged();
            return;
          }

          endpointContext.core().environment().eventBus().publish(
            new UnknownServerPushRequestReceivedEvent(null, ByteBufUtil.getBytes(buf))
          );
          return;

        } finally {
          buf.release();
        }
      }
    }

    ctx.fireChannelRead(msg);
  }
}
