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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.io.UnknownServerPushRequestReceivedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInboundHandlerAdapter;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.SectionReader;
import com.couchbase.client.core.msg.kv.PortableKeyValueRequest;
import com.couchbase.client.core.topology.TopologyRevision;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.couchbase.client.core.util.CbStrings.emptyToNull;
import static java.util.Objects.requireNonNull;

public class ServerPushHandler extends ChannelInboundHandlerAdapter {
  private static final Logger log = LoggerFactory.getLogger(ServerPushHandler.class);

  private final EndpointContext endpointContext;

  public ServerPushHandler(EndpointContext endpointContext) {
    this.endpointContext = requireNonNull(endpointContext);
  }

  private static @Nullable TopologyRevision parseTopologyRevision(KeyValuePacket packet) {
    SectionReader extras = packet.extras();
    switch (extras.remaining()) {
      case 4:
        return new TopologyRevision(0, extras.readUnsignedInt());
      case 16:
        return new TopologyRevision(extras.readLong(), extras.readLong());
      default:
        log.debug("Unexpected CLUSTERMAP_CHANGE_NOTIFICATION extras length: {}", extras.remaining());
        return null;
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      if (isServerPush(buf)) {
        try {
          handleServerPush(endpointContext, buf);
          return;
        } finally {
          buf.release();
        }
      }
    }

    ctx.fireChannelRead(msg);
  }

  /**
   * Returns true if the packet is a request from the server (which {@link #handleServerPush} handles), not a response.
   * Also used by the KV endpoint that doesn't use Netty, during its handshake.
   */
  @Stability.Internal
  public static boolean isServerPush(ByteBuf packet) {
    return MemcacheProtocol.magic(packet) == MemcacheProtocol.Magic.SERVER_PUSH_REQUEST.magic();
  }

  /**
   * Like {@link #isServerPush(ByteBuf)}. Used by the KV endpoint that doesn't use Netty, once it's connected.
   */
  @Stability.Internal
  public static boolean isServerPush(KeyValuePacket packet) {
    return packet.magic() == MemcacheProtocol.Magic.SERVER_PUSH_REQUEST.magic();
  }

  /**
   * Handles a request from the server. Doesn't release the packet.
   * Also used by the KV endpoint that doesn't use Netty, during its handshake.
   */
  @Stability.Internal
  public static void handleServerPush(EndpointContext endpointContext, ByteBuf packet) {
    handleServerPush(endpointContext, PortableKeyValueRequest.packet(packet));
  }

  /**
   * Like {@link #handleServerPush(EndpointContext, ByteBuf)}. Used by the KV endpoint that doesn't use Netty,
   * once it's connected.
   */
  @Stability.Internal
  public static void handleServerPush(EndpointContext endpointContext, KeyValuePacket packet) {
    byte opcodeByte = packet.opcode();
    MemcacheProtocol.ServerPushOpcode opcode = MemcacheProtocol.ServerPushOpcode.of(opcodeByte);
    if (opcode == MemcacheProtocol.ServerPushOpcode.CLUSTERMAP_CHANGE_NOTIFICATION) {
      String bucketOrNullIfGlobal = emptyToNull(packet.keyAsString());
      TopologyRevision newRevision = parseTopologyRevision(packet);
      endpointContext.core().configurationProvider().signalNewTopologyAvailable(
        bucketOrNullIfGlobal,
        newRevision,
        endpointContext.remoteSocket()
      );
      return;
    }

    endpointContext.core().environment().eventBus().publish(
      new UnknownServerPushRequestReceivedEvent(null, packet.toByteArray())
    );
  }
}
