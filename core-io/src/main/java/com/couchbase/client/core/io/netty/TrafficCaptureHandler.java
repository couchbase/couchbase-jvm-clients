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
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.UnsignedLEB128;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

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
      eventBus.publish(new ReadTrafficCapturedEvent(ioContext, byteBufToString((ByteBuf) msg)));
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
    if (msg instanceof ByteBuf) {
      eventBus.publish(new WriteTrafficCapturedEvent(ioContext, byteBufToString((ByteBuf) msg)));
    }
    ctx.write(msg, promise);
  }

  private String byteBufToString(final ByteBuf msg) {
    if (endpointContext.serviceType() == ServiceType.KV) {
      return memcacheToString(msg);
    } else {
      return ByteBufUtil.prettyHexDump(msg);
    }
  }

  private String memcacheToString(final ByteBuf msg) {
    StringBuilder sb = new StringBuilder(ByteBufUtil.prettyHexDump(msg));

    byte magic = MemcacheProtocol.magic(msg);
    if (MemcacheProtocol.Magic.of(magic) != null) {
      sb.append("\n\n");
      sb.append("------ Field ------+ Offset +--- Value ---\n");
      sb.append("Magic              | 0      | 0x").append(String.format("%02X %s\n", magic,
        emptyIfNull(MemcacheProtocol.Magic.of(magic))));

      byte opcode = MemcacheProtocol.opcode(msg);
      sb.append("Opcode             | 1      | 0x").append(String.format("%02X %s\n", opcode,
        emptyIfNull(MemcacheProtocol.Opcode.of(opcode))));

      short keyLength = MemcacheProtocol.keyLength(msg);
      if (MemcacheProtocol.isFlexible(msg)) {
        byte flexExtrasLength = MemcacheProtocol.flexExtrasLength(msg);
        sb.append("Flex Extras Length | 2      | 0x").append(String.format("%02X (%d)\n",
          flexExtrasLength, flexExtrasLength));
        sb.append("Key Length         | 3      | 0x").append(String.format("%02X (%d)\n", keyLength, keyLength));
      } else {
        sb.append("Key Length         | 2-3    | 0x").append(String.format("%04X (%d)\n", keyLength, keyLength));
      }

      byte extrasLength = MemcacheProtocol.extrasLength(msg);
      sb.append("Extras Length      | 4      | 0x").append(String.format("%02X (%d)\n", extrasLength, extrasLength));

      byte datatype = MemcacheProtocol.datatype(msg);
      sb.append("Datatype           | 5      | 0x").append(String.format("%02X (%s)\n", datatype,
        MemcacheProtocol.Datatype.decode(datatype)));

      short status = MemcacheProtocol.status(msg);
      if (MemcacheProtocol.isRequest(msg)) {
        sb.append("VBucket            | 6-7    | 0x").append(String.format("%04X (%d)\n", status, status));
      } else {
        sb.append("Status             | 6-7    | 0x").append(String.format("%04X %s\n", status,
          emptyIfNull(MemcacheProtocol.Status.of(status))));
      }

      int totalBodyLength = MemcacheProtocol.totalBodyLength(msg);
      sb.append("Total Body Length  | 8-11   | 0x").append(String.format("%08X (%d)\n",
        totalBodyLength, totalBodyLength));

      int opaque = MemcacheProtocol.opaque(msg);
      sb.append("Opaque             | 12-15  | 0x").append(String.format("%08X\n", opaque));

      long cas = MemcacheProtocol.cas(msg);
      sb.append("CAS                | 16-23  | 0x").append(String.format("%016X\n", cas));
      sb.append("----- Payload -----+--------+-------------\n");

      if (MemcacheProtocol.isFlexible(msg)) {
        MemcacheProtocol.FlexibleExtras extras = MemcacheProtocol.flexibleExtras(msg);
        if (extras != null) {
          Map<String, Object> flexExtras = new LinkedHashMap<>();
          extras.injectExportableParams(flexExtras);
          sb.append(">> Flexible Extras: ").append(Mapper.encodeAsString(flexExtras)).append("\n");
        }
      }

      MemcacheProtocol
        .key(msg)
        .ifPresent(buf -> {
          int skipped = UnsignedLEB128.skip(buf);
          int keyLen = buf.readableBytes();
          sb.append(">> Key: ")
            .append(buf.toString(skipped, keyLen, StandardCharsets.UTF_8));
          if (skipped > 0) {
            long leb = UnsignedLEB128.read(buf.slice(0, skipped));
            sb.append(" (Collection ID: 0x").append(Integer.toHexString((int) leb)).append(")");
          }
          sb.append("\n");
        });
      sb.append("-------------------+--------+-------------\n");
    }
    sb.append("\n");

    return sb.toString();
  }

  private static String emptyIfNull(Object object) {
    if (object == null) {
      return "";
    } else {
      return "(" + object + ")";
    }
  }

}
