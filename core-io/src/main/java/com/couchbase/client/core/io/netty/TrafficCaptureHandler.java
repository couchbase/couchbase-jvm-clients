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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
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
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.couchbase.client.core.io.TrafficCaptureMasking.MASKED;
import static com.couchbase.client.core.io.TrafficCaptureMasking.maskSecretFields;
import static com.couchbase.client.core.io.TrafficCaptureMasking.replaceAll;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * Similar to the netty LoggingHandler, but it dumps the traffic into the event bus for later
 * consumption instead of logging it right away.
 * <p>
 * Credentials are masked: the bodies of KV SASL packets are never dumped (not even their length).
 * In outbound HTTP requests, the values of the {@code Authorization} and {@code Content-Length}
 * headers and the values of known secret fields in form-encoded and JSON request bodies are
 * replaced with a placeholder.
 * Document keys and KV hex dumps are tagged as user data for log redaction.
 *
 * @since 2.0.0
 */
public class TrafficCaptureHandler extends ChannelDuplexHandler {

  /**
   * Matches the value of an HTTP Authorization header. Group 1 is the header name plus the
   * auth scheme (if present), which are kept; the rest of the line (the credential) is masked.
   */
  private static final Pattern AUTHORIZATION_HEADER = Pattern.compile(
    "(?im)^(authorization[ \\t]*:[ \\t]*(?:[!#$%&'*+.^_`|~0-9a-z-]+[ \\t]+)?)[^\\r\\n]*"
  );

  /**
   * Matches the value of an HTTP Content-Length header. Masked on every outbound request because
   * when a body field is masked, the original length would reveal the length of the secret.
   * Nothing is lost: an unmasked body is dumped in full, and its length is visible in the dump.
   */
  private static final Pattern CONTENT_LENGTH_HEADER = Pattern.compile(
    "(?im)^(content-length[ \\t]*:[ \\t]*)[^\\r\\n]*"
  );

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

  private String byteBufToString(final ByteBuf msg, final Direction direction) {
    if (endpointContext.serviceType() == ServiceType.KV) {
      return memcacheToString(msg);
    } else if (direction == Direction.OUTBOUND) {
      return httpRequestToString(msg);
    } else {
      return ByteBufUtil.prettyHexDump(msg);
    }
  }

  /**
   * Hex dumps an outbound HTTP buffer, masking the credential in any Authorization header,
   * the value of any Content-Length header, and the values of known secret fields in the
   * request body.
   * <p>
   * The SDK only sends full HTTP requests. Netty's HTTP encoder writes the request line and
   * all headers into a single buffer, and writes the body either into that same buffer or
   * into one separate buffer. So neither the headers nor the body are split across buffers.
   * The original buffer is not modified.
   */
  static String httpRequestToString(final ByteBuf msg) {
    // ISO-8859-1 maps each byte to exactly one char, so the masked string converts back
    // to the original bytes everywhere except where something was masked.
    String original = msg.toString(StandardCharsets.ISO_8859_1);

    String masked = replaceAll(AUTHORIZATION_HEADER, original, "$1" + MASKED);
    masked = replaceAll(CONTENT_LENGTH_HEADER, masked, "$1" + MASKED);
    masked = maskSecretFields(masked);

    if (masked == original) {
      return ByteBufUtil.prettyHexDump(msg);
    }
    return ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(masked.getBytes(StandardCharsets.ISO_8859_1)));
  }

  private static boolean isSasl(final byte opcode) {
    return opcode == MemcacheProtocol.Opcode.SASL_AUTH.opcode()
      || opcode == MemcacheProtocol.Opcode.SASL_STEP.opcode();
  }

  /**
   * Describes a KV packet for a traffic capture event: a hex dump (tagged as user data), then its header fields
   * and key. Doesn't change the buffer. Also used by the KV endpoint that doesn't use Netty.
   */
  @Stability.Internal
  public static String memcacheToString(final ByteBuf msg) {
    byte magicByte = MemcacheProtocol.magic(msg);
    MemcacheProtocol.Magic magic = MemcacheProtocol.Magic.of(magicByte);

    // SASL bodies carry credentials. Skip the hex dump entirely, since even the
    // body length would reveal the length of a PLAIN password.
    boolean sasl = magic != null && !magic.isServerPush() && isSasl(MemcacheProtocol.opcode(msg));

    StringBuilder sb = new StringBuilder();
    if (sasl) {
      sb.append("(Hex dump omitted for SASL packet)");
    } else {
      sb.append(redactUser(ByteBufUtil.prettyHexDump(msg)));
    }

    if (magic != null) {
      sb.append("\n\n");
      sb.append("------ Field ------+ Offset +--- Value ---\n");
      sb.append("Magic              | 0      | 0x").append(String.format("%02X %s\n", magicByte,
        emptyIfNull(magic)));

      byte opcode = MemcacheProtocol.opcode(msg);
      sb.append("Opcode             | 1      | 0x").append(String.format("%02X %s\n", opcode,
        emptyIfNull(magic, it -> it.isServerPush() ? MemcacheProtocol.ServerPushOpcode.of(opcode) : MemcacheProtocol.Opcode.of(opcode))));

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

      if (sasl) {
        sb.append("Total Body Length  | 8-11   | ").append(MASKED).append("\n");
      } else {
        int totalBodyLength = MemcacheProtocol.totalBodyLength(msg);
        sb.append("Total Body Length  | 8-11   | 0x").append(String.format("%08X (%d)\n",
          totalBodyLength, totalBodyLength));
      }

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
          if (sasl) {
            // For SASL packets the key is the mechanism name, not a document key.
            sb.append(">> Mechanism: ").append(buf.toString(StandardCharsets.UTF_8)).append("\n");
            return;
          }
          int skipped = UnsignedLEB128.skip(buf);
          int keyLen = buf.readableBytes();
          sb.append(">> Key: ")
            .append(redactUser(buf.toString(skipped, keyLen, StandardCharsets.UTF_8)));
          if (skipped > 0) {
            long leb = UnsignedLEB128.read(buf.slice(0, skipped));
            sb.append(" (Collection ID: 0x").append(Integer.toHexString((int) leb)).append(")");
          }
          sb.append("\n");
        });
      if (sasl) {
        sb.append(">> Body: ").append(MASKED).append("\n");
      }
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

  private static <T> String emptyIfNull(T object, Function<T, Object> transform) {
    return object == null
      ? ""
      : emptyIfNull(transform.apply(object));
  }

}
