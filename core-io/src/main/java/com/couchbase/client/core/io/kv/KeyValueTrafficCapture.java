/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.io.kv;

import com.couchbase.client.core.cnc.events.io.ReadTrafficCapturedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.Bytes;
import com.couchbase.client.core.util.UnsignedLEB128;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static com.couchbase.client.core.io.TrafficCaptureMasking.MASKED;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.util.Objects.requireNonNull;

/**
 * Publishes each packet a KV connection writes and reads, if the config asks for KV's
 * (io.servicesToCapture), described by {@link #memcacheToString}.
 * <p>
 * Credentials are masked: the bodies of SASL packets are never dumped (not even their length).
 * Document keys and hex dumps are tagged as user data for log redaction.
 */
@NullMarked
final class KeyValueTrafficCapture {
  private final IoContext ioContext;
  private final boolean enabled;

  KeyValueTrafficCapture(IoContext ioContext) {
    this.ioContext = requireNonNull(ioContext);
    this.enabled = ioContext.environment().ioConfig().servicesToCapture().contains(ServiceType.KV);
  }

  boolean enabled() {
    return enabled;
  }

  /**
   * Publishes the packet about to be written.
   */
  void written(byte[] packet) {
    if (enabled) {
      String description = memcacheToString(KeyValuePacket.wrap(packet, 0, packet.length));
      ioContext.environment().eventBus().publish(new WriteTrafficCapturedEvent(ioContext, description));
    }
  }

  /**
   * Publishes the packet just read.
   */
  void read(KeyValuePacket packet) {
    if (enabled) {
      String description = memcacheToString(packet);
      ioContext.environment().eventBus().publish(new ReadTrafficCapturedEvent(ioContext, description));
    }
  }

  /**
   * Describes a KV packet: a hex dump (tagged as user data), then its header fields and key.
   */
  static String memcacheToString(KeyValuePacket packet) {
    byte[] bytes = packet.toByteArray();
    byte magicByte = packet.magic();
    MemcacheProtocol.Magic magic = MemcacheProtocol.Magic.of(magicByte);
    byte opcode = packet.opcode();

    // SASL bodies carry credentials. Skip the hex dump entirely, since even the
    // body length would reveal the length of a PLAIN password.
    boolean sasl = magic != null && !magic.isServerPush() && isSasl(opcode);

    StringBuilder sb = new StringBuilder();
    if (sasl) {
      sb.append("(Hex dump omitted for SASL packet)");
    } else {
      sb.append(redactUser(Bytes.prettyHexDump(bytes)));
    }

    if (magic != null) {
      sb.append("\n\n");
      sb.append("------ Field ------+ Offset +--- Value ---\n");
      sb.append("Magic              | 0      | 0x").append(String.format("%02X %s\n", magicByte,
        emptyIfNull(magic)));

      sb.append("Opcode             | 1      | 0x").append(String.format("%02X %s\n", opcode,
        emptyIfNull(magic, it -> it.isServerPush() ? MemcacheProtocol.ServerPushOpcode.of(opcode) : MemcacheProtocol.Opcode.of(opcode))));

      int keyLength = packet.keyLength();
      if (packet.isFlexible()) {
        int framingExtrasLength = packet.framingExtrasLength();
        sb.append("Flex Extras Length | 2      | 0x").append(String.format("%02X (%d)\n",
          framingExtrasLength, framingExtrasLength));
        sb.append("Key Length         | 3      | 0x").append(String.format("%02X (%d)\n", keyLength, keyLength));
      } else {
        sb.append("Key Length         | 2-3    | 0x").append(String.format("%04X (%d)\n", keyLength, keyLength));
      }

      int extrasLength = packet.extrasLength();
      sb.append("Extras Length      | 4      | 0x").append(String.format("%02X (%d)\n", extrasLength, extrasLength));

      byte datatype = packet.datatype();
      sb.append("Datatype           | 5      | 0x").append(String.format("%02X (%s)\n", datatype,
        MemcacheProtocol.Datatype.decode(datatype)));

      short status = packet.status();
      if (isRequest(magicByte)) {
        sb.append("VBucket            | 6-7    | 0x").append(String.format("%04X (%d)\n", status, status));
      } else {
        sb.append("Status             | 6-7    | 0x").append(String.format("%04X %s\n", status,
          emptyIfNull(MemcacheProtocol.Status.of(status))));
      }

      if (sasl) {
        sb.append("Total Body Length  | 8-11   | ").append(MASKED).append("\n");
      } else {
        int totalBodyLength = packet.size() - MemcacheProtocol.HEADER_SIZE;
        sb.append("Total Body Length  | 8-11   | 0x").append(String.format("%08X (%d)\n",
          totalBodyLength, totalBodyLength));
      }

      sb.append("Opaque             | 12-15  | 0x").append(String.format("%08X\n", packet.opaque()));
      sb.append("CAS                | 16-23  | 0x").append(String.format("%016X\n", packet.cas()));
      sb.append("----- Payload -----+--------+-------------\n");

      // Only a response's framing extras are described.
      if (magic == MemcacheProtocol.Magic.FLEXIBLE_RESPONSE) {
        MemcacheProtocol.FlexibleExtras extras = packet.flexibleExtras();
        if (extras != null) {
          Map<String, Object> flexExtras = new LinkedHashMap<>();
          extras.injectExportableParams(flexExtras);
          sb.append(">> Flexible Extras: ").append(Mapper.encodeAsString(flexExtras)).append("\n");
        }
      }

      if (keyLength > 0) {
        int keyOffset = MemcacheProtocol.HEADER_SIZE + packet.framingExtrasLength() + extrasLength;
        byte[] key = Arrays.copyOfRange(bytes, keyOffset, keyOffset + keyLength);
        if (sasl) {
          // For SASL packets the key is the mechanism name, not a document key.
          sb.append(">> Mechanism: ").append(new String(key, StandardCharsets.UTF_8)).append("\n");
        } else {
          appendKey(sb, key);
        }
      }
      if (sasl) {
        sb.append(">> Body: ").append(MASKED).append("\n");
      }
      sb.append("-------------------+--------+-------------\n");
    }
    sb.append("\n");

    return sb.toString();
  }

  /**
   * Appends the key, and its collection ID if it has one (as a prefix, unsigned LEB128-encoded). Assumes it has one
   * if the key starts with a complete LEB128 value.
   */
  private static void appendKey(StringBuilder sb, byte[] key) {
    int prefixLength = 0;
    while (prefixLength < key.length && (key[prefixLength] & 0x80) != 0) {
      prefixLength++;
    }
    prefixLength = Math.min(prefixLength + 1, key.length);

    sb.append(">> Key: ")
      .append(redactUser(new String(key, prefixLength, key.length - prefixLength, StandardCharsets.UTF_8)));
    if (prefixLength > 0) {
      long collectionId = UnsignedLEB128.decode(Arrays.copyOf(key, prefixLength));
      sb.append(" (Collection ID: 0x").append(Integer.toHexString((int) collectionId)).append(")");
    }
    sb.append("\n");
  }

  private static boolean isRequest(byte magic) {
    return magic == MemcacheProtocol.Magic.FLEXIBLE_REQUEST.magic() || magic == MemcacheProtocol.Magic.REQUEST.magic();
  }

  private static boolean isSasl(byte opcode) {
    return opcode == MemcacheProtocol.Opcode.SASL_AUTH.opcode()
      || opcode == MemcacheProtocol.Opcode.SASL_STEP.opcode();
  }

  private static String emptyIfNull(@Nullable Object object) {
    return object == null ? "" : "(" + object + ")";
  }

  private static <T> String emptyIfNull(@Nullable T object, Function<T, @Nullable Object> transform) {
    return object == null
      ? ""
      : emptyIfNull(transform.apply(object));
  }
}
