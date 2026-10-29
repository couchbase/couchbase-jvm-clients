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

package com.couchbase.client.core.io;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.util.UnsignedLEB128;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrafficCaptureFormatTest {

  private static final String PASSWORD = "s3cretPassw0rd";

  // ---- KV packets

  @Test
  void describesHeaderFieldsAndKeyWithCollectionId() {
    ByteBuf key = Unpooled.buffer();
    UnsignedLEB128.write(key, 0x1234);
    key.writeBytes("doc-key".getBytes(StandardCharsets.UTF_8));
    String described = describe(MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, Opcode.GET, (byte) 0,
      (short) 7, 99, 0, Unpooled.EMPTY_BUFFER, key, Unpooled.EMPTY_BUFFER));

    assertTrue(described.contains("Opcode             | 1      | 0x00 (GET)"), described);
    assertTrue(described.contains("VBucket            | 6-7    | 0x0007 (7)"), described);
    assertTrue(described.contains("Opaque             | 12-15  | 0x00000063"), described);
    assertTrue(described.contains(">> Key: doc-key (Collection ID: 0x1234)"), described); // redaction is off by default
  }

  @Test
  void omitsSaslBodyAndItsLength() {
    String described = describe(MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, Opcode.SASL_AUTH, (byte) 0,
      (short) 0, 1, 0, Unpooled.EMPTY_BUFFER, buf("PLAIN"), buf("\0user\0" + PASSWORD)));

    assertFalse(described.contains(PASSWORD), described);
    assertTrue(described.startsWith("(Hex dump omitted for SASL packet)"), described);
    assertTrue(described.contains("Total Body Length  | 8-11   | <masked>"), described);
    assertTrue(described.contains(">> Mechanism: PLAIN"), described);
    assertTrue(described.contains(">> Body: <masked>"), described);
  }

  @Test
  void describesResponseFramingExtras() {
    byte[] framing = {0x02, 0x00, 0x05}; // server duration
    ByteBuf packet = Unpooled.buffer()
      .writeByte(MemcacheProtocol.Magic.FLEXIBLE_RESPONSE.magic()).writeByte(Opcode.GET.opcode())
      .writeByte(framing.length).writeByte(0).writeByte(0).writeByte(0).writeShort(0)
      .writeInt(framing.length).writeInt(0).writeLong(0)
      .writeBytes(framing);
    String described = describe(packet);

    assertTrue(described.contains("Flex Extras Length | 2      | 0x03 (3)"), described);
    assertTrue(described.contains("Status             | 6-7    | 0x0000 (SUCCESS)"), described);
    assertTrue(described.contains(">> Flexible Extras: {\"serverDuration\":"), described);
  }

  @Test
  void describesOnlyTheHexDumpOfAnUnknownPacket() {
    byte[] packet = new byte[MemcacheProtocol.HEADER_SIZE];
    packet[0] = 0x55; // not a known magic
    String described = TrafficCaptureFormat.memcacheToString(KeyValuePacket.wrap(packet, 0, packet.length));
    assertFalse(described.contains("------ Field ------"), described);
  }

  private static String describe(ByteBuf packet) {
    byte[] bytes = ByteBufUtil.getBytes(packet);
    return TrafficCaptureFormat.memcacheToString(KeyValuePacket.wrap(bytes, 0, bytes.length));
  }

  // ---- Hex dumps

  @Test
  void hexDumpMatchesNettys() {
    for (int length : new int[]{0, 1, 15, 16, 17, 40, 300}) {
      byte[] bytes = new byte[length];
      for (int i = 0; i < length; i++) {
        bytes[i] = (byte) (i * 37);
      }
      assertEquals(ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(bytes)), TrafficCaptureFormat.hexDump(bytes), "length " + length);
    }
  }

  @Test
  void hexDumpRowsAreLabelledWithTheirBodyOffset() {
    String dump = TrafficCaptureFormat.hexDump(new byte[20], 0x2000);
    assertTrue(dump.contains("|00002000| 00"), dump);
    assertTrue(dump.contains("|00002010| 00"), dump);
    assertFalse(dump.contains("|00000000|"), dump);
  }

  private static ByteBuf buf(String s) {
    return Unpooled.copiedBuffer(s, StandardCharsets.UTF_8);
  }
}
