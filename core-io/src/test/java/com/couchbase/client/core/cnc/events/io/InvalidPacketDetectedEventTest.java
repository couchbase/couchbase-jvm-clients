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

package com.couchbase.client.core.cnc.events.io;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.io.IoContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

class InvalidPacketDetectedEventTest {

  private static byte[] request(Opcode opcode, String key, String body) {
    return ByteBufUtil.getBytes(MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, opcode, (byte) 0, (short) 0, 1, 0,
      Unpooled.EMPTY_BUFFER, Unpooled.copiedBuffer(key, UTF_8), Unpooled.copiedBuffer(body, UTF_8)));
  }

  @Test
  void keepsOrdinaryPacket() {
    byte[] packet = request(Opcode.GET, "key", "");
    assertArrayEquals(packet, new InvalidPacketDetectedEvent(mock(IoContext.class), packet).packet());
  }

  @Test
  void leavesOutSaslBodyAndItsLength() {
    for (Opcode opcode : new Opcode[]{Opcode.SASL_AUTH, Opcode.SASL_STEP}) {
      byte[] packet = request(opcode, "PLAIN", "\0user\0password");
      InvalidPacketDetectedEvent event = new InvalidPacketDetectedEvent(mock(IoContext.class), packet);

      byte[] expected = Arrays.copyOf(packet, MemcacheProtocol.HEADER_SIZE);
      Arrays.fill(expected, 8, 12, (byte) 0);
      assertArrayEquals(expected, event.packet());
      assertFalse(event.description().contains("password"));
    }
  }

  @Test
  void keepsShortPacket() {
    byte[] packet = {MemcacheProtocol.Magic.REQUEST.magic()};
    assertEquals(1, new InvalidPacketDetectedEvent(mock(IoContext.class), packet).packet().length);
  }
}
