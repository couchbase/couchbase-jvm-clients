/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BytesTest {

  @Test
  void toHexUsesTwoLowercaseDigitsPerByte() {
    assertEquals("", Bytes.toHex(Bytes.EMPTY_BYTE_ARRAY));
    assertEquals("00ff10", Bytes.toHex(new byte[]{0x00, (byte) 0xff, 0x10}));
  }

  @Test
  void toHexMatchesNettysHexDump() {
    byte[] everyByte = new byte[256];
    for (int i = 0; i < everyByte.length; i++) {
      everyByte[i] = (byte) i;
    }
    assertEquals(ByteBufUtil.hexDump(everyByte), Bytes.toHex(everyByte));
  }


  @Test
  void hexDumpMatchesNettys() {
    for (int length : new int[]{0, 1, 15, 16, 17, 40, 300}) {
      byte[] bytes = new byte[length];
      for (int i = 0; i < length; i++) {
        bytes[i] = (byte) (i * 37);
      }
      assertEquals(ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(bytes)), Bytes.prettyHexDump(bytes), "length " + length);
    }
  }

  @Test
  void hexDumpRowsAreLabelledWithTheirBodyOffset() {
    String dump = Bytes.prettyHexDump(new byte[20], 0x2000);
    assertTrue(dump.contains("|00002000| 00"), dump);
    assertTrue(dump.contains("|00002010| 00"), dump);
    assertFalse(dump.contains("|00000000|"), dump);
  }

  private static ByteBuf buf(String s) {
    return Unpooled.copiedBuffer(s, StandardCharsets.UTF_8);
  }
}
