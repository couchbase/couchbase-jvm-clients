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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.io.KeyValueRequestBuilder.HeaderStage;
import org.junit.jupiter.api.Test;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NettyKeyValueRequestEncoderTest {
  private static final byte[] KEY = "key".getBytes(UTF_8);
  private final NettyKeyValueRequestEncoder encoder = new NettyKeyValueRequestEncoder(UnpooledByteBufAllocator.DEFAULT);

  private ByteBuf encodeSet(byte[] value, int valueLength) {
    HeaderStage request = encoder.beginRequest(MemcacheProtocol.Opcode.SET, 99)
      .datatype((byte) 1)
      .partition((short) 7)
      .cas(5);
    request.startExtras().putInt(0xcafe);
    request.startKey().putBytes(KEY);
    request.value(value, valueLength);
    return encoder.finish();
  }

  @Test
  void encodesLikeMemcacheProtocol() {
    byte[] value = "value-and-garbage".getBytes(UTF_8);
    ByteBuf actual = encodeSet(value, 5);

    ByteBuf expected = MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.SET, (byte) 1,
      (short) 7, 99, 5, Unpooled.buffer().writeInt(0xcafe), Unpooled.wrappedBuffer(KEY), Unpooled.wrappedBuffer(value, 0, 5));
    assertArrayEquals(ByteBufUtil.getBytes(expected), ByteBufUtil.getBytes(actual));
  }

  @Test
  void isReusable() {
    byte[] first = ByteBufUtil.getBytes(encodeSet("one".getBytes(UTF_8), 3));
    encoder.beginRequest(MemcacheProtocol.Opcode.GET, 1).startKey().putBytes("abandoned".getBytes(UTF_8));
    encoder.abort();
    byte[] again = ByteBufUtil.getBytes(encodeSet("one".getBytes(UTF_8), 3));
    assertArrayEquals(first, again);
  }

  @Test
  void refusesToFinishWhatWasNotBegun() {
    assertThrows(IllegalStateException.class, encoder::finish);
  }

  @Test
  void refusesScratchOrSecondRequestAfterBegin() {
    encoder.beginRequest(MemcacheProtocol.Opcode.GET, 1);
    assertThrows(IllegalStateException.class, () -> encoder.scratch(1));
    assertThrows(IllegalStateException.class, () -> encoder.beginRequest(MemcacheProtocol.Opcode.GET, 1));
  }

  @Test
  void refusesSectionTooLongForHeader() {
    encoder.beginRequest(MemcacheProtocol.Opcode.GET, 1).startExtras().putBytes(new byte[256]);
    assertThrows(IllegalStateException.class, encoder::finish);
  }

  @Test
  void reusesScratchUpToLimit() {
    byte[] scratch = encoder.scratch(100);
    assertSame(scratch, encoder.scratch(50));
    byte[] huge = encoder.scratch(1024 * 1024);
    assertNotSame(huge, encoder.scratch(1024 * 1024));
  }
}
