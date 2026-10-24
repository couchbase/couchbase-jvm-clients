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
import com.couchbase.client.core.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NettyKeyValueRequestEncoderTest {
  private static final byte[] KEY = "key".getBytes(UTF_8);
  private final NettyKeyValueRequestEncoder encoder = new NettyKeyValueRequestEncoder(
    UnpooledByteBufAllocator.DEFAULT,
    new KeyValueChannelContext(null, Optional.empty(), new CollectionMap(), null, EnumSet.noneOf(ServerFeature.class))
  );

  private ByteBuf encodeSet(byte[] value, int valueLength) {
    KeyValueRequestBuilder request = encoder.beginRequest(Opcode.SET)
      .addDatatype(1)
      .partition(7)
      .cas(5);
    request.key(KEY).value(value, valueLength);
    request.extras().putInt(0xcafe);
    return encoder.finish(99);
  }

  @Test
  void encodesLikeMemcacheProtocol() {
    byte[] value = "value-and-garbage".getBytes(UTF_8);
    ByteBuf actual = encodeSet(value, 5);

    ByteBuf expected = MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, Opcode.SET, (byte) 1,
      (short) 7, 99, 5, Unpooled.buffer().writeInt(0xcafe), Unpooled.wrappedBuffer(KEY), Unpooled.wrappedBuffer(value, 0, 5));
    assertArrayEquals(ByteBufUtil.getBytes(expected), ByteBufUtil.getBytes(actual));
  }

  @Test
  void isReusable() {
    byte[] first = ByteBufUtil.getBytes(encodeSet("one".getBytes(UTF_8), 3));
    encoder.beginRequest(Opcode.GET).extras().putBytes("abandoned".getBytes(UTF_8));
    encoder.abort();
    byte[] again = ByteBufUtil.getBytes(encodeSet("one".getBytes(UTF_8), 3));
    assertArrayEquals(first, again);
  }

  @Test
  void refusesToFinishWhatWasNotBegun() {
    assertThrows(IllegalStateException.class, () -> encoder.finish(1));
  }

  @Test
  void refusesSecondRequestAfterBegin() {
    encoder.beginRequest(Opcode.GET);
    assertThrows(IllegalStateException.class, () -> encoder.beginRequest(Opcode.GET));
  }

  @Test
  void refusesSectionTooLongForHeader() {
    encoder.beginRequest(Opcode.GET).extras().putBytes(new byte[256]);
    assertThrows(IllegalStateException.class, () -> encoder.finish(1));
  }

  @Test
  void valueWriterBuildsTheSameValueAsGivingIt() {
    byte[] value = new byte[100_000]; // bigger than the value writer array that's kept
    Arrays.fill(value, (byte) 'v');

    encoder.beginRequest(Opcode.SET).partition(7).key(KEY).value(value);
    byte[] given = bytes(encoder.finish(1));

    KeyValueRequestBuilder request = encoder.beginRequest(Opcode.SET).partition(7).key(KEY);
    request.valueWriter().putBytes(Arrays.copyOf(value, 40_000)).putBytes(Arrays.copyOfRange(value, 40_000, value.length));
    assertArrayEquals(given, bytes(encoder.finish(1)));
  }

  @Test
  void putsSaslRequestsInUnpooledBuffers() {
    NettyKeyValueRequestEncoder pooled = new NettyKeyValueRequestEncoder(
      PooledByteBufAllocator.DEFAULT,
      new KeyValueChannelContext(null, Optional.empty(), new CollectionMap(), null, EnumSet.noneOf(ServerFeature.class))
    );
    for (Opcode opcode : new Opcode[]{Opcode.SASL_AUTH, Opcode.SASL_STEP}) {
      pooled.beginRequest(opcode).key(KEY).value("password".getBytes(UTF_8));
      ByteBuf encoded = pooled.finish(1);
      try {
        assertSame(UnpooledByteBufAllocator.DEFAULT, encoded.alloc(), opcode.toString());
      } finally {
        encoded.release();
      }
    }

    pooled.beginRequest(Opcode.GET).key(KEY);
    ByteBuf encoded = pooled.finish(1);
    try {
      assertSame(PooledByteBufAllocator.DEFAULT, encoded.alloc());
    } finally {
      encoded.release();
    }
  }

  private static byte[] bytes(ByteBuf encoded) {
    try {
      return ByteBufUtil.getBytes(encoded);
    } finally {
      encoded.release();
    }
  }
}
