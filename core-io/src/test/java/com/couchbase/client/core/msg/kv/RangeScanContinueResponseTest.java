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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.io.SectionReader;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.LastCoreRangeScanItem;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.UnsignedLEB128;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.List;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RangeScanContinueResponseTest {

  @Test
  void feedsKeys() {
    ByteBuf items = Unpooled.buffer();
    writeLengthPrefixed(items, "a".getBytes(UTF_8));
    writeLengthPrefixed(items, new byte[200]); // a length that takes two bytes
    RangeScanContinueResponse response = response(true);

    response.feedItems(body(items), true, true);

    List<CoreRangeScanItem> fed = response.items().collectList().block();
    assertEquals(3, fed.size());
    assertEquals("a", fed.get(0).key());
    assertEquals(200, fed.get(1).keyBytes().length);
    assertSame(LastCoreRangeScanItem.INSTANCE, fed.get(2));
  }

  @Test
  void feedsKeysAndBodies() {
    ByteBuf items = Unpooled.buffer();
    items.writeInt(7).writeInt(0xfffffffe).writeLong(8).writeLong(9).writeByte(0);
    writeLengthPrefixed(items, "a".getBytes(UTF_8));
    writeLengthPrefixed(items, "{}".getBytes(UTF_8));
    items.writeInt(1).writeInt(0).writeLong(2).writeLong(3).writeByte(0);
    writeLengthPrefixed(items, "b".getBytes(UTF_8));
    writeLengthPrefixed(items, new byte[0]);
    RangeScanContinueResponse response = response(false);

    response.feedItems(body(items), false, true);

    List<CoreRangeScanItem> fed = response.items().collectList().block();
    assertEquals(2, fed.size());
    CoreRangeScanItem a = fed.get(0);
    assertEquals(7, a.flags());
    assertEquals(Instant.ofEpochSecond(0xfffffffeL), a.expiry()); // unsigned
    assertEquals(8, a.seqno());
    assertEquals(9, a.cas());
    assertEquals("a", a.key());
    assertArrayEquals("{}".getBytes(UTF_8), a.value());
    CoreRangeScanItem b = fed.get(1);
    assertNull(b.expiry());
    assertEquals("b", b.key());
    assertEquals(0, b.value().length);
  }

  @Test
  void failsFeedIfItemIsTruncated() {
    ByteBuf items = Unpooled.buffer();
    UnsignedLEB128.write(items, 10);
    items.writeBytes(new byte[3]); // a key of 10 bytes, cut short
    RangeScanContinueResponse response = response(true);

    response.feedItems(body(items), false, true);

    assertThrows(IndexOutOfBoundsException.class, () -> response.items().blockLast());
  }

  private static RangeScanContinueResponse response(boolean keysOnly) {
    return new RangeScanContinueResponse(ResponseStatus.SUCCESS, Sinks.many().replay().all(), keysOnly);
  }

  private static void writeLengthPrefixed(ByteBuf buf, byte[] bytes) {
    UnsignedLEB128.write(buf, bytes.length);
    buf.writeBytes(bytes);
  }

  /**
   * Returns a reader of a response packet's body, like the KV implementations give {@link RangeScanContinueResponse#feedItems}.
   */
  private static SectionReader body(ByteBuf items) {
    ByteBuf packet = MemcacheProtocol.response(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.RANGE_SCAN_CONTINUE,
      noDatatype(), (short) 0, 1, noCas(), noExtras(), noKey(), items);
    return KeyValuePacket.wrap(ByteBufUtil.getBytes(packet), 0, packet.readableBytes()).body();
  }
}
