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

import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol.noKey;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueFrameDecoderTest {

  private static byte[] packet(int opaque, int bodySize) {
    byte[] body = new byte[bodySize];
    Arrays.fill(body, (byte) opaque);
    ByteBuf buf = NettyMemcacheProtocol.response(
      Unpooled.buffer().alloc(), MemcacheProtocol.Opcode.GET, noDatatype(), (short) 0, opaque, noCas(),
      noExtras(), noKey(), Unpooled.wrappedBuffer(body)
    );
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.write(part, 0, part.length);
    }
    return out.toByteArray();
  }

  /**
   * Returns the data in reads of at most the given size each.
   */
  private static ReadableByteChannel chunked(byte[] data, int maxReadSize) {
    return new ReadableByteChannel() {
      int pos = 0;

      @Override
      public int read(ByteBuffer dst) {
        if (pos == data.length) {
          return -1;
        }
        int n = Math.min(Math.min(dst.remaining(), maxReadSize), data.length - pos);
        dst.put(data, pos, n);
        pos += n;
        return n;
      }

      @Override
      public boolean isOpen() {
        return true;
      }

      @Override
      public void close() {
      }
    };
  }

  /**
   * Reads every packet, copying each one (since they're only valid until the next fill).
   */
  private static List<byte[]> readAll(KeyValueFrameDecoder decoder, ReadableByteChannel in) throws Exception {
    List<byte[]> packets = new ArrayList<>();
    while (true) {
      KeyValuePacket packet;
      while ((packet = decoder.next()) != null) {
        packets.add(packet.toByteArray());
      }
      if (decoder.fill(in) < 0) {
        return packets;
      }
    }
  }

  @Test
  void decodesSeveralPacketsFromOneRead() throws Exception {
    byte[] a = packet(1, 10);
    byte[] b = packet(2, 0);
    byte[] c = packet(3, 100);
    List<byte[]> packets = readAll(new KeyValueFrameDecoder(), chunked(concat(a, b, c), Integer.MAX_VALUE));
    assertEquals(3, packets.size());
    assertArrayEquals(a, packets.get(0));
    assertArrayEquals(b, packets.get(1));
    assertArrayEquals(c, packets.get(2));
  }

  @Test
  void decodesPacketsSplitAcrossReads() throws Exception {
    byte[] a = packet(1, 1000);
    byte[] b = packet(2, 37);
    for (int readSize : new int[]{1, 7, 23, 24, 25, 1000}) {
      List<byte[]> packets = readAll(new KeyValueFrameDecoder(), chunked(concat(a, b), readSize));
      assertEquals(2, packets.size(), "read size " + readSize);
      assertArrayEquals(a, packets.get(0), "read size " + readSize);
      assertArrayEquals(b, packets.get(1), "read size " + readSize);
    }
  }

  @Test
  void decodesPacketBiggerThanBufferThenShrinks() throws Exception {
    byte[] big = packet(1, 5 * KeyValueFrameDecoder.DEFAULT_CAPACITY);
    byte[] small = packet(2, 10);
    KeyValueFrameDecoder decoder = new KeyValueFrameDecoder();
    List<byte[]> packets = readAll(decoder, chunked(concat(big, small, small), 16 * 1024));
    assertEquals(3, packets.size());
    assertArrayEquals(big, packets.get(0));
    assertArrayEquals(small, packets.get(1));
    assertArrayEquals(small, packets.get(2));
  }

  @Test
  void growsToFitPacketThatStartsPartWayThroughTheBuffer() throws Exception {
    byte[] small = packet(1, 10);
    byte[] big = packet(2, 3 * KeyValueFrameDecoder.DEFAULT_CAPACITY);
    for (int readSize : new int[]{7, 1000, 16 * 1024, Integer.MAX_VALUE}) {
      List<byte[]> packets = readAll(new KeyValueFrameDecoder(), chunked(concat(small, big, small), readSize));
      assertEquals(3, packets.size(), "read size " + readSize);
      assertArrayEquals(small, packets.get(0), "read size " + readSize);
      assertArrayEquals(big, packets.get(1), "read size " + readSize);
      assertArrayEquals(small, packets.get(2), "read size " + readSize);
    }
  }

  @Test
  void returnsNullUntilPacketIsComplete() throws Exception {
    byte[] a = packet(1, 100);
    KeyValueFrameDecoder decoder = new KeyValueFrameDecoder();
    ReadableByteChannel in = chunked(a, 50);
    assertNull(decoder.next());
    assertTrue(decoder.fill(in) > 0);
    assertNull(decoder.next()); // 50 of 124 bytes
    assertTrue(decoder.fill(in) > 0);
    assertNull(decoder.next()); // 100 of 124 bytes
    assertTrue(decoder.fill(in) > 0);
    KeyValuePacket packet = decoder.next();
    assertEquals(a.length, packet.size());
    assertNull(decoder.next());
    assertEquals(-1, decoder.fill(in));
  }

  @Test
  void rejectsImpossibleSize() throws Exception {
    byte[] header = packet(1, 0);
    // total body length 0xFFFFFFFF: more than fits in a buffer
    header[8] = header[9] = header[10] = header[11] = (byte) 0xff;
    KeyValueFrameDecoder decoder = new KeyValueFrameDecoder();
    assertTrue(decoder.fill(chunked(header, Integer.MAX_VALUE)) > 0);
    KeyValueFrameDecoder.InvalidSizeException e = assertThrows(KeyValueFrameDecoder.InvalidSizeException.class, decoder::next);
    assertEquals(MemcacheProtocol.HEADER_SIZE, e.header.length);
  }

  @Test
  void endOfStreamWithPartialPacket() throws IOException, KeyValueFrameDecoder.InvalidSizeException {
    byte[] a = packet(1, 100);
    KeyValueFrameDecoder decoder = new KeyValueFrameDecoder();
    ReadableByteChannel in = chunked(Arrays.copyOf(a, 60), Integer.MAX_VALUE);
    assertTrue(decoder.fill(in) > 0);
    assertNull(decoder.next());
    assertEquals(-1, decoder.fill(in));
  }
}
