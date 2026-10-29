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

import com.couchbase.client.core.compression.snappy.SnappyCodec;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.UnsignedLEB128;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValuePacketTest {
  private static final byte[] EXTRAS = {0, 0, 0, 7};
  private static final byte[] KEY = "key".getBytes(UTF_8);
  private static final byte[] BODY = "{\"a\":1}".getBytes(UTF_8);

  @Test
  void readsHeaderAndSections() {
    KeyValuePacket packet = KeyValuePacket.wrap(ByteBuffer.wrap(response((byte) 0, EXTRAS, KEY, BODY)));

    assertEquals(MemcacheProtocol.Magic.RESPONSE.magic(), packet.magic());
    assertFalse(packet.isFlexible());
    assertEquals(Opcode.GET.opcode(), packet.opcode());
    assertEquals(MemcacheProtocol.Status.NOT_FOUND.status(), packet.status());
    assertEquals(ResponseStatus.NOT_FOUND, packet.responseStatus());
    assertEquals(99, packet.opaque());
    assertEquals(0x0102030405060708L, packet.cas());
    assertEquals(0, packet.framingExtrasLength());
    assertEquals(EXTRAS.length, packet.extrasLength());
    assertEquals(KEY.length, packet.keyLength());
    assertEquals("key", packet.keyAsString());
    assertEquals(7, packet.extrasAsInt(0, -1));
    assertEquals(-1, packet.extrasAsInt(1, -1));
    assertEquals(BODY.length, packet.rawBodyLength());
    assertArrayEquals(BODY, packet.bodyAsBytes());
    assertEquals("{\"a\":1}", packet.bodyAsString());
    assertNull(packet.flexibleExtras());
    assertEquals(0, packet.serverDuration());
    assertEquals(MemcacheProtocol.HEADER_SIZE + EXTRAS.length + KEY.length + BODY.length, packet.size());
  }

  @Test
  void viewsPartOfAnArray() {
    byte[] packet = response((byte) 0, EXTRAS, KEY, BODY);
    byte[] array = new byte[packet.length + 20];
    Arrays.fill(array, (byte) 0xff);
    System.arraycopy(packet, 0, array, 5, packet.length);

    KeyValuePacket view = KeyValuePacket.wrap(array, 5, packet.length);
    assertArrayEquals(packet, view.toByteArray());
    assertArrayEquals(BODY, view.bodyAsBytes());
  }

  @Test
  void viewsDirectBufferWithoutChangingIt() {
    byte[] packet = response(MemcacheProtocol.Datatype.SNAPPY.datatype(), EXTRAS, KEY, SnappyCodec.instance().compress(BODY));
    ByteBuffer direct = ByteBuffer.allocateDirect(packet.length + 4);
    direct.position(4);
    direct.put(packet);
    direct.position(4);

    KeyValuePacket view = KeyValuePacket.wrap(direct);
    assertArrayEquals(BODY, view.bodyAsBytes());
    assertArrayEquals(BODY, view.body().readRemaining());
    assertEquals("key", view.keyAsString());
    assertArrayEquals(packet, view.toByteArray());
    assertEquals(4, direct.position());
  }

  @Test
  void readsAtBufferPositionWithoutChangingIt() {
    byte[] packet = response(MemcacheProtocol.Datatype.SNAPPY.datatype(), EXTRAS, KEY, SnappyCodec.instance().compress(BODY));
    for (ByteBuffer buffer : new ByteBuffer[]{ByteBuffer.allocate(packet.length + 9), ByteBuffer.allocateDirect(packet.length + 9)}) {
      buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN); // the packet reads big-endian anyway
      buffer.position(7);
      buffer.put(packet);
      buffer.position(7).limit(7 + packet.length);

      KeyValuePacket view = KeyValuePacket.wrap(buffer);
      assertEquals(99, view.opaque());
      assertEquals(7, view.extrasAsInt(0, -1));
      assertEquals("key", view.keyAsString());
      assertArrayEquals(BODY, view.bodyAsBytes());
      assertArrayEquals(packet, view.toByteArray());
      assertEquals(7, buffer.position());
      assertEquals(7 + packet.length, buffer.limit());
      assertEquals(java.nio.ByteOrder.LITTLE_ENDIAN, buffer.order());
    }
  }

  @Test
  void sectionReaderLeavesDirectBufferAsItFoundIt() {
    byte[] body = ByteBufUtil.getBytes(Unpooled.buffer().writeInt(3).writeBytes("xyz".getBytes(UTF_8)));
    byte[] packet = response((byte) 0, EXTRAS, KEY, body);
    ByteBuffer direct = ByteBuffer.allocateDirect(packet.length + 6);
    direct.position(6);
    direct.put(packet);
    direct.position(6);

    SectionReader reader = KeyValuePacket.wrap(direct).body();
    assertEquals(3, reader.readInt());
    assertArrayEquals("xyz".getBytes(UTF_8), reader.readBytes(3));
    assertEquals(6, direct.position());
    assertEquals(6 + packet.length, direct.limit());
  }

  @Test
  void sectionReaderStartsAtBufferPosition() {
    byte[] packet = response((byte) 0, EXTRAS, KEY, BODY);
    byte[] array = new byte[packet.length + 11];
    System.arraycopy(packet, 0, array, 11, packet.length);
    assertArrayEquals(BODY, KeyValuePacket.wrap(array, 11, packet.length).body().readRemaining());
  }

  @Test
  void decompressesBody() {
    byte[] compressed = SnappyCodec.instance().compress(BODY);
    KeyValuePacket packet = KeyValuePacket.wrap(ByteBuffer.wrap(response(MemcacheProtocol.Datatype.SNAPPY.datatype(), EXTRAS, KEY, compressed)));

    assertTrue(packet.isSnappyCompressed());
    assertEquals(compressed.length, packet.rawBodyLength());
    assertArrayEquals(BODY, packet.bodyAsBytes());
    assertArrayEquals(BODY, packet.body().readRemaining());
  }

  @Test
  void noBody() {
    KeyValuePacket packet = KeyValuePacket.wrap(ByteBuffer.wrap(response((byte) 0, new byte[0], new byte[0], new byte[0])));

    assertFalse(packet.hasBody());
    assertEquals(0, packet.bodyAsBytes().length);
    assertEquals("", packet.keyAsString());
    assertEquals(0, packet.body().remaining());
    assertEquals(-1, packet.extrasAsInt(0, -1));
  }

  @Test
  void readsFramingExtras() {
    // Tracing (server duration), then read units, then write units: each a control byte (id, length) and 2 bytes.
    byte[] framingExtras = {0x02, 0x00, 0x64, 0x12, 0x00, 0x05, 0x22, 0x00, 0x03};
    byte[] key = new byte[200]; // longer than a signed byte
    Arrays.fill(key, (byte) 'k');
    KeyValuePacket packet = KeyValuePacket.wrap(ByteBuffer.wrap(flexibleResponse(framingExtras, EXTRAS, key, BODY)));

    assertTrue(packet.isFlexible());
    assertEquals(framingExtras.length, packet.framingExtrasLength());
    assertEquals(200, packet.keyLength());
    assertEquals(7, packet.extrasAsInt(0, -1));
    assertArrayEquals(BODY, packet.bodyAsBytes());

    MemcacheProtocol.FlexibleExtras extras = packet.flexibleExtras();
    assertNotNull(extras);
    assertEquals(5, extras.readUnits);
    assertEquals(3, extras.writeUnits);
    long expectedDuration = Math.round(Math.pow(100, 1.74) / 2);
    assertEquals(expectedDuration, extras.serverDuration);
    assertEquals(expectedDuration, packet.serverDuration());

    ByteBuf asByteBuf = Unpooled.wrappedBuffer(flexibleResponse(framingExtras, EXTRAS, new byte[0], BODY));
    assertEquals(NettyMemcacheProtocol.parseServerDurationFromResponse(asByteBuf), packet.serverDuration());
  }

  @Test
  void rejectsWrongSize() {
    byte[] packet = response((byte) 0, EXTRAS, KEY, BODY);
    assertThrows(IllegalArgumentException.class, () -> KeyValuePacket.wrap(packet, 0, packet.length - 1));
    assertThrows(IllegalArgumentException.class, () -> KeyValuePacket.wrap(Arrays.copyOf(packet, packet.length + 1), 0, packet.length + 1));
    assertThrows(IllegalArgumentException.class, () -> KeyValuePacket.wrap(packet, 0, MemcacheProtocol.HEADER_SIZE - 1));
  }

  @Test
  void sectionReaderReadsPiecesAndStopsAtTheEnd() {
    byte[] body = ByteBufUtil.getBytes(Unpooled.buffer()
      .writeByte(1).writeShort(-2).writeInt(3).writeInt(-3).writeLong(4).writeBytes("xyz".getBytes(UTF_8)));
    SectionReader reader = KeyValuePacket.wrap(ByteBuffer.wrap(response((byte) 0, EXTRAS, KEY, body))).body();

    assertEquals(body.length, reader.remaining());
    assertEquals(1, reader.readByte());
    assertEquals(0xfffe, reader.readUnsignedShort());
    assertEquals(3, reader.readInt());
    assertEquals(0xfffffffdL, reader.readUnsignedInt());
    assertEquals(4, reader.readLong());
    reader.skip(1);
    assertArrayEquals("yz".getBytes(UTF_8), reader.readBytes(2));
    assertEquals(0, reader.remaining());
    assertEquals(0, reader.readRemaining().length);
    assertThrows(IndexOutOfBoundsException.class, reader::readByte);
    assertThrows(IllegalArgumentException.class, () -> reader.readBytes(-1));
  }

  @Test
  void readsEachSectionWithItsOwnReader() {
    byte[] framingExtras = {0x02, 0x00, 0x05}; // server duration
    byte[] extras = ByteBufUtil.getBytes(Unpooled.buffer().writeLong(-5).writeInt(6));
    KeyValuePacket packet = KeyValuePacket.wrap(ByteBuffer.wrap(flexibleResponse(framingExtras, extras, KEY, BODY)));

    SectionReader framing = packet.framingExtras();
    assertArrayEquals(framingExtras, framing.readRemaining());

    SectionReader extrasReader = packet.extras();
    assertEquals(extras.length, extrasReader.remaining());
    assertEquals(-5, extrasReader.readLong());
    assertEquals(6, extrasReader.readInt());
    assertThrows(IndexOutOfBoundsException.class, extrasReader::readByte); // stops at the end of the extras

    assertArrayEquals(BODY, packet.body().readRemaining());
  }

  @Test
  void sectionReadersAreEmptyWhenThereIsNoSuchSection() {
    KeyValuePacket packet = KeyValuePacket.wrap(ByteBuffer.wrap(response((byte) 0, new byte[0], KEY, BODY)));
    assertEquals(0, packet.framingExtras().remaining());
    assertEquals(0, packet.extras().remaining());
  }

  @Test
  void sectionReaderReadsUleb128() {
    ByteBuf buf = Unpooled.buffer();
    UnsignedLEB128.write(buf, 0);
    UnsignedLEB128.write(buf, 300);
    UnsignedLEB128.write(buf, -1L); // the largest unsigned 64-bit value
    buf.writeByte(0x80); // starts a value that never ends
    SectionReader reader = KeyValuePacket.wrap(ByteBuffer.wrap(response((byte) 0, EXTRAS, KEY, ByteBufUtil.getBytes(buf)))).body();

    assertEquals(0, reader.readUleb128());
    assertEquals(300, reader.readUleb128());
    assertEquals(-1L, reader.readUleb128());
    assertThrows(IndexOutOfBoundsException.class, reader::readUleb128);
  }

  private static byte[] response(byte datatype, byte[] extras, byte[] key, byte[] body) {
    ByteBuf packet = NettyMemcacheProtocol.response(UnpooledByteBufAllocator.DEFAULT, Opcode.GET, datatype,
      MemcacheProtocol.Status.NOT_FOUND.status(), 99, 0x0102030405060708L,
      Unpooled.wrappedBuffer(extras), Unpooled.wrappedBuffer(key), Unpooled.wrappedBuffer(body));
    return ByteBufUtil.getBytes(packet);
  }

  private static byte[] flexibleResponse(byte[] framingExtras, byte[] extras, byte[] key, byte[] body) {
    ByteBuf packet = Unpooled.buffer()
      .writeByte(MemcacheProtocol.Magic.FLEXIBLE_RESPONSE.magic())
      .writeByte(Opcode.GET.opcode())
      .writeByte(framingExtras.length)
      .writeByte(key.length)
      .writeByte(extras.length)
      .writeByte(0)
      .writeShort(0)
      .writeInt(framingExtras.length + extras.length + key.length + body.length)
      .writeInt(1)
      .writeLong(2)
      .writeBytes(framingExtras)
      .writeBytes(extras)
      .writeBytes(key)
      .writeBytes(body);
    return ByteBufUtil.getBytes(packet);
  }
}
