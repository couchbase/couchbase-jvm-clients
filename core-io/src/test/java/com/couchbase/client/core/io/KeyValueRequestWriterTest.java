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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueRequestWriterTest {
  private static final int BUFFER_SIZE = 4096;
  private static final byte[] KEY = "key".getBytes(UTF_8);

  /**
   * Records each write, so tests can tell what was buffered and what was written straight through.
   */
  private static final class RecordingStream extends ByteArrayOutputStream {
    final List<Integer> writes = new ArrayList<>();
    int flushes;

    @Override
    public synchronized void write(byte[] b, int off, int len) {
      writes.add(len);
      super.write(b, off, len);
    }

    @Override
    public void flush() {
      flushes++;
    }
  }

  private final RecordingStream out = new RecordingStream();
  private final KeyValueRequestWriter writer = new KeyValueRequestWriter(out, BUFFER_SIZE);

  /**
   * Builds a SET with 4 bytes of extras, the key, and the value.
   */
  private void build(byte[] value) {
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET, 99).partition((short) 7);
    request.extras().putInt(0xcafe);
    request.key().put(KEY);
    request.value(value, value.length);
  }

  private void writeRequest(byte[] value) throws IOException {
    writer.makeRoomForRequest();
    build(value);
    writer.completeRequestHead();
    writer.finishRequest();
  }

  private static int packetSize(byte[] value) {
    return MemcacheProtocol.HEADER_SIZE + 4 + KEY.length + value.length;
  }

  private byte[] flushed() throws IOException {
    writer.flush();
    return out.toByteArray();
  }

  @Test
  void buffersSmallRequestsUntilFlush() throws IOException {
    byte[] value = "v".getBytes(UTF_8);
    writeRequest(value);
    writeRequest(value);
    assertEquals(0, out.size());

    writer.flush();
    assertEquals(Arrays.asList(2 * packetSize(value)), out.writes);
    assertEquals(1, out.flushes);
  }

  @Test
  void writesHeaderLikeMemcacheProtocol() throws IOException {
    byte[] value = "value".getBytes(UTF_8);
    writeRequest(value);

    ByteBuf expected = MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.SET, (byte) 0,
      (short) 7, 99, 0, Unpooled.buffer().writeInt(0xcafe), Unpooled.wrappedBuffer(KEY), Unpooled.wrappedBuffer(value));
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void writesEveryHeaderField() throws IOException {
    byte[] framingExtras = {0x10, 0x01, 0x00, 0x05};
    byte[] value = "value-and-garbage".getBytes(UTF_8);
    writer.makeRoomForRequest();
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.REPLACE, 99)
      .datatype((byte) 3)
      .partition((short) 7)
      .cas(0x0102030405060708L);
    request.framingExtras().put(framingExtras);
    request.extras().putInt(0xcafe);
    request.key().put(KEY);
    request.value(value, 5);
    writer.completeRequestHead();
    writer.finishRequest();

    ByteBuf expected = MemcacheProtocol.flexibleRequest(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.REPLACE,
      (byte) 3, (short) 7, 99, 0x0102030405060708L, Unpooled.wrappedBuffer(framingExtras), Unpooled.buffer().writeInt(0xcafe),
      Unpooled.wrappedBuffer(KEY), Unpooled.wrappedBuffer(value, 0, 5));
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void emptyFramingExtrasMeansNotFlexible() throws IOException {
    writer.makeRoomForRequest();
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET, 1);
    request.framingExtras(); // writes nothing
    request.key().put(KEY);
    writer.completeRequestHead();
    writer.finishRequest();

    byte[] packet = flushed();
    assertEquals(MemcacheProtocol.Magic.REQUEST.magic(), packet[0]);
    assertEquals(KEY.length, ByteBuffer.wrap(packet).getShort(2));
  }

  @Test
  void requestWithNoSections() throws IOException {
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.NOOP, 5);
    writer.completeRequestHead();
    writer.finishRequest();

    ByteBuf expected = MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.NOOP, (byte) 0,
      (short) 0, 5, 0, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void abortLeavesNoTrace() throws IOException {
    byte[] value = "v".getBytes(UTF_8);
    writeRequest(value);

    writer.makeRoomForRequest();
    build(value);
    writer.abortRequest();

    writeRequest(value);
    writer.flush();
    assertEquals(2 * packetSize(value), out.size());
  }

  @Test
  void refusesSectionsOutOfOrder() {
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET, 1);
    request.key();
    assertThrows(IllegalStateException.class, request::extras);
    assertThrows(IllegalStateException.class, request::key);
    request.value(new byte[0], 0);
    assertThrows(IllegalStateException.class, request::key);
    assertThrows(IllegalStateException.class, () -> request.value(new byte[0], 0));
  }

  @Test
  void refusesWritingAfterValue() {
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET, 1);
    ByteBuffer key = request.key().put(KEY);
    request.value(new byte[0], 0);
    key.put((byte) 0);
    assertThrows(IllegalStateException.class, writer::completeRequestHead);
  }

  @Test
  void refusesSectionTooLongForHeader() {
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET, 1);
    request.extras().put(new byte[256]);
    assertThrows(IllegalStateException.class, writer::completeRequestHead);
    // Reported as far as it goes.
    assertEquals(MemcacheProtocol.HEADER_SIZE + 256, writer.requestBytes().length);
  }

  @Test
  void refusesHeadLongerThanTheMost() {
    ByteBuffer key = writer.beginRequest(MemcacheProtocol.Opcode.SET, 1).key();
    key.put(new byte[KeyValueRequestEncoder.MAX_REQUEST_HEAD_SIZE - MemcacheProtocol.HEADER_SIZE]);
    assertThrows(BufferOverflowException.class, () -> key.put((byte) 0));
  }

  @Test
  void refusesToFinishIncompleteHead() {
    build(new byte[0]);
    assertThrows(IllegalStateException.class, writer::finishRequest);
  }

  @Test
  void refusesToCompleteWhatWasNotBegun() {
    assertThrows(IllegalStateException.class, writer::completeRequestHead);
  }

  @Test
  void refusesToBeginWithoutRoom() throws IOException {
    // Buffered (each is smaller than what's written straight through), leaving less room than a head may take.
    writer.write(new byte[2000], 0, 2000);
    writer.write(new byte[1100], 0, 1100);
    assertThrows(IllegalStateException.class, () -> writer.beginRequest(MemcacheProtocol.Opcode.SET, 1));
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.SET, 1);
  }

  @Test
  void requestBytesIncludesValue() {
    byte[] value = "value".getBytes(UTF_8);
    build(value);
    writer.completeRequestHead();
    byte[] bytes = writer.requestBytes();
    assertEquals(packetSize(value), bytes.length);
    assertEquals(4 + KEY.length + value.length, ByteBuffer.wrap(bytes).getInt(8));
    assertArrayEquals(value, Arrays.copyOfRange(bytes, bytes.length - value.length, bytes.length));
  }

  @Test
  void nothingOfRequestReachesStreamBeforeFinish() throws IOException {
    byte[] big = new byte[BUFFER_SIZE * 2];
    writer.makeRoomForRequest();
    build(big);
    writer.completeRequestHead();
    assertEquals(0, out.size());

    writer.finishRequest();
    // The head (from the buffer), then the value straight from the array.
    assertEquals(Arrays.asList(packetSize(big) - big.length, big.length), out.writes);
  }

  @Test
  void writesBigValueAfterWhatWasBuffered() throws IOException {
    byte[] small = "s".getBytes(UTF_8);
    byte[] big = new byte[BUFFER_SIZE];
    Arrays.fill(big, (byte) 'b');
    writeRequest(small);
    writeRequest(big);
    writeRequest(small);
    writer.flush();

    assertEquals(Arrays.asList(packetSize(small) + packetSize(big) - big.length, big.length, packetSize(small)), out.writes);
    assertEquals(2 * packetSize(small) + packetSize(big), out.size());
  }

  @Test
  void makeRoomFlushesWhenNearlyFull() throws IOException {
    byte[] value = new byte[1000];
    writeRequest(value);
    writeRequest(value);
    writeRequest(value); // leaves less than MAX_REQUEST_HEAD_SIZE free
    assertEquals(0, out.size());
    writer.makeRoomForRequest();
    assertEquals(3 * packetSize(value), out.size());
  }

  @Test
  void reusesScratchUpToBufferSize() {
    byte[] small = writer.scratch(10);
    assertSame(small, writer.scratch(5));

    byte[] bigger = writer.scratch(small.length + 1);
    assertTrue(bigger.length <= BUFFER_SIZE);
    assertSame(bigger, writer.scratch(bigger.length));

    byte[] full = writer.scratch(BUFFER_SIZE);
    assertEquals(BUFFER_SIZE, full.length);
    assertSame(full, writer.scratch(1));
  }

  @Test
  void doesNotKeepScratchBiggerThanBuffer() {
    byte[] huge = writer.scratch(BUFFER_SIZE + 1);
    assertEquals(BUFFER_SIZE + 1, huge.length);
    assertNotSame(huge, writer.scratch(BUFFER_SIZE + 1));
    assertTrue(writer.scratch(1).length < BUFFER_SIZE);
  }

  @Test
  void refusesScratchWhileWritingRequest() {
    writer.beginRequest(MemcacheProtocol.Opcode.SET, 1);
    assertThrows(IllegalStateException.class, () -> writer.scratch(1));
  }
}
