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
import com.couchbase.client.core.io.KeyValueRequestBuilder.SectionWriter;
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
    writer.beginRequest(MemcacheProtocol.Opcode.SET).partition(7).key(KEY).value(value)
      .extras().putInt(0xcafe);
  }

  private void writeRequest(byte[] value) throws IOException {
    writer.makeRoomForRequest();
    build(value);
    writer.completeRequestHead(99);
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
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.REPLACE)
      .addDatatype(3)
      .partition(7)
      .cas(0x0102030405060708L);
    request.framingExtras().putBytes(framingExtras);
    request.extras().putInt(0xcafe);
    request.key(KEY);
    request.value(value, 5);
    writer.completeRequestHead(99);
    writer.finishRequest();

    ByteBuf expected = MemcacheProtocol.flexibleRequest(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.REPLACE,
      (byte) 3, (short) 7, 99, 0x0102030405060708L, Unpooled.wrappedBuffer(framingExtras), Unpooled.buffer().writeInt(0xcafe),
      Unpooled.wrappedBuffer(KEY), Unpooled.wrappedBuffer(value, 0, 5));
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void emptyFramingExtrasMeansNotFlexible() throws IOException {
    writer.makeRoomForRequest();
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET);
    request.framingExtras(); // writes nothing
    request.key(KEY);
    writer.completeRequestHead(1);
    writer.finishRequest();

    byte[] packet = flushed();
    assertEquals(MemcacheProtocol.Magic.REQUEST.magic(), packet[0]);
    assertEquals(KEY.length, ByteBuffer.wrap(packet).getShort(2));
  }

  @Test
  void requestWithNoSections() throws IOException {
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.NOOP);
    writer.completeRequestHead(5);
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
  void partsMayBeGivenInAnyOrder() throws IOException {
    byte[] value = "value".getBytes(UTF_8);
    writer.makeRoomForRequest();
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.REPLACE).value(value);
    SectionWriter extras = request.extras().putShort(0xca);
    request.key(KEY);
    SectionWriter framingExtras = request.framingExtras().putByte(0x10);
    extras.putShort(0xfe);
    request.cas(5).partition(7);
    framingExtras.putBytes(new byte[]{0x01, 0x00, 0x05});
    writer.completeRequestHead(99);
    writer.finishRequest();

    ByteBuf expected = MemcacheProtocol.flexibleRequest(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.REPLACE,
      (byte) 0, (short) 7, 99, 5, Unpooled.wrappedBuffer(new byte[]{0x10, 0x01, 0x00, 0x05}),
      Unpooled.buffer().writeShort(0xca).writeShort(0xfe),
      Unpooled.wrappedBuffer(KEY), Unpooled.wrappedBuffer(value));
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void returnsTheSameSectionWriterEachTime() {
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET);
    assertSame(request.extras(), request.extras());
    assertSame(request.framingExtras(), request.framingExtras());
  }

  @Test
  void putByteAndPutShortTakeSignedOrUnsigned() throws IOException {
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.SET)
      .extras().putByte(-128).putByte(255).putShort(-32768).putShort(65535);
    writer.completeRequestHead(1);
    writer.finishRequest();

    byte[] packet = flushed();
    assertArrayEquals(new byte[]{(byte) 0x80, (byte) 0xff, (byte) 0x80, 0, (byte) 0xff, (byte) 0xff},
      Arrays.copyOfRange(packet, MemcacheProtocol.HEADER_SIZE, packet.length));
  }

  @Test
  void addDatatypeOrsIntoTheDatatypeAndEachRequestStartsAtZero() throws IOException {
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.SET).addDatatype(0x02).addDatatype(0x01).addDatatype(0x02);
    writer.completeRequestHead(1);
    writer.finishRequest();
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.SET).addDatatype(0x04);
    writer.completeRequestHead(2);
    writer.finishRequest();

    ByteBuffer packets = ByteBuffer.wrap(flushed());
    assertEquals(0x03, packets.get(5));
    assertEquals(0x04, packets.get(MemcacheProtocol.HEADER_SIZE + 5));
  }

  @Test
  void datatypeTakesSignedOrUnsignedAndPartitionTakesUnsigned() throws IOException {
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.GET).addDatatype(255).partition(65535).value(new byte[0]);
    writer.completeRequestHead(1);
    writer.finishRequest();
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.GET).addDatatype(-128).partition(0).value(new byte[0]);
    writer.completeRequestHead(1);
    writer.finishRequest();

    ByteBuffer packets = ByteBuffer.wrap(flushed());
    assertEquals((byte) 0xff, packets.get(5));
    assertEquals((short) 0xffff, packets.getShort(6));
    assertEquals((byte) 0x80, packets.get(MemcacheProtocol.HEADER_SIZE + 5));
    assertEquals(0, packets.getShort(MemcacheProtocol.HEADER_SIZE + 6));
  }

  @Test
  void refusesDatatypeOrPartitionOutOfRange() {
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.GET);
    assertThrows(IllegalArgumentException.class, () -> request.addDatatype(256));
    assertThrows(IllegalArgumentException.class, () -> request.addDatatype(-129));
    assertThrows(IllegalArgumentException.class, () -> request.partition(65536));
    assertThrows(IllegalArgumentException.class, () -> request.partition(-1));
  }

  @Test
  void refusesValuesOutOfRangeInsteadOfTruncating() {
    SectionWriter extras = writer.beginRequest(MemcacheProtocol.Opcode.SET).extras();
    assertThrows(IllegalArgumentException.class, () -> extras.putByte(256));
    assertThrows(IllegalArgumentException.class, () -> extras.putByte(-129));
    assertThrows(IllegalArgumentException.class, () -> extras.putShort(65536));
    assertThrows(IllegalArgumentException.class, () -> extras.putShort(-32769));
  }

  @Test
  void keyMayBeSetAnyTimeAndGoesAfterTheExtras() throws IOException {
    writer.makeRoomForRequest();
    KeyValueRequestBuilder header = writer.beginRequest(MemcacheProtocol.Opcode.GET).partition(3);
    SectionWriter extras = header.extras().putInt(3);
    header.key("asd".getBytes(UTF_8));
    extras.putByte(3);
    writer.completeRequestHead(123);
    writer.finishRequest();

    ByteBuf expected = MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.GET, (byte) 0,
      (short) 3, 123, 0, Unpooled.buffer().writeInt(3).writeByte(3), Unpooled.wrappedBuffer("asd".getBytes(UTF_8)),
      Unpooled.EMPTY_BUFFER);
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void writesCollectionPrefixBeforeKey() throws IOException {
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.GET).key(new byte[]{0x09}, KEY);
    writer.completeRequestHead(1);
    writer.finishRequest();

    ByteBuf expected = MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.GET, (byte) 0,
      (short) 0, 1, 0, Unpooled.EMPTY_BUFFER, Unpooled.buffer().writeByte(0x09).writeBytes(KEY), Unpooled.EMPTY_BUFFER);
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void refusesKeyTooLongForFlexibleHeader() {
    writer.beginRequest(MemcacheProtocol.Opcode.SET).key(new byte[256]).framingExtras().putByte(0x50);
    assertThrows(IllegalStateException.class, () -> writer.completeRequestHead(1));
  }

  @Test
  void refusesKeyThatMakesTheHeadTooBig() {
    writer.beginRequest(MemcacheProtocol.Opcode.SET).key(new byte[900]).extras().putBytes(new byte[200]);
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> writer.completeRequestHead(1));
    assertTrue(e.getMessage().contains("the most is " + KeyValueRequestEncoder.MAX_REQUEST_HEAD_SIZE), e.getMessage());
    // The request is as it was, and can be reported.
    assertEquals(MemcacheProtocol.HEADER_SIZE + 200 + 900, writer.requestBytes().length);
  }

  @Test
  void refusesWriterKeptAfterTheHeadIsComplete() throws IOException {
    writer.makeRoomForRequest();
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET);
    SectionWriter extras = request.extras().putInt(1);
    writer.completeRequestHead(1);
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> extras.putInt(2));
    assertEquals("The request's head is already complete", e.getMessage());
    assertThrows(IllegalStateException.class, () -> request.key(KEY));

    writer.finishRequest();
    e = assertThrows(IllegalStateException.class, () -> extras.putInt(2));
    assertEquals("No request was begun", e.getMessage());
  }

  @Test
  void emptySectionIsLikeNoSection() throws IOException {
    writer.makeRoomForRequest();
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.GET).key(KEY);
    request.framingExtras();
    request.extras();
    writer.completeRequestHead(1);
    writer.finishRequest();

    ByteBuf expected = MemcacheProtocol.request(UnpooledByteBufAllocator.DEFAULT, MemcacheProtocol.Opcode.GET, (byte) 0,
      (short) 0, 1, 0, Unpooled.EMPTY_BUFFER, Unpooled.wrappedBuffer(KEY), Unpooled.EMPTY_BUFFER);
    assertArrayEquals(ByteBufUtil.getBytes(expected), flushed());
  }

  @Test
  void refusesSectionTooLongForHeader() {
    KeyValueRequestBuilder request = writer.beginRequest(MemcacheProtocol.Opcode.SET);
    request.extras().putBytes(new byte[256]);
    assertThrows(IllegalStateException.class, () -> writer.completeRequestHead(1));
    // Reported as far as it goes.
    assertEquals(MemcacheProtocol.HEADER_SIZE + 256, writer.requestBytes().length);
  }

  @Test
  void refusesHeadLongerThanTheMost() {
    SectionWriter extras = writer.beginRequest(MemcacheProtocol.Opcode.SET).extras();
    extras.putBytes(new byte[KeyValueRequestEncoder.MAX_REQUEST_HEAD_SIZE - MemcacheProtocol.HEADER_SIZE]);
    assertThrows(BufferOverflowException.class, () -> extras.putBytes(new byte[1]));
  }

  @Test
  void refusesToFinishIncompleteHead() {
    build(new byte[0]);
    assertThrows(IllegalStateException.class, writer::finishRequest);
  }

  @Test
  void refusesToCompleteWhatWasNotBegun() {
    assertThrows(IllegalStateException.class, () -> writer.completeRequestHead(99));
  }

  @Test
  void refusesToBeginWithoutRoom() throws IOException {
    // Buffered (each is smaller than what's written straight through), leaving less room than a head may take.
    writer.write(new byte[2000], 0, 2000);
    writer.write(new byte[1100], 0, 1100);
    assertThrows(IllegalStateException.class, () -> writer.beginRequest(MemcacheProtocol.Opcode.SET));
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.SET);
  }

  @Test
  void requestBytesIncludesValue() {
    byte[] value = "value".getBytes(UTF_8);
    build(value);
    writer.completeRequestHead(99);
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
    writer.completeRequestHead(99);
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
    byte[] small = scratchForNextRequest(10);
    assertSame(small, scratchForNextRequest(5));

    byte[] bigger = scratchForNextRequest(small.length + 1);
    assertTrue(bigger.length <= BUFFER_SIZE);
    assertSame(bigger, scratchForNextRequest(bigger.length));

    byte[] full = scratchForNextRequest(BUFFER_SIZE);
    assertEquals(BUFFER_SIZE, full.length);
    assertSame(full, scratchForNextRequest(1));
  }

  /**
   * Like a request that fails before it begins: takes the scratch array, then lets it go.
   */
  private byte[] scratchForNextRequest(int minLength) {
    byte[] scratch = writer.scratch(minLength);
    writer.abortRequest();
    return scratch;
  }

  @Test
  void doesNotKeepScratchBiggerThanBuffer() {
    byte[] huge = scratchForNextRequest(BUFFER_SIZE + 1);
    assertEquals(BUFFER_SIZE + 1, huge.length);
    assertNotSame(huge, scratchForNextRequest(BUFFER_SIZE + 1));
    assertTrue(scratchForNextRequest(1).length < BUFFER_SIZE);
  }

  @Test
  void refusesScratchWhileWritingRequest() {
    writer.beginRequest(MemcacheProtocol.Opcode.SET);
    assertThrows(IllegalStateException.class, () -> writer.scratch(1));
  }

  @Test
  void refusesSecondScratchForSameRequest() {
    writer.scratch(10);
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> writer.scratch(10));
    assertTrue(e.getMessage().startsWith("scratch() was already called for this request"), e.getMessage());
  }

  @Test
  void scratchIsAvailableAgainOnceTheRequestIsFinished() throws IOException {
    byte[] body = writer.scratch(3);
    writer.makeRoomForRequest();
    writer.beginRequest(MemcacheProtocol.Opcode.SET).value(body, 3);
    writer.completeRequestHead(1);
    writer.finishRequest();

    assertSame(body, writer.scratch(3));
  }
}
