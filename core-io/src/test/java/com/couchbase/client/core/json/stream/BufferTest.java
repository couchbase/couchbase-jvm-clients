/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.json.stream;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BufferTest {

  @Test
  void canGetBytesFromEmptyBuffer() {
    Buffer b = new Buffer();
    b.getBytes(0, new byte[0]);
    b.getBytes(b.array().length, new byte[0]);
    b.getBytes(0, new byte[b.array().length]);
  }

  @Test
  void getOutOfRangeThrowIndexOutOfBounds() {
    Buffer b = new Buffer();

    assertThrows(IndexOutOfBoundsException.class, () -> b.getBytes(-1, new byte[0]));
    assertThrows(IndexOutOfBoundsException.class, () -> b.getBytes(0, new byte[b.array().length + 1]));
    assertThrows(IndexOutOfBoundsException.class, () -> b.getBytes(b.array().length + 1, new byte[1]));
    assertThrows(IndexOutOfBoundsException.class, () -> b.getBytes(b.array().length + 1, new byte[0]));
    assertThrows(IndexOutOfBoundsException.class, () -> b.getBytes(b.array().length + 1, new byte[0]));
  }

  @Test
  void canGetBytes() {
    Buffer b = new Buffer();

    for (int i = 0; i < 128; i++) {
      b.writeBytes(new byte[]{(byte) i});
    }

    byte[] result = new byte[3];

    b.getBytes(0, result);
    assertArrayEquals(new byte[]{0, 1, 2}, result);

    b.getBytes(125, result);
    assertArrayEquals(new byte[]{125, 126, 127}, result);
  }

  @Test
  void doesNotGrowWhenExactlyFull() {
    Buffer b = new Buffer();
    int oldArrayLen = b.array().length;
    b.writeBytes(new byte[oldArrayLen]);
    assertEquals(oldArrayLen, b.array().length);
  }

  @Test
  void canGrow() {
    Buffer b = new Buffer();
    int oldArrayLen = b.array().length;
    b.writeBytes(new byte[oldArrayLen + 1]);
    assertEquals(oldArrayLen * 2, b.array().length);
  }

  @Test
  void growingDoesNotDiscardReadBytes() {
    Buffer b = new Buffer();

    byte[] oldArray = b.array();
    int oldArrayLen = oldArray.length;

    int half = oldArrayLen / 2;
    for (int i = 0; i < oldArrayLen; i++) {
      b.writeBytes(new byte[]{(byte) i});
    }

    assertEquals(oldArrayLen, b.writerIndex());

    b.skipBytes(half);
    assertEquals(half, b.readerIndex());

    int oldWriterIndex = b.writerIndex();
    int oldReaderIndex = b.readerIndex();

    // trigger growth!
    byte magicNumber = 3;
    b.writeBytes(new byte[]{magicNumber});
    assertEquals(oldArrayLen * 2, b.array().length);

    // but reader index should remain unchanged, and writer only incremented by 1
    assertEquals(oldReaderIndex, b.readerIndex());
    assertEquals(oldWriterIndex + 1, b.writerIndex());

    // the new array should have the same contents as the old array,
    // even the bits before the reader index.
    for (int i = 0; i < oldArray.length; i++) {
      assertEquals(oldArray[i], b.array()[i]);
    }

    // and of course, the value we wrote to trigger the growth
    // should be present in the new array
    assertEquals(magicNumber, b.array()[b.writerIndex() - 1]);
  }

  @Test
  void growsEvenIfCompactionWouldMakeEnoughRoom() {
    Buffer b = new Buffer();
    int oldArrayLen = b.array().length;
    b.writeBytes(new byte[1]);
    b.skipBytes(1);
    b.writeBytes(new byte[oldArrayLen]);
    assertEquals(oldArrayLen * 2, b.array().length);
  }

  @Test
  void canDiscardSomeReadBytes() {
    Buffer b = new Buffer();

    int half = b.array().length / 2;

    for (int i = 0; i < b.array().length; i++) {
      assertEquals(i, b.writerIndex());
      b.writeBytes(new byte[]{(byte) i});
    }

    assertEquals(0, b.readerIndex());
    assertEquals(b.array().length, b.writerIndex());

    b.skipBytes(half);
    assertEquals(half, b.readerIndex());

    b.discardSomeReadBytes();

    // last half of the values should now be in the first half of the array
    assertEquals(0, b.readerIndex());
    assertEquals(half, b.writerIndex());
    assertEquals(half, b.array()[0] & 0xff);
  }

  @Test
  void canWriteArray() {
    Buffer b = new Buffer();

    b.writeBytes(new byte[]{-1, 0, 1, 2, -1}, 1, 3);
    assertEquals(0, b.readerIndex());
    assertEquals(3, b.writerIndex());

    byte[] bytes = new byte[3];
    b.getBytes(0, bytes);
    assertArrayEquals(new byte[]{0, 1, 2}, bytes);
  }

  @Test
  void canWriteNioByteBuffer() {
    Buffer b = new Buffer();

    b.writeBytes(ByteBuffer.wrap(new byte[]{0, 1, 2}));
    assertEquals(0, b.readerIndex());
    assertEquals(3, b.writerIndex());

    byte[] bytes = new byte[3];
    b.getBytes(0, bytes);
    assertArrayEquals(new byte[]{0, 1, 2}, bytes);
  }
}
