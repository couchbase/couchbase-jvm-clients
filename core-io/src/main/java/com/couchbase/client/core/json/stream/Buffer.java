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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * A lightweight cousin of Netty's un-pooled heap ByteBuf.
 */
class Buffer {
  private byte[] array = new byte[256];
  private int readerIndex = 0;
  private int writerIndex = 0;

  public byte[] array() {
    return array;
  }

  public int readerIndex() {
    return readerIndex;
  }

  public int readableBytes() {
    return writerIndex - readerIndex;
  }

  int writerIndex() {
    return writerIndex;
  }

  int maxFastWritableBytes() {
    return array.length - writerIndex;
  }

  public Buffer skipBytes(int len) {
    int newReaderIndex = readerIndex + len;
    if (newReaderIndex < 0 || newReaderIndex > writerIndex) {
      throw new IndexOutOfBoundsException("Skipping " + len + " bytes would put the reader index (" + readerIndex + ") past the writer index (" + writerIndex + ")");
    }
    readerIndex = newReaderIndex;
    return this;
  }

  public Buffer writeBytes(byte[] bytes) {
    return writeBytes(bytes, 0, bytes.length);
  }

  public Buffer writeBytes(byte[] bytes, int offset, int length) {
    ensureWritable(length);
    System.arraycopy(bytes, offset, array, writerIndex, length);
    writerIndex += length;
    return this;
  }

  public Buffer writeBytes(ByteBuffer bytes) {
    int bytesToWrite = bytes.remaining();
    ensureWritable(bytesToWrite);
    bytes.get(array, writerIndex, bytesToWrite);
    writerIndex += bytesToWrite;
    return this;
  }

  public Buffer writeBytes(Buffer source, int len) {
    if (len > source.readableBytes()) {
      throw new IndexOutOfBoundsException("Can't read " + len + " bytes from source buffer, because it only has " + source.readableBytes() + " readable bytes.");
    }

    writeBytes(source.array, source.readerIndex, len);
    source.readerIndex += len;
    return this;
  }

  public Buffer ensureWritable(int length) {
    if (length < 0) throw new IllegalArgumentException("length must be non-negative, but got: " + length);

    int spaceLeft = maxFastWritableBytes();
    if (spaceLeft >= length) return this;

    int additionalRequired = length - spaceLeft;
    growToSize(GrowthStrategy.adjustSize(array.length + additionalRequired));
    return this;
  }

  private void growToSize(int newSize) {
    if (newSize < array.length) {
      throw new UnsupportedOperationException("shrinking is not supported");
    }

    byte[] newBuffer = new byte[newSize];
    System.arraycopy(array, 0, newBuffer, 0, array.length);
    this.array = newBuffer;
  }

  public Buffer discardSomeReadBytes() {
    if (readerIndex == writerIndex) {
      return clear();
    }

    if (readerIndex >= array.length / 2) {
      int oldReadableBytes = readableBytes();
      System.arraycopy(array, readerIndex, array, 0, oldReadableBytes);
      readerIndex = 0;
      writerIndex = oldReadableBytes;
    }

    return this;
  }

  public Buffer getBytes(int sourceOffset, byte[] destination) {
    System.arraycopy(array, sourceOffset, destination, 0, destination.length);
    return this;
  }

  public boolean isReadable() {
    return readableBytes() > 0;
  }

  public Buffer clear() {
    this.readerIndex = 0;
    this.writerIndex = 0;
    return this;
  }

  @Override
  public String toString() {
    return "Buffer{" +
      "arrayLength=" + array.length +
      ", readerIndex=" + readerIndex +
      ", writerIndex=" + writerIndex +
      ", readableBytes=" + readableBytes() +
      ", writableBytes=" + maxFastWritableBytes() +
      '}';
  }

  public String toString(Charset charset) {
    return new String(array, readerIndex, readableBytes(), charset);
  }
}
