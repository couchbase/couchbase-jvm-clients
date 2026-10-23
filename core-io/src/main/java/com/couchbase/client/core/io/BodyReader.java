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

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.NullMarked;

import java.nio.ByteBuffer;

/**
 * Reads a packet's body piece by piece (see {@link KeyValuePacket#body}), for bodies made of several parts,
 * like a sub-document lookup's results. Big-endian, like the rest of the protocol.
 * <p>
 * Valid only as long as the packet is; {@link #readBytes} copies.
 */
@Stability.Internal
@NullMarked
public final class BodyReader {
  private final ByteBuffer buffer; // read by absolute index, except in readBytes
  private final int end;
  private int index;

  BodyReader(ByteBuffer buffer, int start, int end) {
    this.buffer = buffer;
    this.index = start;
    this.end = end;
  }

  /**
   * How many bytes are left to read.
   */
  public int remaining() {
    return end - index;
  }

  public byte readByte() {
    int i = advance(Byte.BYTES);
    return buffer.get(i);
  }

  public short readShort() {
    int i = advance(Short.BYTES);
    return buffer.getShort(i);
  }

  public int readUnsignedShort() {
    return readShort() & 0xffff;
  }

  public int readInt() {
    int i = advance(Integer.BYTES);
    return buffer.getInt(i);
  }

  public long readLong() {
    int i = advance(Long.BYTES);
    return buffer.getLong(i);
  }

  /**
   * Returns a copy of the next {@code length} bytes.
   */
  public byte[] readBytes(int length) {
    if (length < 0) {
      throw new IllegalArgumentException("Negative length: " + length);
    }
    int i = advance(length);
    byte[] bytes = new byte[length];
    if (buffer.hasArray()) {
      System.arraycopy(buffer.array(), buffer.arrayOffset() + i, bytes, 0, length);
    } else {
      // Like KeyValuePacket: moves the position, and puts it back (instead of allocating a duplicate).
      int position = buffer.position();
      try {
        buffer.position(i);
        buffer.get(bytes);
      } finally {
        buffer.position(position);
      }
    }
    return bytes;
  }

  /**
   * Returns a copy of the rest of the body.
   */
  public byte[] readRemaining() {
    return readBytes(remaining());
  }

  public void skip(int length) {
    if (length < 0) {
      throw new IllegalArgumentException("Negative length: " + length);
    }
    advance(length);
  }

  /**
   * Moves past the next {@code length} bytes, and returns where they start.
   */
  private int advance(int length) {
    if (length > remaining()) {
      throw new IndexOutOfBoundsException("Can't read " + length + " bytes; only " + remaining() + " remain");
    }
    int start = index;
    index += length;
    return start;
  }

  @Override
  public String toString() {
    return "BodyReader{remaining=" + remaining() + "}";
  }
}
