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

package com.couchbase.client.core.service;

import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

/**
 * Splits what a KV connection reads into packets (frames): reads as much as the socket has into one buffer,
 * then hands out each complete packet in it.
 * <p>
 * Each packet shares the buffer's bytes (it isn't a copy), so it's only valid until the next {@link #fill}.
 * Whoever handles a packet must be done with it (and anything derived from it) by then.
 * <p>
 * The buffer grows to fit a packet bigger than it, then shrinks back once that packet is handled,
 * so one large document doesn't keep a large buffer.
 * <p>
 * Not thread-safe: used by one thread at a time (the connecting thread, then the connection's event loop).
 */
@NullMarked
final class KeyValueFrameDecoder {
  /**
   * The buffer's usual size, and the most it's allowed to keep when it doesn't hold a big packet.
   */
  static final int DEFAULT_CAPACITY = 64 * 1024;

  /**
   * The least room {@link #fill} makes for a read by default, so reads aren't tiny.
   */
  static final int DEFAULT_MIN_READ = 8 * 1024;

  /**
   * The least room {@link #fill} makes for a read.
   */
  private final int minRead;

  private byte[] buffer = new byte[DEFAULT_CAPACITY];

  /**
   * Wraps {@link #buffer}, for reading from a channel. Replaced when the buffer is.
   */
  private ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

  /**
   * See {@link #lastFillFilledRoom}.
   */
  private boolean lastFillFilledRoom;

  /**
   * Where the next packet to hand out starts.
   */
  private int readIndex;

  /**
   * Where the next read goes: the end of what's been read.
   */
  private int writeIndex;

  KeyValueFrameDecoder() {
    this(DEFAULT_MIN_READ);
  }

  /**
   * @param minRead the least room to make for a read. For example, a TLS channel reads a whole record at a time,
   * and copies it if there isn't room for it.
   */
  KeyValueFrameDecoder(int minRead) {
    if (minRead < 1 || minRead > DEFAULT_CAPACITY) {
      throw new IllegalArgumentException("minRead must be between 1 and " + DEFAULT_CAPACITY + ", but was " + minRead);
    }
    this.minRead = minRead;
  }

  /**
   * Reads what the channel has into the buffer. Doesn't wait if the channel is non-blocking.
   *
   * @return how many bytes were read (0 if the channel had none), or -1 if the channel ended.
   */
  int fill(ReadableByteChannel in) throws IOException {
    makeRoom();
    int room = buffer.length - writeIndex;
    byteBuffer.limit(buffer.length).position(writeIndex);
    int read = in.read(byteBuffer);
    if (read > 0) {
      writeIndex += read;
    }
    lastFillFilledRoom = read == room;
    return read;
  }

  /**
   * Returns true if the last {@link #fill} filled all the room it had, so the channel
   * probably has more.
   */
  boolean lastFillFilledRoom() {
    return lastFillFilledRoom;
  }

  /**
   * Makes sure there's room to read: at least {@link #minRead} bytes, or the rest of a partly read packet,
   * whichever is more. Moves bytes (the unread part of a packet) only when the room after them runs out,
   * like {@link java.io.BufferedInputStream}, so most reads copy nothing.
   */
  private void makeRoom() {
    if (!hasUnreadBytes()) {
      // everything was handed out, so start over at the beginning (no copy)
      readIndex = 0;
      writeIndex = 0;
    }

    int unread = writeIndex - readIndex;
    int needed = Math.max(minRead, remainingOfPartialPacket());

    if (buffer.length > DEFAULT_CAPACITY && unread + needed <= DEFAULT_CAPACITY) {
      // Done with the big packet that made the buffer grow: go back to the usual size.
      moveUnreadTo(new byte[DEFAULT_CAPACITY]);
      return;
    }

    if (buffer.length - writeIndex >= needed) {
      return; // enough room after what's unread
    }

    // Move what's unread (part of one packet) to the start, then grow only if the packet doesn't fit at all.
    moveUnreadTo(unread + needed > buffer.length ? new byte[newCapacity(unread + needed)] : buffer);
  }

  /**
   * Doubles the capacity until it's enough (or as big as an array can be).
   */
  private int newCapacity(int minCapacity) {
    long capacity = buffer.length;
    while (capacity < minCapacity) {
      capacity *= 2;
    }
    return (int) Math.min(capacity, Integer.MAX_VALUE - 8);
  }

  /**
   * Moves the unread bytes to the start of the given buffer (which may be the current one), and uses it from now on.
   */
  private void moveUnreadTo(byte[] destination) {
    int unread = writeIndex - readIndex;
    System.arraycopy(buffer, readIndex, destination, 0, unread);
    if (destination != buffer) {
      buffer = destination;
      byteBuffer = ByteBuffer.wrap(destination);
    }
    readIndex = 0;
    writeIndex = unread;
  }

  /**
   * The bytes still to read of the packet at {@link #readIndex}, or 0 if there isn't a partial packet
   * whose size is known.
   */
  private int remainingOfPartialPacket() {
    int unread = writeIndex - readIndex;
    if (unread < MemcacheProtocol.HEADER_SIZE) {
      return 0;
    }
    long size = packetSize();
    return size > unread && size <= Integer.MAX_VALUE ? (int) (size - unread) : 0;
  }

  /**
   * The size of the packet at {@link #readIndex} (header and body), from its header.
   * Too big to be real if the header is invalid.
   */
  private long packetSize() {
    int i = readIndex + 8; // the total body length
    long bodyLength = ((buffer[i] & 0xffL) << 24)
      | ((buffer[i + 1] & 0xffL) << 16)
      | ((buffer[i + 2] & 0xffL) << 8)
      | (buffer[i + 3] & 0xffL);
    return MemcacheProtocol.HEADER_SIZE + bodyLength;
  }

  /**
   * Returns true if the buffer holds bytes not handed out yet (for example, the start of a packet).
   */
  boolean hasUnreadBytes() {
    return readIndex < writeIndex;
  }

  /**
   * Returns the next complete packet in the buffer, or null if there isn't one (call {@link #fill} for more).
   * The packet is valid until the next {@link #fill}.
   *
   * @throws InvalidSizeException if the packet's header gives an impossible size.
   */
  @Nullable KeyValuePacket next() throws InvalidSizeException {
    int unread = writeIndex - readIndex;
    if (unread < MemcacheProtocol.HEADER_SIZE) {
      return null;
    }
    long size = packetSize();
    if (size > Integer.MAX_VALUE) {
      throw new InvalidSizeException(Arrays.copyOfRange(buffer, readIndex, readIndex + MemcacheProtocol.HEADER_SIZE));
    }
    if (unread < size) {
      return null;
    }
    KeyValuePacket packet = KeyValuePacket.wrap(buffer, readIndex, (int) size); // shares the buffer's bytes (no copy)
    readIndex += (int) size;
    return packet;
  }

  /**
   * Thrown when a packet's header gives a size too big to be real.
   */
  static final class InvalidSizeException extends Exception {
    final byte[] header;

    InvalidSizeException(byte[] header) {
      super("Invalid packet size", null, false, false);
      this.header = header;
    }
  }
}
