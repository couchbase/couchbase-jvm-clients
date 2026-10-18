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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;

/**
 * Splits what a KV connection reads into packets (frames): reads as much as the socket has into one buffer,
 * then hands out each complete packet in it, like the Netty implementation's MemcacheProtocolDecodeHandler.
 * <p>
 * Each packet shares the buffer's bytes (it isn't a copy), so it's only valid until the next {@link #fill}.
 * Whoever handles a packet must be done with it (and anything derived from it) by then.
 * <p>
 * The buffer grows to fit a packet bigger than it, then shrinks back once that packet is handled,
 * so one large document doesn't keep a large buffer.
 * <p>
 * Not thread-safe: used by one thread at a time (the connecting thread, then the reader).
 */
@NullMarked
final class KeyValueFrameDecoder {
  /**
   * The buffer's usual size, and the most it's allowed to keep when it doesn't hold a big packet.
   */
  static final int DEFAULT_CAPACITY = 64 * 1024;

  /**
   * The least room {@link #fill} makes for a read, so reads aren't tiny.
   */
  private static final int MIN_READ = 8 * 1024;

  private ByteBuf buffer = Unpooled.buffer(DEFAULT_CAPACITY);

  /**
   * Reads what the stream has (blocking until it has something) into the buffer.
   *
   * @return false if the stream ended.
   */
  boolean fill(InputStream in) throws IOException {
    makeRoom();
    int writerIndex = buffer.writerIndex();
    int read = in.read(buffer.array(), buffer.arrayOffset() + writerIndex, buffer.writableBytes());
    if (read < 0) {
      return false;
    }
    buffer.writerIndex(writerIndex + read);
    return true;
  }

  /**
   * Makes sure there's room to read: at least {@link #MIN_READ} bytes, or the rest of a partly read packet,
   * whichever is more. Moves bytes (the unread part of a packet) only when the room after them runs out,
   * like {@link java.io.BufferedInputStream}, so most reads copy nothing.
   */
  private void makeRoom() {
    if (!buffer.isReadable()) {
      buffer.clear(); // everything was handed out, so start over at the beginning (no copy)
    }

    int needed = Math.max(MIN_READ, remainingOfPartialPacket());

    if (buffer.capacity() > DEFAULT_CAPACITY && buffer.readableBytes() + needed <= DEFAULT_CAPACITY) {
      // Done with the big packet that made the buffer grow: go back to the usual size.
      ByteBuf smaller = Unpooled.buffer(DEFAULT_CAPACITY);
      smaller.writeBytes(buffer);
      buffer = smaller;
      return;
    }

    if (buffer.writableBytes() >= needed) {
      return; // enough room after what's unread
    }

    // Move what's unread (part of one packet) to the start, then grow only if the packet doesn't fit at all.
    buffer.discardReadBytes();
    buffer.ensureWritable(needed);
  }

  /**
   * The bytes still to read of the packet at the start of the buffer, or 0 if there isn't a partial packet
   * whose size is known.
   */
  private int remainingOfPartialPacket() {
    if (buffer.readableBytes() < MemcacheProtocol.HEADER_SIZE) {
      return 0;
    }
    long size = packetSize();
    return size > buffer.readableBytes() && size <= Integer.MAX_VALUE ? (int) (size - buffer.readableBytes()) : 0;
  }

  /**
   * The size of the packet at the start of the buffer (header and body), from its header.
   * Negative or too big if the header is invalid.
   */
  private long packetSize() {
    long bodyLength = buffer.getUnsignedInt(buffer.readerIndex() + 8);
    return MemcacheProtocol.HEADER_SIZE + bodyLength;
  }

  /**
   * Returns true if the buffer holds bytes not handed out yet (for example, the start of a packet).
   */
  boolean hasUnreadBytes() {
    return buffer.isReadable();
  }

  /**
   * Returns the next complete packet in the buffer, or null if there isn't one (call {@link #fill} for more).
   * The packet is valid until the next {@link #fill}.
   *
   * @throws InvalidSizeException if the packet's header gives an impossible size.
   */
  @Nullable ByteBuf next() throws InvalidSizeException {
    if (buffer.readableBytes() < MemcacheProtocol.HEADER_SIZE) {
      return null;
    }
    long size = packetSize();
    if (size > Integer.MAX_VALUE) {
      throw new InvalidSizeException(buffer.copy(buffer.readerIndex(), MemcacheProtocol.HEADER_SIZE));
    }
    if (buffer.readableBytes() < size) {
      return null;
    }
    // Shares the buffer's bytes (no copy), but has a reference count of its own, so releasing it
    // (as callers do when they're done with a packet) doesn't release this decoder's buffer.
    ByteBuf packet = Unpooled.wrappedBuffer(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), (int) size);
    buffer.skipBytes((int) size);
    return packet;
  }

  /**
   * Thrown when a packet's header gives a size too big to be real.
   */
  static final class InvalidSizeException extends Exception {
    final ByteBuf header;

    InvalidSizeException(ByteBuf header) {
      super("Invalid packet size", null, false, false);
      this.header = header;
    }
  }
}
