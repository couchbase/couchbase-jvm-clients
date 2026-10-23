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
import com.couchbase.client.core.compression.snappy.SnappyCodec;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.util.Bytes;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.FRAMING_EXTRAS_READ_UNITS_USED;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.FRAMING_EXTRAS_TRACING;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.FRAMING_EXTRAS_WRITE_UNITS_USED;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.HEADER_SIZE;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.UNITS_NOT_PRESENT;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A read-only view of one KV packet (usually a response), which a request decodes its response from
 * (see {@link com.couchbase.client.core.msg.kv.PortableKeyValueRequest#decode(KeyValuePacket, com.couchbase.client.core.io.netty.kv.KeyValueChannelContext)}).
 * <p>
 * The packet shares its bytes with whatever it was read into (it isn't a copy), so it's valid only while the response
 * is being handled: a decoder must copy out whatever it keeps (as {@link #bodyAsBytes} and {@link BodyReader#readBytes} do).
 * <p>
 * Reads by absolute index (except while copying bytes out, when it moves the buffer's position and then puts it back),
 * so readers can't disturb each other.
 */
@Stability.Internal
@NullMarked
public final class KeyValuePacket {
  private static final SnappyCodec snappy = SnappyCodec.instance();

  /**
   * Holds the packet, from {@link #start} for {@link #size} bytes. Never exposed; read by absolute index only,
   * so the packet doesn't depend on its position or limit (beyond the packet), and doesn't change them.
   */
  private final ByteBuffer buffer;
  private final int start;
  private final int size;

  private KeyValuePacket(ByteBuffer buffer, int start, int size) {
    this.buffer = buffer;
    this.start = start;
    this.size = size;
  }

  /**
   * Returns a view of the packet in the buffer's remaining bytes (which must be exactly one packet).
   * Doesn't copy the buffer. The packet reads the buffer's bytes where they are, so while the packet is in use,
   * the caller mustn't change the buffer's limit or bytes, or use the buffer from another thread. (The packet
   * moves the buffer's position while it copies bytes out, and then puts it back.)
   *
   * @throws IllegalArgumentException if the remaining bytes aren't one packet, as far as the header says.
   */
  public static KeyValuePacket wrap(ByteBuffer buffer) {
    if (buffer.order() != ByteOrder.BIG_ENDIAN) {
      buffer = buffer.duplicate().order(ByteOrder.BIG_ENDIAN); // doesn't change the caller's buffer
    }
    int start = buffer.position();
    int size = buffer.remaining();
    if (size < HEADER_SIZE) {
      throw new IllegalArgumentException("A packet is at least " + HEADER_SIZE + " bytes, but this is " + size);
    }
    long expectedSize = HEADER_SIZE + Integer.toUnsignedLong(buffer.getInt(start + TOTAL_BODY_LENGTH_OFFSET));
    if (size != expectedSize) {
      throw new IllegalArgumentException("The packet's header says it's " + expectedSize + " bytes, but it's " + size);
    }
    return new KeyValuePacket(buffer, start, size);
  }

  /**
   * Like {@link #wrap(ByteBuffer)}, for a packet in part of an array.
   */
  public static KeyValuePacket wrap(byte[] array, int offset, int length) {
    return wrap(ByteBuffer.wrap(array, offset, length));
  }

  private static final int OPCODE_OFFSET = 1;
  private static final int EXTRAS_LENGTH_OFFSET = 4;
  private static final int DATATYPE_OFFSET = 5;
  private static final int STATUS_OFFSET = 6;
  private static final int TOTAL_BODY_LENGTH_OFFSET = 8;
  private static final int OPAQUE_OFFSET = 12;
  private static final int CAS_OFFSET = 16;

  /**
   * The packet's size: header and body.
   */
  public int size() {
    return size;
  }

  public byte magic() {
    return getByte(0);
  }

  /**
   * Whether the packet has framing extras ("flexible" framing).
   */
  public boolean isFlexible() {
    byte magic = magic();
    return magic == MemcacheProtocol.Magic.FLEXIBLE_RESPONSE.magic() || magic == MemcacheProtocol.Magic.FLEXIBLE_REQUEST.magic();
  }

  public byte opcode() {
    return getByte(OPCODE_OFFSET);
  }

  public byte datatype() {
    return getByte(DATATYPE_OFFSET);
  }

  /**
   * The response's status (as the server sent it; see also {@link #responseStatus}).
   */
  public short status() {
    return getShort(STATUS_OFFSET);
  }

  /**
   * The response's status, converted.
   */
  public ResponseStatus responseStatus() {
    return MemcacheProtocol.decodeStatus(status());
  }

  public int opaque() {
    return getInt(OPAQUE_OFFSET);
  }

  public long cas() {
    return getLong(CAS_OFFSET);
  }

  public int framingExtrasLength() {
    return isFlexible() ? getByte(2) & 0xff : 0;
  }

  public int extrasLength() {
    return getByte(EXTRAS_LENGTH_OFFSET) & 0xff;
  }

  public int keyLength() {
    return isFlexible() ? getByte(3) & 0xff : getShort(2) & 0xffff;
  }

  private int extrasOffset() {
    return HEADER_SIZE + framingExtrasLength();
  }

  private int keyOffset() {
    return extrasOffset() + extrasLength();
  }

  private int bodyOffset() {
    return keyOffset() + keyLength();
  }

  /**
   * The body's length as sent: compressed, if it's compressed.
   */
  public int rawBodyLength() {
    return size() - bodyOffset();
  }

  public boolean hasBody() {
    return rawBodyLength() > 0;
  }

  public boolean isSnappyCompressed() {
    return MemcacheProtocol.Datatype.isSnappy(datatype());
  }

  /**
   * Returns a copy of the body, decompressed. Empty if there's no body.
   */
  public byte[] bodyAsBytes() {
    int bodyLength = rawBodyLength();
    if (bodyLength <= 0) {
      return Bytes.EMPTY_BYTE_ARRAY;
    }
    int bodyOffset = bodyOffset();
    if (isSnappyCompressed()) {
      return snappy.decompress(copy(bodyOffset, bodyLength));
    }
    return copy(bodyOffset, bodyLength);
  }

  /**
   * Returns the body (decompressed) as a UTF-8 string. Empty if there's no body.
   */
  public String bodyAsString() {
    return new String(bodyAsBytes(), UTF_8);
  }

  /**
   * Returns a reader of the body (decompressed), for a body made of several parts.
   */
  public BodyReader body() {
    if (isSnappyCompressed() && hasBody()) {
      byte[] decompressed = bodyAsBytes();
      return new BodyReader(ByteBuffer.wrap(decompressed), 0, decompressed.length);
    }
    return new BodyReader(buffer, start + bodyOffset(), start + size);
  }

  /**
   * Returns the key as a UTF-8 string. Empty if there's no key.
   */
  public String keyAsString() {
    return new String(copy(keyOffset(), keyLength()), UTF_8);
  }

  /**
   * Returns the int at the given offset in the extras, or the default value if the extras are too short.
   * (Allocates nothing, for the hot path.)
   */
  public int extrasAsInt(int offset, int defaultValue) {
    if (offset < 0 || offset + Integer.BYTES > extrasLength()) {
      return defaultValue;
    }
    return getInt(extrasOffset() + offset);
  }

  /**
   * Returns the mutation token in a successful mutation's extras, if they have one and tokens are enabled.
   */
  public Optional<MutationToken> mutationToken(boolean enabled, short partition, String bucket) {
    if (!enabled || responseStatus() != ResponseStatus.SUCCESS || extrasLength() < 2 * Long.BYTES) {
      return Optional.empty();
    }
    int extrasOffset = extrasOffset();
    return Optional.of(new MutationToken(
      partition,
      getLong(extrasOffset),
      getLong(extrasOffset + Long.BYTES),
      bucket
    ));
  }

  /**
   * Returns the framing extras the client understands, or null if the packet has none.
   */
  public MemcacheProtocol.@Nullable FlexibleExtras flexibleExtras() {
    int framingExtrasLength = framingExtrasLength();
    if (framingExtrasLength == 0) {
      return null;
    }
    int readUnits = UNITS_NOT_PRESENT;
    int writeUnits = UNITS_NOT_PRESENT;
    long serverDuration = UNITS_NOT_PRESENT;
    for (int offset = 0; offset < framingExtrasLength; offset++) {
      byte control = getByte(HEADER_SIZE + offset);
      int id = (control & 0xF0) >> 4;
      int len = control & 0x0F;
      if (id == FRAMING_EXTRAS_TRACING) {
        serverDuration = serverDuration(framingExtraValue(offset));
      } else if (id == FRAMING_EXTRAS_READ_UNITS_USED) {
        readUnits = framingExtraValue(offset);
      } else if (id == FRAMING_EXTRAS_WRITE_UNITS_USED) {
        writeUnits = framingExtraValue(offset);
      }
      offset += len;
    }
    return new MemcacheProtocol.FlexibleExtras(readUnits, writeUnits, serverDuration);
  }

  /**
   * Returns how long the server took (in microseconds, from the tracing framing extra), or 0 if it didn't say.
   * Like {@link MemcacheProtocol#parseServerDurationFromResponse}.
   */
  public long serverDuration() {
    int framingExtrasLength = framingExtrasLength();
    for (int offset = 0; offset < framingExtrasLength; offset++) {
      byte control = getByte(HEADER_SIZE + offset);
      int id = (control & 0xF0) >> 4;
      int len = control & 0x0F;
      if (id == FRAMING_EXTRAS_TRACING) {
        return serverDuration(framingExtraValue(offset));
      }
      offset += len;
    }
    return 0;
  }

  /**
   * Returns the unsigned short value of the framing extra whose control byte is at the given offset.
   */
  private int framingExtraValue(int offset) {
    return getShort(HEADER_SIZE + offset + 1) & 0xffff;
  }

  private static long serverDuration(int encoded) {
    return Math.round(Math.pow(encoded, 1.74) / 2);
  }

  /**
   * Returns a copy of the whole packet.
   */
  public byte[] toByteArray() {
    return copy(0, size());
  }

  private byte[] copy(int index, int length) {
    byte[] bytes = new byte[length];
    if (buffer.hasArray()) {
      System.arraycopy(buffer.array(), buffer.arrayOffset() + start + index, bytes, 0, length);
    } else {
      // Java 8 has no absolute bulk get, so moves the position, and puts it back (instead of allocating a duplicate).
      int position = buffer.position();
      try {
        buffer.position(start + index);
        buffer.get(bytes);
      } finally {
        buffer.position(position);
      }
    }
    return bytes;
  }

  // Absolute reads, relative to the packet's start.

  private byte getByte(int index) {
    return buffer.get(start + index);
  }

  private short getShort(int index) {
    return buffer.getShort(start + index);
  }

  private int getInt(int index) {
    return buffer.getInt(start + index);
  }

  private long getLong(int index) {
    return buffer.getLong(start + index);
  }

  @Override
  public String toString() {
    return "KeyValuePacket{opcode=0x" + Integer.toHexString(opcode() & 0xff)
      + ", status=0x" + Integer.toHexString(status() & 0xffff)
      + ", opaque=" + opaque()
      + ", size=" + size() + "}";
  }
}
