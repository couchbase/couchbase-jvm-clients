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
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;

import static com.couchbase.client.core.io.KeyValueRequestEncoder.MAX_REQUEST_HEAD_SIZE;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.HEADER_SIZE;
import static java.util.Objects.requireNonNull;

/**
 * Builds a request's head (header, framing extras, extras and key) into a heap {@link ByteBuffer}, for the
 * {@link KeyValueRequestEncoder}s, which own the buffer and write the value.
 * <p>
 * The framing extras and extras are written into arrays of their own (one each, allocated with the builder),
 * and the key and value are kept by reference (or the value is written into a growable array of the builder's,
 * with {@link #valueWriter()}), so a request can give them in any order. {@link #complete} checks
 * the request, and writes the head into the buffer: header, framing extras, extras and key. The value isn't copied
 * here; the encoder writes it after the head.
 * <p>
 * Reusable: {@link #begin} starts another request after {@link #reset}. Not thread-safe.
 */
@Stability.Internal
@NullMarked
public final class ByteBufferKeyValueRequestBuilder implements KeyValueRequestBuilder {
  private static final byte[] NO_VALUE = new byte[0];

  /**
   * The most that may be written into a section: whatever's left of the head after the header. (A section's
   * own limit in the header is lower, and checked by {@link #complete}, which also checks the head as a whole.)
   */
  private static final int MAX_SECTION_SIZE = MAX_REQUEST_HEAD_SIZE - HEADER_SIZE;

  /**
   * The biggest value writer array kept for later requests. (A bigger one is dropped when its request is done,
   * so one big value doesn't leave the builder holding a big array.)
   */
  private static final int MAX_RETAINED_VALUE_SIZE = 64 * 1024;

  private final KeyValueChannelContext context;
  private final Section framingExtras = new Section();
  private final Section extras = new Section();
  private final ValueWriter valueWriter = new ValueWriter();

  private @Nullable ByteBuffer buffer;
  private int start = -1; // where the head goes; -1 if no request is being built

  private MemcacheProtocol.@Nullable Opcode opcode;
  private int opaque;
  private int datatype; // 0 to 255
  private int partition; // checked to fit an unsigned short
  private long cas;
  private byte[] keyPrefix = NO_VALUE; // written before the key; for example, a collection ID
  private byte[] key = NO_VALUE;
  private byte[] value = NO_VALUE;
  private int valueLength;
  private boolean valueGiven; // with value()
  private boolean valueWritten; // with valueWriter()

  private boolean complete;
  private int headLength; // once complete

  public ByteBufferKeyValueRequestBuilder(KeyValueChannelContext context) {
    this.context = requireNonNull(context);
  }

  @Override
  public KeyValueChannelContext context() {
    return context;
  }

  /**
   * Starts a request at the buffer's position. The buffer must have a backing array, and room for a head
   * ({@link KeyValueRequestEncoder#MAX_REQUEST_HEAD_SIZE}). Nothing is written into the buffer until
   * {@link #complete}.
   *
   * @throws IllegalStateException if a request is being built, or the buffer doesn't have room.
   * @throws IllegalArgumentException if the buffer has no backing array.
   */
  public void begin(ByteBuffer buffer, Opcode opcode) {
    if (inProgress()) {
      throw new IllegalStateException("The previous request wasn't finished or aborted");
    }
    if (buffer.remaining() < MAX_REQUEST_HEAD_SIZE) {
      throw new IllegalStateException("The buffer has room for " + buffer.remaining() + " bytes, but a request's head may take "
        + MAX_REQUEST_HEAD_SIZE + ". (Was makeRoomForRequest called?)");
    }
    if (!buffer.hasArray()) {
      throw new IllegalArgumentException("The buffer must have a backing array"); // complete() copies into it
    }
    this.buffer = buffer;
    this.start = buffer.position();
    this.opcode = requireNonNull(opcode);
    this.opaque = 0;
    this.datatype = 0;
    this.partition = 0;
    this.cas = 0;
    this.keyPrefix = NO_VALUE;
    this.key = NO_VALUE;
    this.value = NO_VALUE;
    this.valueLength = 0;
    this.valueGiven = false;
    this.valueWritten = false;
    this.framingExtras.clear();
    this.extras.clear();
    this.valueWriter.clear();
    this.complete = false;
  }

  /**
   * Whether a request is being built (begun, and not yet reset).
   */
  public boolean inProgress() {
    return start != -1;
  }

  /**
   * Where the request starts in the buffer.
   */
  public int start() {
    return start;
  }

  @Override
  public ByteBufferKeyValueRequestBuilder addDatatype(int datatype) {
    if (datatype < Byte.MIN_VALUE || datatype > 0xff) {
      throw new IllegalArgumentException("Datatype isn't a byte (signed or unsigned): " + datatype);
    }
    checkBuilding();
    this.datatype |= datatype & 0xff;
    return this;
  }

  @Override
  public ByteBufferKeyValueRequestBuilder partition(int partition) {
    if (partition < 0 || partition > 0xffff) {
      throw new IllegalArgumentException("Partition must be 0 to 65535, but was " + partition);
    }
    checkBuilding();
    this.partition = partition;
    return this;
  }

  @Override
  public ByteBufferKeyValueRequestBuilder cas(long cas) {
    checkBuilding();
    this.cas = cas;
    return this;
  }

  @Override
  public ByteBufferKeyValueRequestBuilder key(byte[] key) {
    return key(NO_VALUE, key);
  }

  @Override
  public ByteBufferKeyValueRequestBuilder key(byte[] collectionPrefix, byte[] key) {
    checkBuilding();
    this.keyPrefix = requireNonNull(collectionPrefix);
    this.key = requireNonNull(key);
    return this;
  }

  @Override
  public ByteBufferKeyValueRequestBuilder value(byte[] value) {
    return value(value, value.length);
  }

  @Override
  public ByteBufferKeyValueRequestBuilder value(byte[] value, int length) {
    if (length < 0 || length > value.length) {
      throw new IllegalArgumentException("Value length " + length + " is out of range; the array is " + value.length + " bytes");
    }
    checkBuilding();
    if (valueWritten) {
      throw new IllegalStateException("The value is being written with valueWriter()");
    }
    this.value = value;
    this.valueLength = length;
    this.valueGiven = true;
    return this;
  }

  /**
   * Whether the request is a SASL request (SASL_AUTH or SASL_STEP), which carries credentials, so its owner
   * keeps it out of buffers that are reused or pooled.
   */
  public boolean isSasl() {
    return opcode == Opcode.SASL_AUTH || opcode == Opcode.SASL_STEP;
  }

  @Override
  public SectionWriter framingExtras() {
    checkBuilding();
    return framingExtras;
  }

  @Override
  public SectionWriter extras() {
    checkBuilding();
    return extras;
  }

  @Override
  public SectionWriter valueWriter() {
    checkBuilding();
    if (valueGiven) {
      throw new IllegalStateException("The value was already given with value()");
    }
    valueWritten = true;
    return valueWriter;
  }

  private void checkBuilding() {
    if (!inProgress()) {
      throw new IllegalStateException("No request was begun");
    }
    if (complete) {
      throw new IllegalStateException("The request's head is already complete");
    }
  }

  /**
   * Writes into a section of the request: the framing extras, the extras, or the value.
   */
  private abstract class Writer implements SectionWriter {
    /**
     * Returns the buffer to write the given number of bytes into, if the request is being built.
     */
    abstract ByteBuffer writable(int bytes);

    @Override
    public SectionWriter putByte(int b) {
      if (b < Byte.MIN_VALUE || b > 0xff) {
        throw new IllegalArgumentException("Not a byte (signed or unsigned): " + b);
      }
      writable(Byte.BYTES).put((byte) b);
      return this;
    }

    @Override
    public SectionWriter putShort(int s) {
      if (s < Short.MIN_VALUE || s > 0xffff) {
        throw new IllegalArgumentException("Not a short (signed or unsigned): " + s);
      }
      writable(Short.BYTES).putShort((short) s);
      return this;
    }

    @Override
    public SectionWriter putInt(int i) {
      writable(Integer.BYTES).putInt(i);
      return this;
    }

    @Override
    public SectionWriter putLong(long l) {
      writable(Long.BYTES).putLong(l);
      return this;
    }

    @Override
    public SectionWriter putBytes(byte[] bytes) {
      writable(bytes.length).put(bytes);
      return this;
    }
  }

  /**
   * One section of the head (framing extras or extras), written into an array of its own.
   */
  private final class Section extends Writer {
    private final ByteBuffer bytes = ByteBuffer.allocate(MAX_SECTION_SIZE);

    void clear() {
      bytes.clear();
    }

    int length() {
      return bytes.position();
    }

    byte[] array() {
      return bytes.array();
    }

    @Override
    ByteBuffer writable(int bytes) {
      checkBuilding();
      return this.bytes; // throws BufferOverflowException if the section is full
    }
  }

  /**
   * The value, when the request writes it (see {@link #valueWriter()}): written into an array that grows as needed,
   * and is kept for later requests (unless it's grown too big).
   */
  private final class ValueWriter extends Writer {
    private ByteBuffer bytes = ByteBuffer.wrap(NO_VALUE);

    void clear() {
      bytes.clear();
    }

    /**
     * Drops the array if it's too big to keep.
     */
    void trim() {
      if (bytes.capacity() > MAX_RETAINED_VALUE_SIZE) {
        bytes = ByteBuffer.wrap(NO_VALUE);
      }
    }

    int length() {
      return bytes.position();
    }

    byte[] array() {
      return bytes.array();
    }

    @Override
    ByteBuffer writable(int bytes) {
      checkBuilding();
      if (this.bytes.remaining() < bytes) {
        grow(bytes);
      }
      return this.bytes;
    }

    private void grow(int more) {
      int length = bytes.position();
      long needed = (long) length + more;
      long doubled = Math.max(256, 2L * bytes.capacity());
      ByteBuffer grown = ByteBuffer.allocate(Math.toIntExact(Math.max(needed, Math.min(doubled, Integer.MAX_VALUE - 8))));
      grown.put(bytes.array(), 0, length);
      bytes = grown;
    }
  }

  private int keyLength() {
    return keyPrefix.length + key.length;
  }

  /**
   * Checks the request, and writes its head into the buffer: header, framing extras, extras and key. Then the head
   * is from {@link #start} to the buffer's position, and the value is {@link #value()}.
   *
   * @param opaque the request's opaque, which its response will have. (The request doesn't set it; whoever writes
   * the request assigns it.)
   * @throws IllegalStateException if no request was begun, or the request can't be encoded (for example,
   * its extras are too long for the header's length field, or its head is too big). The request is then as it was.
   */
  public void complete(int opaque) {
    checkBuilding();
    this.opaque = opaque; // first, so a request that fails the checks is reported with it
    ByteBuffer buffer = requireNonNull(this.buffer);
    int framingExtrasLength = framingExtras.length();
    int extrasLength = extras.length();
    int keyLength = keyLength();
    boolean flexible = framingExtrasLength > 0;
    if (framingExtrasLength > 0xff || extrasLength > 0xff || keyLength > (flexible ? 0xff : 0xffff)) {
      throw new IllegalStateException("Framing extras (" + framingExtrasLength + " bytes), extras (" + extrasLength
        + " bytes) or key (" + keyLength + " bytes) too long for the header");
    }
    int headLength = HEADER_SIZE + framingExtrasLength + extrasLength + keyLength;
    if (headLength > MAX_REQUEST_HEAD_SIZE) {
      throw new IllegalStateException("The request's head is " + headLength + " bytes; the most is " + MAX_REQUEST_HEAD_SIZE);
    }

    putHead(buffer, framingExtrasLength, extrasLength, keyLength);
    this.headLength = headLength;
    this.complete = true;
  }

  /**
   * Whether {@link #complete} succeeded for the request being built.
   */
  public boolean isComplete() {
    return complete;
  }

  /**
   * Returns a copy of the request's head: as written, if it's complete; otherwise as it would be now, without
   * checking it, so the request can be reported (for example, after {@link #complete} failed).
   */
  public byte[] headBytes() {
    if (!inProgress()) {
      return NO_VALUE;
    }
    if (complete) {
      ByteBuffer buffer = requireNonNull(this.buffer);
      byte[] head = new byte[headLength];
      System.arraycopy(buffer.array(), buffer.arrayOffset() + start, head, 0, headLength);
      return head;
    }
    // Lengths that don't fit the header are truncated, like the header's fields.
    ByteBuffer head = ByteBuffer.allocate(HEADER_SIZE + framingExtras.length() + extras.length() + keyLength());
    putHead(head, framingExtras.length(), extras.length(), keyLength());
    return head.array();
  }

  /**
   * Writes the head at the target's position (which it moves past the head). The target must have a backing array,
   * and room for the head.
   */
  private void putHead(ByteBuffer target, int framingExtrasLength, int extrasLength, int keyLength) {
    // The header, by absolute index; then the sections and key, straight into the backing array.
    int i = target.position();
    if (framingExtrasLength > 0) {
      target
        .put(i, MemcacheProtocol.Magic.FLEXIBLE_REQUEST.magic())
        .put(i + 2, (byte) framingExtrasLength)
        .put(i + 3, (byte) keyLength);
    } else {
      target
        .put(i, MemcacheProtocol.Magic.REQUEST.magic())
        .putShort(i + 2, (short) keyLength);
    }
    target
      .put(i + 1, requireNonNull(opcode).opcode())
      .put(i + 4, (byte) extrasLength)
      .put(i + 5, (byte) datatype)
      .putShort(i + 6, (short) partition)
      .putInt(i + 8, framingExtrasLength + extrasLength + keyLength + valueLength())
      .putInt(i + 12, opaque)
      .putLong(i + 16, cas);

    byte[] array = target.array();
    int at = target.arrayOffset() + i + HEADER_SIZE;
    if (framingExtrasLength > 0) {
      System.arraycopy(framingExtras.array(), 0, array, at, framingExtrasLength);
      at += framingExtrasLength;
    }
    if (extrasLength > 0) {
      System.arraycopy(extras.array(), 0, array, at, extrasLength);
      at += extrasLength;
    }
    System.arraycopy(keyPrefix, 0, array, at, keyPrefix.length);
    System.arraycopy(key, 0, array, at + keyPrefix.length, key.length);
    target.position(i + HEADER_SIZE + framingExtrasLength + extrasLength + keyLength);
  }

  /**
   * The value: the array given with {@link #value(byte[], int)}, or the value writer's. Only the first
   * {@link #valueLength()} bytes are the value.
   */
  public byte[] value() {
    return valueWritten ? valueWriter.array() : value;
  }

  public int valueLength() {
    return valueWritten ? valueWriter.length() : valueLength;
  }

  /**
   * Forgets the request. Doesn't change the buffer's position: the owner decides whether the request's bytes stay
   * (it was finished) or go (it was aborted; see {@link #start}).
   */
  public void reset() {
    this.buffer = null;
    this.start = -1;
    this.opcode = null;
    this.keyPrefix = NO_VALUE;
    this.key = NO_VALUE;
    this.value = NO_VALUE;
    this.valueLength = 0;
    this.valueGiven = false;
    this.valueWritten = false;
    this.valueWriter.trim(); // the owner already took the value; the array's contents stay as they are
    this.complete = false;
  }

  @Override
  public String toString() {
    return "ByteBufferKeyValueRequestBuilder{inProgress=" + inProgress() + ", complete=" + complete + "}";
  }
}
