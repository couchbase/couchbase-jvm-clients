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
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;

import static com.couchbase.client.core.io.KeyValueRequestEncoder.MAX_REQUEST_HEAD_SIZE;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.HEADER_SIZE;
import static java.util.Objects.requireNonNull;

/**
 * Builds a request's head (header, framing extras, extras and key) in a heap {@link ByteBuffer}, for the
 * {@link KeyValueRequestEncoder}s, which own the buffer and write the value.
 * <p>
 * Reserves room for the header when the request begins, and writes the header in {@link #complete}, once the
 * sections' lengths are known. Reusable: {@link #begin} starts another request after {@link #reset}.
 * <p>
 * Not thread-safe.
 */
@Stability.Internal
@NullMarked
public final class ByteBufferKeyValueRequestBuilder implements KeyValueRequestBuilder {
  private static final byte[] NO_VALUE = new byte[0];

  private static final int FRAMING_EXTRAS = 0;
  private static final int EXTRAS = 1;
  private static final int KEY = 2;
  private static final int SECTIONS = 3;

  private @Nullable ByteBuffer buffer;
  private int start = -1; // where the header goes; -1 if no request is being built

  private MemcacheProtocol.@Nullable Opcode opcode;
  private int opaque;
  private byte datatype;
  private short partition;
  private long cas;

  // Where each section starts (-1 if the request didn't write it), and which was started last.
  private final int[] sectionStarts = new int[SECTIONS];
  private int lastSection;

  private boolean hasValue;
  private int headEnd; // where the head ended, once the value is given
  private byte[] value = NO_VALUE;
  private int valueLength;

  private boolean complete;

  /**
   * Starts a request at the buffer's position, which must have room for a head ({@link KeyValueRequestEncoder#MAX_REQUEST_HEAD_SIZE}).
   * Limits the buffer to that, until {@link #reset}.
   *
   * @throws IllegalStateException if a request is being built, or the buffer doesn't have room.
   */
  public void begin(ByteBuffer buffer, MemcacheProtocol.Opcode opcode, int opaque) {
    if (inProgress()) {
      throw new IllegalStateException("The previous request wasn't finished or aborted");
    }
    if (buffer.remaining() < MAX_REQUEST_HEAD_SIZE) {
      throw new IllegalStateException("The buffer has room for " + buffer.remaining() + " bytes, but a request's head may take "
        + MAX_REQUEST_HEAD_SIZE + ". (Was makeRoomForRequest called?)");
    }
    this.buffer = buffer;
    this.start = buffer.position();
    this.opcode = requireNonNull(opcode);
    this.opaque = opaque;
    this.datatype = 0;
    this.partition = 0;
    this.cas = 0;
    for (int i = 0; i < SECTIONS; i++) {
      sectionStarts[i] = -1;
    }
    this.lastSection = -1;
    this.hasValue = false;
    this.complete = false;

    buffer.limit(start + MAX_REQUEST_HEAD_SIZE);
    buffer.position(start + HEADER_SIZE); // the header is written by complete()
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
  public KeyValueRequestBuilder datatype(byte datatype) {
    checkNotComplete();
    this.datatype = datatype;
    return this;
  }

  @Override
  public KeyValueRequestBuilder partition(short partition) {
    checkNotComplete();
    this.partition = partition;
    return this;
  }

  @Override
  public KeyValueRequestBuilder cas(long cas) {
    checkNotComplete();
    this.cas = cas;
    return this;
  }

  @Override
  public ByteBuffer framingExtras() {
    return startSection(FRAMING_EXTRAS);
  }

  @Override
  public ByteBuffer extras() {
    return startSection(EXTRAS);
  }

  @Override
  public ByteBuffer key() {
    return startSection(KEY);
  }

  private ByteBuffer startSection(int section) {
    ByteBuffer buffer = checkBuilding();
    if (section <= lastSection) {
      throw new IllegalStateException("Sections must be written once each, in order: framing extras, extras, key, value");
    }
    sectionStarts[section] = buffer.position();
    lastSection = section;
    return buffer;
  }

  @Override
  public void value(byte[] value, int length) {
    ByteBuffer buffer = checkBuilding();
    if (length < 0 || length > value.length) {
      throw new IllegalArgumentException("Value length " + length + " is out of range; the array is " + value.length + " bytes");
    }
    this.hasValue = true;
    this.headEnd = buffer.position();
    this.value = value;
    this.valueLength = length;
  }

  private ByteBuffer checkBuilding() {
    ByteBuffer buffer = this.buffer;
    if (!inProgress() || buffer == null) {
      throw new IllegalStateException("No request was begun");
    }
    checkNotComplete();
    if (hasValue) {
      throw new IllegalStateException("The value was already given; it comes last");
    }
    return buffer;
  }

  private void checkNotComplete() {
    if (complete) {
      throw new IllegalStateException("The request's head is already complete");
    }
  }

  private int headEnd() {
    return hasValue ? headEnd : requireNonNull(buffer).position();
  }

  /**
   * Returns how long the given section is: up to where the next section starts, or where the head ends.
   */
  private int sectionLength(int section) {
    int sectionStart = sectionStarts[section];
    if (sectionStart == -1) {
      return 0;
    }
    for (int next = section + 1; next < SECTIONS; next++) {
      if (sectionStarts[next] != -1) {
        return sectionStarts[next] - sectionStart;
      }
    }
    return headEnd() - sectionStart;
  }

  /**
   * Checks the request, and writes its header. Then the head is from {@link #start} to the buffer's position,
   * and the value is {@link #value()}. Restores the buffer's limit.
   *
   * @throws IllegalStateException if no request was begun, or the request can't be encoded (for example,
   * its extras are too long for the header's length field). The request is then as it was.
   */
  public void complete() {
    ByteBuffer buffer = this.buffer;
    if (!inProgress() || buffer == null) {
      throw new IllegalStateException("No request was begun");
    }
    checkNotComplete();
    if (hasValue && buffer.position() != headEnd) {
      throw new IllegalStateException("The request wrote " + (buffer.position() - headEnd) + " bytes after giving its value");
    }
    int framingExtrasLength = sectionLength(FRAMING_EXTRAS);
    int extrasLength = sectionLength(EXTRAS);
    int keyLength = sectionLength(KEY);
    boolean flexible = framingExtrasLength > 0;
    if (framingExtrasLength > 0xff || extrasLength > 0xff || keyLength > (flexible ? 0xff : 0xffff)) {
      throw new IllegalStateException("Framing extras (" + framingExtrasLength + " bytes), extras (" + extrasLength
        + " bytes) or key (" + keyLength + " bytes) too long for the header");
    }
    putHeader(buffer, framingExtrasLength, extrasLength, keyLength);
    buffer.limit(buffer.capacity());
    complete = true;
  }

  /**
   * Whether {@link #complete} succeeded for the request being built.
   */
  public boolean isComplete() {
    return complete;
  }

  /**
   * Writes the header as it would be now, without checking it, so a copy of the request can be reported
   * (for example, after {@link #complete} failed).
   */
  public void putHeaderUnchecked() {
    if (!complete && inProgress()) {
      putHeader(requireNonNull(buffer), sectionLength(FRAMING_EXTRAS), sectionLength(EXTRAS), sectionLength(KEY));
    }
  }

  private void putHeader(ByteBuffer buffer, int framingExtrasLength, int extrasLength, int keyLength) {
    int i = start;
    if (framingExtrasLength > 0) {
      buffer
        .put(i, MemcacheProtocol.Magic.FLEXIBLE_REQUEST.magic())
        .put(i + 2, (byte) framingExtrasLength)
        .put(i + 3, (byte) keyLength);
    } else {
      buffer
        .put(i, MemcacheProtocol.Magic.REQUEST.magic())
        .putShort(i + 2, (short) keyLength);
    }
    buffer
      .put(i + 1, requireNonNull(opcode).opcode())
      .put(i + 4, (byte) extrasLength)
      .put(i + 5, datatype)
      .putShort(i + 6, partition)
      .putInt(i + 8, framingExtrasLength + extrasLength + keyLength + valueLength())
      .putInt(i + 12, opaque)
      .putLong(i + 16, cas);
  }

  public byte[] value() {
    return hasValue ? value : NO_VALUE;
  }

  public int valueLength() {
    return hasValue ? valueLength : 0;
  }

  /**
   * Forgets the request, and restores the buffer's limit. Doesn't change the buffer's position: the owner decides
   * whether the request's bytes stay (it was finished) or go (it was aborted; see {@link #start}).
   */
  public void reset() {
    ByteBuffer buffer = this.buffer;
    if (buffer != null) {
      buffer.limit(buffer.capacity());
    }
    this.buffer = null;
    this.start = -1;
    this.opcode = null;
    this.value = NO_VALUE;
    this.hasValue = false;
    this.complete = false;
  }

  @Override
  public String toString() {
    return "ByteBufferKeyValueRequestBuilder{inProgress=" + inProgress() + ", complete=" + complete + "}";
  }
}
