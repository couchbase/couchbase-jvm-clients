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
import com.couchbase.client.core.io.KeyValueRequestBuilder.HeaderStage;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Buffers KV request packets on their way to a stream, and lets a request encode itself straight into the buffer
 * (see {@link KeyValueRequestEncoder}), so encoding doesn't allocate a buffer per request.
 * <p>
 * A request is written in steps. {@link #beginRequest} reserves room for the header, and returns a builder that writes
 * the request's framing extras, extras and key (its "head") into the buffer, and takes its value.
 * {@link #completeRequestHead} checks the request and writes the header. Nothing of the request reaches the stream
 * until {@link #finishRequest}, which writes the value: copied into the buffer if it's small, or written straight
 * to the stream if it's big. So the caller can register the request as in flight (before the server could possibly
 * answer it), or abandon it with {@link #abortRequest}, after encoding.
 * <p>
 * Not thread-safe: used by the connection's writer thread.
 */
@Stability.Internal
@NullMarked
public final class KeyValueRequestWriter implements KeyValueRequestEncoder {
  private final OutputStream out;
  private final ByteBuffer buffer;

  /**
   * Values at least this big are written straight to the stream, instead of being copied into the buffer first.
   */
  private final int directWriteMinSize;

  /**
   * Builds the request being written (between beginRequest and finishRequest or abortRequest), if any.
   */
  private final ByteBufferKeyValueRequestBuilder request = new ByteBufferKeyValueRequestBuilder();

  /**
   * See {@link #scratch}.
   */
  private byte[] scratch = NO_VALUE;

  private static final byte[] NO_VALUE = new byte[0];

  public KeyValueRequestWriter(OutputStream out, int bufferSize) {
    if (bufferSize < MAX_REQUEST_HEAD_SIZE) {
      throw new IllegalArgumentException("Buffer size must be at least " + MAX_REQUEST_HEAD_SIZE + ", but was " + bufferSize);
    }
    this.out = requireNonNull(out);
    this.buffer = ByteBuffer.allocate(bufferSize);
    this.directWriteMinSize = bufferSize / 2;
  }

  /**
   * Flushes the buffer to the stream if it doesn't have room for a request's head ({@link #MAX_REQUEST_HEAD_SIZE}).
   * Call before each request is encoded, so encoding never does I/O.
   */
  public void makeRoomForRequest() throws IOException {
    if (buffer.remaining() < MAX_REQUEST_HEAD_SIZE) {
      writeBuffer();
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * The writer reuses it for later requests, except an array bigger than the write buffer
   * (so one big value doesn't leave the connection holding a big array).
   */
  @Override
  public byte[] scratch(int minLength) {
    checkNoRequest();
    if (minLength > buffer.capacity()) {
      return new byte[minLength];
    }
    if (scratch.length < minLength) {
      scratch = new byte[Math.min(buffer.capacity(), Math.max(minLength, scratch.length * 2))];
    }
    return scratch;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException also if the buffer doesn't have room for a request's head
   * (see {@link #makeRoomForRequest}).
   */
  @Override
  public HeaderStage beginRequest(MemcacheProtocol.Opcode opcode, int opaque) {
    request.begin(buffer, opcode, opaque);
    return request;
  }

  /**
   * Checks the request being written, and writes its header.
   *
   * @throws IllegalStateException if no request was begun, or it can't be encoded (see
   * {@link ByteBufferKeyValueRequestBuilder#complete}). Then {@link #requestBytes} reports it, as far as it goes,
   * and the caller aborts it.
   */
  public void completeRequestHead() {
    request.complete();
  }

  /**
   * Returns a copy of the request being written, as far as it's written, followed by its value.
   * For traffic capture and error reports.
   */
  public byte[] requestBytes() {
    if (!request.inProgress()) {
      return NO_VALUE;
    }
    request.putHeaderUnchecked();
    int start = request.start();
    int headLength = buffer.position() - start;
    int valueLength = request.valueLength();
    byte[] result = Arrays.copyOfRange(buffer.array(), buffer.arrayOffset() + start, buffer.arrayOffset() + start + headLength + valueLength);
    System.arraycopy(request.value(), 0, result, headLength, valueLength);
    return result;
  }

  /**
   * Forgets the request being written (if any), as if it had never begun.
   */
  public void abortRequest() {
    if (!request.inProgress()) {
      return;
    }
    buffer.position(request.start());
    request.reset();
  }

  /**
   * Writes the value of the request being written. Then the request is as good as written
   * (it's written to the stream by a later {@link #flush}, or sooner).
   *
   * @throws IllegalStateException if {@link #completeRequestHead} wasn't called (successfully).
   */
  public void finishRequest() throws IOException {
    if (!request.isComplete()) {
      throw new IllegalStateException("The request's head isn't complete");
    }
    byte[] value = request.value();
    int valueLength = request.valueLength();
    request.reset();
    write(value, 0, valueLength);
  }

  /**
   * Writes bytes (for example, a whole packet encoded some other way).
   */
  public void write(byte[] bytes, int offset, int length) throws IOException {
    checkNoRequest();
    if (length <= buffer.remaining() && length < directWriteMinSize) {
      buffer.put(bytes, offset, length);
      return;
    }
    writeBuffer();
    if (length < directWriteMinSize) {
      buffer.put(bytes, offset, length);
    } else {
      out.write(bytes, offset, length);
    }
  }

  /**
   * Writes what's buffered to the stream, and flushes the stream.
   */
  public void flush() throws IOException {
    writeBuffer();
    out.flush();
  }

  private void writeBuffer() throws IOException {
    checkNoRequest();
    if (buffer.position() > 0) {
      out.write(buffer.array(), buffer.arrayOffset(), buffer.position());
      buffer.clear();
    }
  }

  private void checkNoRequest() {
    if (request.inProgress()) {
      throw new IllegalStateException("A request is being written");
    }
  }

  @Override
  public String toString() {
    return "KeyValueRequestWriter{buffered=" + buffer.position() + ", writingRequest=" + request.inProgress() + "}";
  }
}
