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
 * A request is written in steps. {@link #beginRequest} returns a builder for the request.
 * {@link #completeRequestHead} checks the request, and writes its head (header, framing extras, extras and key)
 * into the buffer. Nothing of the request reaches the stream until {@link #finishRequest}, which writes the value:
 * copied into the buffer if it's small, or written straight to the stream if it's big. So the caller can register
 * the request as in flight (before the server could possibly answer it), or abandon it with {@link #abortRequest},
 * after encoding.
 * <p>
 * The buffer is cleared once a SASL request in it (which carries credentials) is written to the stream,
 * since it's reused for the connection's later requests.
 * <p>
 * Not thread-safe: used by one thread at a time (the connecting thread during the handshake, then the connection's
 * event loop).
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
  private final ByteBufferKeyValueRequestBuilder request;

  private static final byte[] NO_VALUE = new byte[0];

  /**
   * Whether the buffer holds a SASL request, so it's cleared once it's written to the stream.
   */
  private boolean bufferHoldsCredentials;

  public KeyValueRequestWriter(
    OutputStream out,
    int bufferSize,
    KeyValueChannelContext context
  ) {
    if (bufferSize < MAX_REQUEST_HEAD_SIZE) {
      throw new IllegalArgumentException("Buffer size must be at least " + MAX_REQUEST_HEAD_SIZE + ", but was " + bufferSize);
    }
    this.out = requireNonNull(out);
    this.request = new ByteBufferKeyValueRequestBuilder(context);
    this.buffer = ByteBuffer.allocate(bufferSize);
    this.directWriteMinSize = bufferSize / 2;
  }

  @Override
  public KeyValueChannelContext context() {
    return request.context();
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
   *
   * @throws IllegalStateException also if the buffer doesn't have room for a request's head
   * (see {@link #makeRoomForRequest}).
   */
  @Override
  public KeyValueRequestBuilder beginRequest(Opcode opcode) {
    request.begin(buffer, opcode);
    return request;
  }

  /**
   * Checks the request being written, and writes its head (header, framing extras, extras and key) into the buffer.
   *
   * @param opaque the request's opaque.
   * @throws IllegalStateException if no request was begun, or it can't be encoded (see
   * {@link ByteBufferKeyValueRequestBuilder#complete}). Then nothing was written; {@link #requestBytes} can still
   * report the request, and the caller aborts it.
   */
  public void completeRequestHead(int opaque) {
    request.complete(opaque);
  }

  /**
   * Returns a copy of the request being written: its head, followed by its value. Once the head is complete,
   * that's the packet as it will be sent; before that (for example, after {@link #completeRequestHead} failed),
   * it's the packet as it would be, unchecked. Empty if no request is being written.
   * For traffic capture and error reports.
   */
  public byte[] requestBytes() {
    if (!request.inProgress()) {
      return NO_VALUE;
    }
    byte[] head = request.headBytes();
    int valueLength = request.valueLength();
    byte[] result = Arrays.copyOf(head, head.length + valueLength);
    System.arraycopy(request.value(), 0, result, head.length, valueLength);
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
    if (request.isSasl()) {
      bufferHoldsCredentials = true; // the head, at least, is there; the value may be too
    }
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
      try {
        out.write(buffer.array(), buffer.arrayOffset(), buffer.position());
      } finally {
        if (bufferHoldsCredentials) { // even if writing failed
          Arrays.fill(buffer.array(), buffer.arrayOffset(), buffer.arrayOffset() + buffer.position(), (byte) 0);
          bufferHoldsCredentials = false;
        }
      }
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
