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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Remembers the first few bytes read from the wrapped stream (the "head"),
 * so they can be included in an error message if the stream's content turns out to be unexpected.
 * <p>
 * Also remembers the first failure to read from the wrapped stream, so a reader that fails can tell whether
 * the stream broke (for example, the connection closed) or its content was the problem (for example, malformed
 * JSON, which a parser may also report as an {@link IOException}).
 * <p>
 * NOT THREAD SAFE
 */
@NullMarked
public class HeadInterceptInputStream extends FilterInputStream {
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private final int limit;
  private boolean done; // track this ourselves to avoid synchronization overhead of ByteArrayOutputStream.size()
  private @Nullable IOException readFailure;

  public HeadInterceptInputStream(InputStream in, int limit) {
    super(in);
    this.limit = limit;
    this.done = limit <= 0;
  }

  @Override
  public int read() throws IOException {
    int i;
    try {
      i = in.read();
    } catch (IOException e) {
      recordReadFailure(e);
      throw e;
    }
    if (!done && i != -1) {
      buffer.write(i);
      done = buffer.size() >= limit;
    }
    return i;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int count;
    try {
      count = in.read(b, off, len);
    } catch (IOException e) {
      recordReadFailure(e);
      throw e;
    }
    if (!done && count > 0) {
      buffer.write(b, off, Math.min(count, limit - buffer.size()));
      done = buffer.size() >= limit;
    }
    return count;
  }

  /**
   * Reads from the wrapped stream until the head is full or the stream ends, so {@link #getHead()}
   * returns as much of the start of the stream as possible, even if the reader gave up early
   * (for example, a parser that failed on the first byte, before much of the stream was read).
   * <p>
   * Doesn't read beyond the head. Bytes read this way are not returned by later reads.
   */
  public void fillHead() throws IOException {
    byte[] scratch = new byte[Math.max(1, Math.min(limit, 1024))];
    while (!done) {
      int remaining = limit - buffer.size();
      if (read(scratch, 0, Math.min(scratch.length, remaining)) == -1) {
        return;
      }
    }
  }

  /**
   * Returns the first failure to read from the wrapped stream, or null if reading hasn't failed.
   */
  public @Nullable IOException readFailure() {
    return readFailure;
  }

  private void recordReadFailure(IOException e) {
    if (readFailure == null) {
      readFailure = e;
    }
  }

  public byte[] getHead() {
    return buffer.toByteArray();
  }

  public String getHeadAsString() {
    return new String(getHead(), UTF_8);
  }
}
