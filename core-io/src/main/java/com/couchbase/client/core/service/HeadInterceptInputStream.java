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

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * NOT THREAD SAFE
 */
@NullMarked
public class HeadInterceptInputStream extends FilterInputStream {
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private final int limit;
  private boolean done; // track this ourselves to avoid synchronization overhead of ByteArrayOutputStream.size()

  public HeadInterceptInputStream(InputStream in, int limit) {
    super(in);
    this.limit = limit;
  }

  @Override
  public int read() throws IOException {
    int i = in.read();
    if (!done && i != -1) {
      buffer.write(i);
      done = buffer.size() >= limit;
    }
    return i;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int count = in.read(b, off, len);
    if (!done && count > 0) {
      buffer.write(b, off, Math.min(count, limit - buffer.size()));
      done = buffer.size() >= limit;
    }
    return count;
  }

  public byte[] getHead() {
    return buffer.toByteArray();
  }

  public String getHeadAsString() {
    return new String(getHead(), UTF_8);
  }
}
