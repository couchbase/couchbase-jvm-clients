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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.io.ByteBufferKeyValueRequestBuilder;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.msg.kv.PortableKeyValueRequest;
import org.jspecify.annotations.NullMarked;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Encodes requests into {@link ByteBuf}s, for the Netty KV implementation.
 * <p>
 * The request builds its head in a reusable array; {@link #finish} copies the head and the value into a buffer
 * of exactly the packet's size. That's the same allocation, and the same copy of the value, as encoding
 * straight into a {@code ByteBuf}.
 * <p>
 * Reusable, so a channel can keep one (and reuse its arrays). Not thread-safe.
 */
@Stability.Internal
@NullMarked
public final class NettyKeyValueRequestEncoder implements KeyValueRequestEncoder {
  /**
   * The biggest scratch array kept for reuse. (Bigger values get an array of their own.)
   */
  private static final int MAX_REUSED_SCRATCH_SIZE = 64 * 1024;

  private static final byte[] NO_VALUE = new byte[0];

  private final ByteBufAllocator alloc;
  private final ByteBuffer head = ByteBuffer.allocate(MAX_REQUEST_HEAD_SIZE);
  private final ByteBufferKeyValueRequestBuilder request = new ByteBufferKeyValueRequestBuilder();
  private byte[] scratch = NO_VALUE;

  public NettyKeyValueRequestEncoder(ByteBufAllocator alloc) {
    this.alloc = requireNonNull(alloc);
  }

  /**
   * Encodes the request.
   */
  public ByteBuf encode(PortableKeyValueRequest<?> request, int opaque, KeyValueChannelContext ctx) {
    try {
      request.encodeTo(this, opaque, ctx);
      return finish();
    } finally {
      abort(); // does nothing if the request was finished
    }
  }

  @Override
  public byte[] scratch(int minLength) {
    checkNoRequest();
    if (minLength > MAX_REUSED_SCRATCH_SIZE) {
      return new byte[minLength];
    }
    if (scratch.length < minLength) {
      scratch = new byte[Math.min(MAX_REUSED_SCRATCH_SIZE, Math.max(minLength, scratch.length * 2))];
    }
    return scratch;
  }

  @Override
  public KeyValueRequestBuilder.HeaderStage beginRequest(MemcacheProtocol.Opcode opcode, int opaque) {
    checkNoRequest(); // before clearing the head buffer, which holds the request in progress
    head.clear();
    request.begin(head, opcode, opaque);
    return request;
  }

  /**
   * Checks the request, and returns it encoded.
   *
   * @throws IllegalStateException if no request was begun, or it can't be encoded
   * (see {@link ByteBufferKeyValueRequestBuilder#complete}).
   */
  public ByteBuf finish() {
    request.complete();
    int headSize = head.position();
    int valueLength = request.valueLength();
    ByteBuf encoded = alloc.buffer(headSize + valueLength)
      .writeBytes(head.array(), 0, headSize)
      .writeBytes(request.value(), 0, valueLength);
    request.reset();
    return encoded;
  }

  /**
   * Forgets the request being encoded (if any).
   */
  public void abort() {
    request.reset();
  }

  private void checkNoRequest() {
    if (request.inProgress()) {
      throw new IllegalStateException("A request is being encoded");
    }
  }
}
