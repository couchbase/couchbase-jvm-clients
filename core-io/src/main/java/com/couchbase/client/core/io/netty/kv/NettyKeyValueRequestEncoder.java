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
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import org.jspecify.annotations.NullMarked;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Encodes requests into {@link ByteBuf}s, for the Netty KV implementation.
 * <p>
 * The request's head is put together in a reusable array; {@link #finish} copies the head and the value into
 * a {@code ByteBuf} of exactly the packet's size, which is the only allocation per request.
 * <p>
 * Reusable, so a channel can keep one (and reuse its arrays), with the channel's context. Not thread-safe.
 */
@Stability.Internal
@NullMarked
public final class NettyKeyValueRequestEncoder implements KeyValueRequestEncoder {
  private final ByteBufAllocator alloc;
  private final ByteBuffer head = ByteBuffer.allocate(MAX_REQUEST_HEAD_SIZE);
  private final ByteBufferKeyValueRequestBuilder request;

  public NettyKeyValueRequestEncoder(
    ByteBufAllocator alloc,
    KeyValueChannelContext context
  ) {
    this.alloc = requireNonNull(alloc);
    this.request = new ByteBufferKeyValueRequestBuilder(context);
  }

  @Override
  public KeyValueChannelContext context() {
    return request.context();
  }

  /**
   * Encodes the request with the given opaque: {@link KeyValueRequest#encodeTo}, then {@link #finish}.
   * If either throws, forgets the request, so the encoder is ready for the next one.
   */
  public ByteBuf encode(KeyValueRequest<?> request, int opaque) {
    try {
      request.encodeTo(this);
      return finish(opaque);
    } finally {
      abort(); // does nothing if the request was finished
    }
  }

  @Override
  public KeyValueRequestBuilder beginRequest(Opcode opcode) {
    checkNoRequest(); // before clearing the head buffer, which holds the request in progress
    head.clear();
    request.begin(head, opcode);
    return request;
  }

  /**
   * Checks the request, and returns it encoded.
   *
   * @param opaque the request's opaque.
   * @throws IllegalStateException if no request was begun, or it can't be encoded
   * (see {@link ByteBufferKeyValueRequestBuilder#complete}).
   */
  public ByteBuf finish(int opaque) {
    request.complete(opaque);
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
