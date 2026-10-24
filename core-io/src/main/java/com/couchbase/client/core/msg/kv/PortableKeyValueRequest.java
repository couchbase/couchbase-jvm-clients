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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.NettyKeyValueRequestEncoder;
import com.couchbase.client.core.msg.Response;

import java.nio.ByteBuffer;

/**
 * A KV request that's portable between KV implementations: it encodes with a {@link KeyValueRequestEncoder},
 * and decodes from a {@link KeyValuePacket}, so it doesn't depend on Netty's {@link ByteBuf}. The socket KV
 * connection encodes it straight into its write buffer, and decodes it from a view of its read buffer.
 * The Netty implementation encodes it with a {@link NettyKeyValueRequestEncoder}, and decodes it through
 * the {@code ByteBuf} decode bridge below.
 * <p>
 * Transitional: once every KV request is converted, these methods move to {@link KeyValueRequest},
 * and the {@code ByteBuf} methods (which here are bridges, for the Netty KV implementation and for tests) go away.
 */
@Stability.Internal
public interface PortableKeyValueRequest<R extends Response> extends KeyValueRequest<R> {
  /**
   * Builds the request with {@link KeyValueRequestEncoder#beginRequest}, for the connection the encoder belongs to
   * (see {@link KeyValueRequestEncoder#context()}). The caller finishes it, and gives its opaque.
   * <p>
   * Throws if the request can't be encoded; for example, a
   * {@link com.couchbase.client.core.error.CollectionNotFoundException}. It may throw at any point (the caller
   * abandons the request), but throwing before {@link KeyValueRequestEncoder#beginRequest} avoids wasted work.
   */
  void encodeTo(KeyValueRequestEncoder encoder);

  /**
   * Decodes the response. The packet is valid only during the call: copy out whatever the response keeps.
   */
  R decode(KeyValuePacket response, KeyValueChannelContext ctx);

  /**
   * Encodes the request into a {@link ByteBuf} with {@link #encodeTo}. (The Netty KV implementation uses
   * a {@link NettyKeyValueRequestEncoder} of its own instead, which it reuses.)
   */
  @Override
  default ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    return new NettyKeyValueRequestEncoder(alloc, ctx).encode(this, opaque);
  }

  /**
   * Decodes the response with {@link #decode(KeyValuePacket, KeyValueChannelContext)}. Doesn't copy the response.
   * <p>
   * Like {@link com.couchbase.client.core.io.netty.kv.MemcacheProtocol}'s readers, takes the packet to start
   * at index 0 (whatever the reader index). See {@link #packet}.
   */
  @Override
  default R decode(ByteBuf response, KeyValueChannelContext ctx) {
    return decode(packet(response), ctx);
  }

  /**
   * Returns a view of the packet in the buffer, from index 0 to the writer index (whatever the reader index),
   * without copying it. Uses the buffer's internal NIO buffer when it has one, so it usually allocates nothing
   * but the packet. That's safe because the packet is used only while the response is decoded, and nothing else
   * uses the buffer's internal NIO buffer meanwhile.
   */
  static KeyValuePacket packet(ByteBuf response) {
    int length = response.writerIndex();
    ByteBuffer nio = response.nioBufferCount() == 1
      ? response.internalNioBuffer(0, length)
      : response.nioBuffer(0, length); // several components: a copy
    return KeyValuePacket.wrap(nio);
  }
}
