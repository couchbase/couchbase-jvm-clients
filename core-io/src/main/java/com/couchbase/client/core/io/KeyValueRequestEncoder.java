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

/**
 * What a KV request encodes itself with (see {@link com.couchbase.client.core.msg.kv.PortableKeyValueRequest}).
 * <p>
 * The request calls {@link #beginRequest}, and builds its packet with the {@link KeyValueRequestBuilder} that returns.
 * Whoever called {@code encodeTo} then finishes the packet, or abandons it.
 * <p>
 * An encoder belongs to one connection, and has its {@link #context()}: what the request needs to know about
 * the connection (for example, its collections, and the features the server negotiated).
 * <p>
 * Implementations: {@link KeyValueRequestWriter} (the socket KV connection's writer, which encodes straight into
 * its write buffer), and {@link com.couchbase.client.core.io.netty.kv.NettyKeyValueRequestEncoder}
 * (which encodes a request into a {@code ByteBuf}, for the Netty KV implementation).
 */
@Stability.Internal
@NullMarked
public interface KeyValueRequestEncoder {
  /**
   * The most a request's head (header, framing extras, extras and key) may take.
   */
  int MAX_REQUEST_HEAD_SIZE = 1024;

  /**
   * Returns the context of the connection the encoder belongs to.
   */
  KeyValueChannelContext context();

  /**
   * Returns an array of at least the given length, for a request to encode its value into (for example,
   * a sub-document lookup's specs) before {@link #beginRequest}. The encoder reuses the array (for later requests,
   * and for later calls), so its contents last only until the request is finished, and a request may call this
   * only once.
   *
   * @throws IllegalStateException if a request is being built, or this was already called for this request.
   */
  byte[] scratch(int minLength);

  /**
   * Starts a request, and returns the builder for it. The builder is valid until the request is finished
   * or abandoned. (Whoever finishes the request gives its opaque.)
   *
   * @throws IllegalStateException if a request is already being built.
   */
  KeyValueRequestBuilder beginRequest(Opcode opcode);
}
