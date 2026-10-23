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

/**
 * What a KV request encodes itself with (see {@link com.couchbase.client.core.msg.kv.PortableKeyValueRequest}).
 * <p>
 * The request calls {@link #beginRequest}, and builds its packet through the stages that returns
 * (see {@link KeyValueRequestBuilder}).
 * Whoever called the request finishes the packet (or abandons it).
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
   * Returns an array of at least the given length, for a request to encode its value into (for example,
   * a sub-document lookup's specs) before {@link #beginRequest}. The encoder may reuse it for later requests, so its contents
   * last only until the request is finished.
   *
   * @throws IllegalStateException if called after {@link #beginRequest}.
   */
  byte[] scratch(int minLength);

  /**
   * Starts a request.
   *
   * @throws IllegalStateException if a request was already begun.
   */
  HeaderStage beginRequest(MemcacheProtocol.Opcode opcode, int opaque);
}
