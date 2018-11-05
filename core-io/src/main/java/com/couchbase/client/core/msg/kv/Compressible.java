/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.env.CompressionConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Marker interface for all {@link KeyValueRequest KeyValueRequests} that can be compressed.
 *
 * @since 2.0.0
 */
public interface Compressible {

  /**
   * Encode this request with the given allocator and opaque.
   *
   * <p>It also checks if compression should be tried. Note that if the config is null
   * or disabled, compression won't even be attempted.</p>
   *
   * @param alloc the allocator where to grab the buffers from.
   * @param opaque the opaque value to use.
   * @param compressionConfig if compression should be attempted.
   * @return the encoded request as a {@link ByteBuf}.
   */
  ByteBuf encode(ByteBufAllocator alloc, int opaque, CompressionConfig compressionConfig);

}
