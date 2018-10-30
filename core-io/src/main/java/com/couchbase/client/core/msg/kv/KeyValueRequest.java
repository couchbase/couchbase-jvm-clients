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

import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Main parent interface for all Key/Value requests.
 *
 * @param <RES> the generic type of the response.
 * @since 1.0.0
 */
public interface KeyValueRequest<RES extends Response> extends Request<RES> {

  /**
   * Reads the currently set partition this request is targeted against.
   */
  short partition();

  /**
   * Allows to set the partition used for this request.
   *
   * @param partition the partition to set.
   */
  void partition(short partition);

  /**
   * Encode this request with the given allocator and opaque.
   *
   * @param alloc the allocator where to grab the buffers from.
   * @param opaque the opaque value to use.
   * @return the encoded request as a {@link ByteBuf}.
   */
  ByteBuf encode(ByteBufAllocator alloc, int opaque);

  /**
   * Decode the encoded response into its message representation.
   *
   * @param response the response to decode.
   * @return the decoded response as {@link RES}.
   */
  RES decode(ByteBuf response);

}
