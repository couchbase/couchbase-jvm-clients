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

import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.ErrorMap;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ScopedRequest;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

/**
 * Main parent interface for all Key/Value requests.
 *
 * @param <R> the generic type of the response.
 * @since 1.0.0
 */
public interface KeyValueRequest<R extends Response> extends Request<R>, ScopedRequest {

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
   * Returns the index of the replica set member this request targets.
   * <ul>
   *   <li>0 = primary ("active")
   *   <li>1 = first replica
   *   <li>2 = second replica
   *   <li>3 = third replica
   * </ul>
   */
  default int replica() {
    return 0;
  }

  /**
   * Encode this request with the given allocator and opaque.
   *
   * @param alloc the allocator where to grab the buffers from.
   * @param opaque the opaque value to use.
   * @param ctx more encode context.
   * @return the encoded request as a {@link ByteBuf}.
   */
  ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx);

  /**
   * Decode the encoded response into its message representation.
   *
   * @param response the response to decode.
   * @return the decoded response as the generic type R.
   */
  R decode(ByteBuf response, KeyValueChannelContext ctx);

  /**
   * The key of the kv request.
   *
   * @return the key of the request.
   */
  byte[] key();

  CollectionIdentifier collectionIdentifier();

  int opaque();

  /**
   * Returns the number of times this request has been rejected with a not my vbucket response before.
   */
  int rejectedWithNotMyVbucket();

  /**
   * Increments the counter indicating that this request has been rejected with a not my vbucket response.
   */
  void indicateRejectedWithNotMyVbucket();

  /**
   * Sets the error code on the request for debugging purposes.
   *
   * @param errorCode the error code to set for the request.
   */
  void errorCode(ErrorMap.ErrorCode errorCode);

}
