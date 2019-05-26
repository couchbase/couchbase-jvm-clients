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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

/**
 * Represents a KV GetAndTouch operation.
 *
 * @since 2.0.0
 */
public class GetAndTouchRequest extends BaseKeyValueRequest<GetAndTouchResponse> {

  private final Duration expiration;


  public GetAndTouchRequest(final String key, final Duration timeout, final CoreContext ctx,
                            CollectionIdentifier collectionIdentifier, final RetryStrategy retryStrategy,
                            final Duration expiration) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier);
    this.expiration = expiration;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = Unpooled.wrappedBuffer(ctx.collectionsEnabled() ? keyWithCollection(ctx) : key());
    ByteBuf extras = alloc.buffer(4).writeInt((int) expiration.getSeconds());

    ByteBuf request = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET_AND_TOUCH, noDatatype(),
      partition(), opaque, noCas(), extras, key, noBody());

    extras.release();
    key.release();
    return request;
  }

  @Override
  public GetAndTouchResponse decode(final ByteBuf response, ChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    long cas = cas(response);
    if (status.success()) {
      byte[] content = body(response)
        .map(ByteBufUtil::getBytes)
        .map(bytes -> tryDecompression(bytes, datatype(response)))
        .orElse(new byte[] {});
      int flags = extras(response).map(x -> x.getInt(0)).orElse(0);
      return new GetAndTouchResponse(status, content, cas, flags);
    } else {
      return new GetAndTouchResponse(status, null, cas, 0);
    }
  }

}
