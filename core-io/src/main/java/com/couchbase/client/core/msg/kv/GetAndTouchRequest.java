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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.util.Bytes;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

/**
 * Represents a KV GetAndTouch operation.
 *
 * @since 2.0.0
 */
public class GetAndTouchRequest extends BaseKeyValueRequest<GetAndTouchResponse> {

  private final long expiration;

  public GetAndTouchRequest(final String key, final Duration timeout, final CoreContext ctx,
                            CollectionIdentifier collectionIdentifier, final RetryStrategy retryStrategy,
                            final long expiration, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.expiration = expiration;

    if (span != null) {
      span.setAttribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_GAT);
    }
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf extras = null;

    try {
      key = encodedKeyWithCollection(alloc, ctx);
      extras = alloc.buffer(Integer.BYTES).writeInt((int) expiration);

      return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET_AND_TOUCH, noDatatype(),
        partition(), opaque, noCas(), extras, key, noBody());
    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(extras);
    }
  }

  @Override
  public GetAndTouchResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    long cas = cas(response);
    if (status.success()) {
      byte[] bytes = bodyAsBytes(response);
      byte[] content = bytes != null ? tryDecompression(bytes, datatype(response)) : Bytes.EMPTY_BYTE_ARRAY;
      int flags = extrasAsInt(response, 0, 0);
      return new GetAndTouchResponse(status, content, cas, flags);
    } else {
      return new GetAndTouchResponse(status, null, cas, 0);
    }
  }

  @Override
  public String name() {
    return "get_and_touch";
  }
}
