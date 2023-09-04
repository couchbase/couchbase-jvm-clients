/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extrasAsInt;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;

/**
 * Represents a kv get meta operation.
 *
 * @since 2.0.0
 */
public class GetMetaRequest extends BaseKeyValueRequest<GetMetaResponse> {

  public GetMetaRequest(final String key, final Duration timeout, final CoreContext ctx,
                        final CollectionIdentifier collectionIdentifier, final RetryStrategy retryStrategy, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);

    if (span != null) {
      span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_EXISTS);
    }
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf extras = null;

    try {
      extras = alloc.buffer(Byte.BYTES);
      extras.writeByte(2);

      key = encodedKeyWithCollection(alloc, ctx);
      return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET_META, noDatatype(),
        partition(), opaque, noCas(), extras, key, noBody());
    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(extras);
    }
  }

  @Override
  public GetMetaResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    boolean deleted = extrasAsInt(response, 0, 0) != 0;
    return new GetMetaResponse(decodeStatus(response), cas(response), deleted);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "exists";
  }
}
