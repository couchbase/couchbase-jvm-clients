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
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.util.Bytes;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.datatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.tryDecompression;

/**
 * Represents a KV Get (full document) operation.
 *
 * @since 2.0.0
 */
public class GetRequest extends BaseKeyValueRequest<GetResponse> {

  public static final String OPERATION_NAME = "get";

  public GetRequest(final String key, final Duration timeout, final CoreContext ctx,
                    final CollectionIdentifier collectionIdentifier, final RetryStrategy retryStrategy, final InternalSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    try {
      key = encodedKeyWithCollection(alloc, ctx);
      return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET, noDatatype(),
        partition(), opaque, noCas(), noExtras(), key, noBody());
    } finally {
      ReferenceCountUtil.release(key);
    }
  }

  @Override
  public GetResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    long cas = cas(response);

    if (status.success()) {
      byte[] content = body(response)
        .map(ByteBufUtil::getBytes)
        .map(bytes -> tryDecompression(bytes, datatype(response)))
        .orElse(Bytes.EMPTY_BYTE_ARRAY);
      int flags = extras(response).map(x -> x.getInt(0)).orElse(0);
      return new GetResponse(status, content, cas, flags);
    } else {
      return new GetResponse(status, null, cas, 0);
    }
  }

  @Override
  public boolean idempotent() {
    return true;
  }

}
