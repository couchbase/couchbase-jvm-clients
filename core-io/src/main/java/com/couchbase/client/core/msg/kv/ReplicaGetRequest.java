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
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

import java.time.Duration;
import java.util.Map;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;

public class ReplicaGetRequest extends GetRequest {

  private final short replica;

  public ReplicaGetRequest(final String key, final Duration timeout,
                           final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                           final RetryStrategy retryStrategy, final short replica, final RequestSpan span) {
    super(key, timeout, ctx, collectionIdentifier, retryStrategy, span);
    this.replica = replica;

    if (span != null) {
      span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_GET_REPLICA);
    }
  }

  @Override
  public int replica() {
    return replica;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    try {
      key = encodedKeyWithCollection(alloc, ctx);
      return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET_REPLICA, noDatatype(),
        partition(), opaque, noCas(), noExtras(), key, noBody());
    } finally {
      ReferenceCountUtil.release(key);
    }
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx =  super.serviceContext();
    ctx.put("isReplica", true);
    ctx.put("replicaNum", replica);
    return ctx;
  }

  @Override
  public String name() {
    return "get_replica";
  }

}
