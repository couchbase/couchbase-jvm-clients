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
import com.couchbase.client.core.io.netty.kv.EncodeContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;

public class ReplicaGetRequest extends GetRequest {

  private final short replica;

  public ReplicaGetRequest(final String key, final byte[] collection, final Duration timeout,
                           final CoreContext ctx, final String bucket,
                           final RetryStrategy retryStrategy, final short replica) {
    super(key, collection, timeout, ctx, bucket, retryStrategy);
    this.replica = replica;
  }

  public short replica() {
    return replica;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, EncodeContext ctx) {
    ByteBuf key = Unpooled.wrappedBuffer(ctx.collectionsEnabled() ? keyWithCollection() : key());
    ByteBuf r = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET_REPLICA, noDatatype(),
      partition(), opaque, noCas(), noExtras(), key, noBody());
    key.release();
    return r;
  }

}
