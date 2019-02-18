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
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extractToken;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.flexibleSyncReplication;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;

public class TouchRequest extends BaseKeyValueRequest<TouchResponse> {

  private final long expiry;
  private final Optional<DurabilityLevel> syncReplicationType;


  public TouchRequest(Duration timeout, CoreContext ctx, String bucket,
                      RetryStrategy retryStrategy, String key, byte[] collection, long expiry,
                      final Optional<DurabilityLevel> syncReplicationType) {
    super(timeout, ctx, bucket, retryStrategy, key, collection);
    this.expiry = expiry;
    this.syncReplicationType = syncReplicationType;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = Unpooled.wrappedBuffer(ctx.collectionsEnabled() ? keyWithCollection() : key());
    ByteBuf extras = alloc.buffer(Integer.BYTES);
    extras.writeInt((int) expiry);

    ByteBuf request;
    if (syncReplicationType.isPresent()) {
      if (ctx.syncReplicationEnabled()) {
        Duration timeoutAdjusted = RequestUtil.handleDurabilityTimeout(context(), timeout());
        ByteBuf flexibleExtras = flexibleSyncReplication(alloc, syncReplicationType.get(), timeoutAdjusted);
        request = MemcacheProtocol.flexibleRequest(alloc, MemcacheProtocol.Opcode.TOUCH, noDatatype(),
                partition(), opaque, noCas(), flexibleExtras, extras, key, noBody());
        flexibleExtras.release();
      }
      else {
        throw new DurabilityLevelNotAvailableException(syncReplicationType.get());
      }
    } else {
      request = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.TOUCH, noDatatype(),
        partition(), opaque, noCas(), extras, key, noBody());
    }

    key.release();
    extras.release();
    return request;
  }

  @Override
  public TouchResponse decode(ByteBuf response, ChannelContext ctx) {
    return new TouchResponse(
      decodeStatus(response),
      cas(response),
      extractToken(ctx.mutationTokensEnabled(), partition(), response, ctx.bucket())
    );
  }
}
