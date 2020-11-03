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
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

public class IncrementRequest extends BaseKeyValueRequest<IncrementResponse> implements SyncDurabilityRequest {

  public static final int COUNTER_NOT_EXISTS_EXPIRY = 0xffffffff;

  private final long delta;
  private final Optional<Long> initial;
  private final long expiry;
  private final Optional<DurabilityLevel> syncReplicationType;

  public IncrementRequest(Duration timeout, CoreContext ctx, CollectionIdentifier collectionIdentifier,
                          RetryStrategy retryStrategy, String key,
                          long delta, Optional<Long> initial, final long expiration,
                          final Optional<DurabilityLevel> syncReplicationType, RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    if (initial.isPresent() && initial.get() < 0) {
      throw InvalidArgumentException.fromMessage("The initial needs to be >= 0");
    }
    this.delta = delta;
    this.initial = initial;
    this.expiry = expiration;
    this.syncReplicationType = syncReplicationType;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf extras = null;
    ByteBuf flexibleExtras = null;

    try {
      key = encodedKeyWithCollection(alloc, ctx);
      extras = alloc.buffer((Long.BYTES * 2) + Integer.BYTES);
      extras.writeLong(delta);
      if (initial.isPresent()) {
        extras.writeLong(initial.get());
        extras.writeInt((int) expiry);
      } else {
        extras.writeLong(0); // no initial present, will lead to doc not found
        extras.writeInt(COUNTER_NOT_EXISTS_EXPIRY);
      }

      ByteBuf request;
      if (syncReplicationType.isPresent()) {
        if (ctx.syncReplicationEnabled()) {
          flexibleExtras = flexibleSyncReplication(alloc, syncReplicationType.get(), timeout(), context());
          request = MemcacheProtocol.flexibleRequest(alloc, MemcacheProtocol.Opcode.INCREMENT, noDatatype(),
            partition(), opaque, noCas(), flexibleExtras, extras, key, noBody());
        }
        else {
          throw new DurabilityLevelNotAvailableException(KeyValueErrorContext.incompleteRequest(this));
        }
      } else {
        request = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.INCREMENT, noDatatype(),
          partition(), opaque, noCas(), extras, key, noBody());
      }
      return request;
    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(extras);
      ReferenceCountUtil.release(flexibleExtras);
    }
  }

  @Override
  public IncrementResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    return new IncrementResponse(
      decodeStatus(response),
      body(response).map(ByteBuf::readLong).orElse(0L),
      cas(response),
      extractToken(ctx.mutationTokensEnabled(), partition(), response, ctx.bucket().get())
    );
  }

  @Override
  public Optional<DurabilityLevel> durabilityLevel() {
    return syncReplicationType;
  }
}
