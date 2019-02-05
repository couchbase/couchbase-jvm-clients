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
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

/**
 * Represents a KV delete operation.
 *
 * @since 2.0.0
 */
public class RemoveRequest extends BaseKeyValueRequest<RemoveResponse> {

  private final long cas;
  private final Optional<DurabilityLevel> syncReplicationType;


  public RemoveRequest(final String key, final byte[] collection, final long cas, final Duration timeout,
                       final CoreContext ctx, final String bucket,
                       final RetryStrategy retryStrategy,
                       final Optional<DurabilityLevel> syncReplicationType) {
    super(timeout, ctx, bucket, retryStrategy, key, collection);
    this.cas = cas;
    this.syncReplicationType = syncReplicationType;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = Unpooled.wrappedBuffer(ctx.collectionsEnabled() ? keyWithCollection() : key());

    ByteBuf request;
    if (ctx.syncReplicationEnabled() && syncReplicationType.isPresent()) {
      ByteBuf flexibleExtras = flexibleSyncReplication(alloc, syncReplicationType.get(), timeout());
      request = MemcacheProtocol.flexibleRequest(alloc, MemcacheProtocol.Opcode.DELETE, noDatatype(),
        partition(), opaque, cas, flexibleExtras, noExtras(), key, noBody());
      flexibleExtras.release();
    } else {
      request = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.DELETE, noDatatype(),
        partition(), opaque, cas, noExtras(), key, noBody());
    }

    key.release();
    return request;
  }

  @Override
  public RemoveResponse decode(final ByteBuf response, ChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    Optional<MutationToken> mutationToken = extractToken(
      ctx.mutationTokensEnabled(),
      partition(),
      response,
      ctx.bucket()
    );
    return new RemoveResponse(status, cas(response), mutationToken);
  }

}
