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
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

public class AppendRequest extends BaseKeyValueRequest<AppendResponse> {

  private final byte[] content;
  private final long cas;
  private final Optional<DurabilityLevel> syncReplicationType;

  public AppendRequest(Duration timeout, CoreContext ctx, CollectionIdentifier collectionIdentifier,
                       RetryStrategy retryStrategy, String key, byte[] content,
                       long cas, final Optional<DurabilityLevel> syncReplicationType) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier);
    this.content = content;
    this.cas = cas;
    this.syncReplicationType = syncReplicationType;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = encodedKeyWithCollection(alloc, ctx);

    byte datatype = 0;
    ByteBuf content;
    CompressionConfig config = ctx.compressionConfig();
    if (config != null && config.enabled() && this.content.length >= config.minSize()) {
      ByteBuf maybeCompressed = MemcacheProtocol.tryCompression(this.content, config.minRatio());
      if (maybeCompressed != null) {
        datatype |= MemcacheProtocol.Datatype.SNAPPY.datatype();
        content = maybeCompressed;
      } else {
        content = Unpooled.wrappedBuffer(this.content);
      }
    } else {
      content = Unpooled.wrappedBuffer(this.content);
    }

    ByteBuf request;
    if (syncReplicationType.isPresent()) {
      if (ctx.syncReplicationEnabled()) {
        ByteBuf flexibleExtras = flexibleSyncReplication(alloc, syncReplicationType.get(), timeout());
        request = MemcacheProtocol.flexibleRequest(alloc, MemcacheProtocol.Opcode.APPEND, datatype, partition(),
                opaque, cas, flexibleExtras, noExtras(), key, content);
        flexibleExtras.release();
      }
      else {
        throw new DurabilityLevelNotAvailableException(syncReplicationType.get());
      }
    } else {
      request = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.APPEND, datatype, partition(),
        opaque, cas, noExtras(), key, content);
    }

    key.release();
    content.release();

    return request;
  }

  @Override
  public AppendResponse decode(final ByteBuf response, final ChannelContext ctx) {
    return new AppendResponse(
      decodeStatus(response),
      cas(response),
      extractToken(ctx.mutationTokensEnabled(), partition(), response, ctx.bucket())
    );
  }
}
