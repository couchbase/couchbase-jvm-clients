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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extractToken;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.mutationFlexibleExtras;

/**
 * Uses the KV replace command to replace a document if it exists.
 *
 * @since 2.0.0
 */
public class ReplaceRequest extends BaseKeyValueRequest<ReplaceResponse> implements SyncDurabilityRequest {

  private final byte[] content;
  private final long expiration;
  private final boolean preserveExpiry;
  private final int flags;
  private final long cas;
  private final Optional<DurabilityLevel> syncReplicationType;

  public ReplaceRequest(final String key, final byte[] content, final long expiration, final boolean preserveExpiry,
                        final int flags, final Duration timeout,
                        final long cas, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                        final RetryStrategy retryStrategy,
                        final Optional<DurabilityLevel> syncReplicationType, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);

    if (expiration != 0 && preserveExpiry) {
      throw InvalidArgumentException.fromMessage("For replace, must not specify both `expiry` and `preserveExpiry`.");
    }

    this.content = content;
    this.expiration = expiration;
    this.preserveExpiry = preserveExpiry;
    this.flags = flags;
    this.cas = cas;
    this.syncReplicationType = syncReplicationType;

    if (span != null) {
      span.setAttribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_REPLACE);
    }
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf content = null;
    ByteBuf extras = null;
    ByteBuf flexibleExtras = mutationFlexibleExtras(this, ctx, alloc, syncReplicationType, preserveExpiry);

    try {
      key = encodedKeyWithCollection(alloc, ctx);
      byte datatype = 0;
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

      extras = alloc.buffer(Integer.BYTES * 2);
      extras.writeInt(flags);
      extras.writeInt((int) expiration);

      return MemcacheProtocol.flexibleRequest(alloc, MemcacheProtocol.Opcode.REPLACE, datatype, partition(),
          opaque, cas, flexibleExtras, extras, key, content);

    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(extras);
      ReferenceCountUtil.release(flexibleExtras);
      ReferenceCountUtil.release(content);
    }
  }

  @Override
  public ReplaceResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    ResponseStatus status = MemcacheProtocol.decodeStatus(response);
    return new ReplaceResponse(
      status,
      cas(response),
      extractToken(ctx.mutationTokensEnabled(), partition(), response, ctx.bucket().get())
    );
  }

  @Override
  public Optional<DurabilityLevel> durabilityLevel() {
    return syncReplicationType;
  }

  @Override
  public String name() {
    return "replace";
  }

}
