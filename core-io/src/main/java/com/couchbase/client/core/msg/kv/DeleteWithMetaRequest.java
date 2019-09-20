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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

/**
 * Represents the KV "del-with-meta" command.
 *
 * @since 2.0.0
 */
public class DeleteWithMetaRequest extends BaseKeyValueRequest<DeleteWithMetaResponse> {

  private final byte[] content;
  private final long expiration;
  private final int options;
  private final long revSeqNoToSet;
  private final long cas;
  private final long casToSet;

  private final byte FORCE_ACCEPT_WITH_META_OPS = 0x2;
  private final byte REGENERATE_CAS = 0x4;
  private final byte SKIP_CONFLICT_RESOLUTION_FLAG = 0x8;


  public DeleteWithMetaRequest(final String key, final byte[] content, final long expiration,
                               final boolean regenerateCas, final Duration timeout,
                               final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                               final RetryStrategy retryStrategy,
                               final long cas, final long casToSet,
                               final long revSeqNoToSet, final boolean isLastWriteWinsBucket) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier);
    this.content = content;
    this.expiration = expiration;
    this.cas = cas;
    this.casToSet = casToSet;
    this.revSeqNoToSet = revSeqNoToSet;
    int options = 0x0;
    if (isLastWriteWinsBucket) {
      options |= FORCE_ACCEPT_WITH_META_OPS;
    }
    if (regenerateCas) {
      options |= REGENERATE_CAS;
      // if REGENERATE_CAS is set, SKIP_CONFLICT_RESOLUTION_FLAG must also be set
      options |= SKIP_CONFLICT_RESOLUTION_FLAG;
    }
    this.options = options;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf content = null;
    ByteBuf extras = null;
    ByteBuf flexibleExtras = null;

    try {
      key = encodedKeyWithCollection(alloc, ctx);

      byte datatype = 0;
      if (this.content.length > 0) {
          datatype = Datatype.XATTR.datatype();
      }
      content = Unpooled.wrappedBuffer(this.content);

      // 28 bytes is required amount for the mandatory settings plus options
      extras = alloc.buffer(28);
      extras.writeInt(0); // flags
      extras.writeInt((int) expiration);
      extras.writeLong(revSeqNoToSet);
      extras.writeLong(casToSet);
      extras.writeInt(options);

      return MemcacheProtocol.request(alloc, Opcode.DELETE_WITH_META, datatype, partition(),
          opaque, cas, extras, key, content);
    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(extras);
      ReferenceCountUtil.release(flexibleExtras);
      ReferenceCountUtil.release(content);
    }
  }

  @Override
  public DeleteWithMetaResponse decode(final ByteBuf response, ChannelContext ctx) {
    ResponseStatus status = MemcacheProtocol.decodeStatus(response);
    return new DeleteWithMetaResponse(
      status,
      cas(response),
      extractToken(ctx.mutationTokensEnabled(), partition(), response, ctx.bucket().get())
    );
  }
}
