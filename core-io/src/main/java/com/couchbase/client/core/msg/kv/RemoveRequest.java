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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

/**
 * Represents a KV delete operation.
 *
 * @since 2.0.0
 */
public class RemoveRequest extends BaseKeyValueRequest<RemoveResponse> implements SyncDurabilityRequest {

  private final long cas;
  private final Optional<DurabilityLevel> syncReplicationType;

  public RemoveRequest(final String key, final long cas, final Duration timeout,
                       final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                       final RetryStrategy retryStrategy,
                       final Optional<DurabilityLevel> syncReplicationType, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.cas = cas;
    this.syncReplicationType = syncReplicationType;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, TracingIdentifiers.SPAN_REQUEST_KV_REMOVE);
      applyLevelOnSpan(syncReplicationType, span, tip);
    }
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf flexibleExtras = mutationFlexibleExtras(this, ctx, alloc, syncReplicationType);

    try {
      key = encodedKeyWithCollection(alloc, ctx);

      return MemcacheProtocol.flexibleRequest(alloc, MemcacheProtocol.Opcode.DELETE, noDatatype(),
            partition(), opaque, cas, flexibleExtras, noExtras(), key, noBody());

    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(flexibleExtras);
    }
  }

  @Override
  public RemoveResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    Optional<MutationToken> mutationToken = extractToken(
      ctx.mutationTokensEnabled(),
      partition(),
      response,
      ctx.bucket().get()
    );
    return new RemoveResponse(status, cas(response), mutationToken, flexibleExtras(response));
  }

  @Override
  public Optional<DurabilityLevel> durabilityLevel() {
    return syncReplicationType;
  }

  @Override
  public String name() {
    return "remove";
  }
}
