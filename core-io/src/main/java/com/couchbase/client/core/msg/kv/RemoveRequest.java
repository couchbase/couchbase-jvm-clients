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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.putMutationFlexibleExtras;

/**
 * Represents a KV delete operation.
 *
 * @since 2.0.0
 */
public class RemoveRequest extends BasePortableKeyValueRequest<RemoveResponse> implements SyncDurabilityRequest {

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
  public void encodeTo(
    KeyValueRequestEncoder encoder,
    KeyValueChannelContext ctx
  ) {
    KeyValueRequestBuilder request = beginDocumentRequest(encoder, MemcacheProtocol.Opcode.DELETE, ctx)
      .cas(cas);
    putMutationFlexibleExtras(request, this, ctx, syncReplicationType, false);
  }

  @Override
  public RemoveResponse decode(
    KeyValuePacket response,
    KeyValueChannelContext ctx
  ) {
    return new RemoveResponse(
      response.responseStatus(),
      response.cas(),
      response.mutationToken(ctx.mutationTokensEnabled(), partition(), ctx.bucket().get()),
      response.flexibleExtras()
    );
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
