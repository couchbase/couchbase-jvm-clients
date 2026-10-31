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
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.kv.KeyValuePacket;
import com.couchbase.client.core.io.kv.KeyValueRequestBuilder;
import com.couchbase.client.core.io.kv.KeyValueRequestBuilder.SectionWriter;
import com.couchbase.client.core.io.kv.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.putMutationFlexibleExtras;

public class DecrementRequest extends BaseKeyValueRequest<DecrementResponse> implements SyncDurabilityRequest {

  private final long delta;
  private final Optional<Long> initial;
  private final long expiry;
  private final Optional<DurabilityLevel> syncReplicationType;

  public DecrementRequest(Duration timeout, CoreContext ctx, CollectionIdentifier collectionIdentifier,
                          RetryStrategy retryStrategy, String key,
                          long delta, Optional<Long> initial, long expiry,
                          final Optional<DurabilityLevel> syncReplicationType, RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    if (initial.isPresent() && initial.get() < 0) {
      throw InvalidArgumentException.fromMessage("The initial needs to be >= 0");
    }
    this.delta = delta;
    this.initial = initial;
    this.expiry = expiry;
    this.syncReplicationType = syncReplicationType;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, TracingIdentifiers.SPAN_REQUEST_KV_DECREMENT);
      applyLevelOnSpan(syncReplicationType, span, tip);
    }
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    KeyValueRequestBuilder request = beginDocumentRequest(encoder, Opcode.DECREMENT);
    putMutationFlexibleExtras(request, this, syncReplicationType, false);
    SectionWriter extras = request.extras().putLong(delta);
    if (initial.isPresent()) {
      extras.putLong(initial.get()).putInt((int) expiry);
    } else {
      extras.putLong(0).putInt(IncrementRequest.COUNTER_NOT_EXISTS_EXPIRY); // no initial present, will lead to doc not found
    }
  }

  @Override
  public DecrementResponse decode(
    KeyValuePacket response,
    KeyValueChannelContext ctx
  ) {
    return new DecrementResponse(
      response.responseStatus(),
      response.hasBody() ? response.body().readLong() : 0,
      response.cas(),
      response.mutationToken(ctx.mutationTokensEnabled(), partition(), ctx.bucket().get())
    );
  }

  @Override
  public Optional<DurabilityLevel> durabilityLevel() {
    return syncReplicationType;
  }

  @Override
  public String name() {
    return "decrement";
  }
}
