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
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestBuilder.FramingExtrasStage;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.putMutationFlexibleExtras;

/**
 * Uses the KV "set" command to unconditionally replace or insert documents regardless if they
 * exist or not.
 *
 * @since 2.0.0
 */
public class UpsertRequest extends BasePortableKeyValueRequest<UpsertResponse> implements SyncDurabilityRequest {

  private final byte[] content;
  private final long expiration;
  private final boolean preserveExpiry;
  private final int flags;
  private final Optional<DurabilityLevel> syncReplicationType;

  public UpsertRequest(final String key, final byte[] content,
                       final long expiration, final boolean preserveExpiry, final int flags, final Duration timeout,
                       final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                       final RetryStrategy retryStrategy,
                       final Optional<DurabilityLevel> syncReplicationType, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.content = content;
    this.expiration = expiration;
    this.preserveExpiry = preserveExpiry;
    this.flags = flags;
    this.syncReplicationType = syncReplicationType;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, TracingIdentifiers.SPAN_REQUEST_KV_UPSERT);
      applyLevelOnSpan(syncReplicationType, span, tip);
    }
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder, int opaque, KeyValueChannelContext ctx) {
    byte[] collection = collectionPrefix(ctx);

    byte datatype = 0;
    byte[] value = this.content;
    CompressionConfig config = ctx.compressionConfig();
    if (config != null && config.enabled() && this.content.length >= config.minSize()) {
      byte[] compressed = MemcacheProtocol.tryCompressionToArray(this.content, config.minRatio());
      if (compressed != null) {
        datatype |= MemcacheProtocol.Datatype.SNAPPY.datatype();
        value = compressed;
      }
    }

    FramingExtrasStage request = encoder.beginRequest(MemcacheProtocol.Opcode.SET, opaque)
      .partition(partition())
      .datatype(datatype)
      .startFramingExtras();
    putMutationFlexibleExtras(request, this, ctx, syncReplicationType, preserveExpiry);
    request.startExtras().putInt(flags).putInt((int) expiration)
      .startKey().putBytes(collection).putBytes(key())
      .value(value);
  }

  @Override
  public UpsertResponse decode(final KeyValuePacket response, KeyValueChannelContext ctx) {
    return new UpsertResponse(
      response.responseStatus(),
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
    return "upsert";
  }
}
