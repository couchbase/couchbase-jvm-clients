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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.putMutationFlexibleExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.putValueMaybeCompressed;

/**
 * Uses the KV replace command to replace a document if it exists.
 *
 * @since 2.0.0
 */
public class ReplaceRequest extends BasePortableKeyValueRequest<ReplaceResponse> implements SyncDurabilityRequest {

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

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, TracingIdentifiers.SPAN_REQUEST_KV_REPLACE);
      applyLevelOnSpan(syncReplicationType, span, tip);
    }
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    KeyValueRequestBuilder request = beginDocumentRequest(encoder, Opcode.REPLACE)
      .cas(cas);
    putValueMaybeCompressed(request, content);
    putMutationFlexibleExtras(request, this, syncReplicationType, preserveExpiry);
    request.extras().putInt(flags).putInt((int) expiration);
  }

  @Override
  public ReplaceResponse decode(
    KeyValuePacket response,
    KeyValueChannelContext ctx
  ) {
    return new ReplaceResponse(
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
    return "replace";
  }

}
