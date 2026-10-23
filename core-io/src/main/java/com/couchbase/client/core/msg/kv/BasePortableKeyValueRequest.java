/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

/**
 * The base of the {@link PortableKeyValueRequest}s. Makes the {@code ByteBuf} bridges final, so a subclass
 * can't override them: the socket KV implementation doesn't call them, so it would quietly ignore the override.
 * A subclass overrides {@link #encodeTo} and {@link #decode(com.couchbase.client.core.io.KeyValuePacket, KeyValueChannelContext)} instead.
 * <p>
 * Transitional, like {@link PortableKeyValueRequest}.
 */
@Stability.Internal
public abstract class BasePortableKeyValueRequest<R extends Response>
  extends BaseKeyValueRequest<R>
  implements PortableKeyValueRequest<R> {

  protected BasePortableKeyValueRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                                           final String key, final CollectionIdentifier collectionIdentifier, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
  }

  @Override
  public final ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    return PortableKeyValueRequest.super.encode(alloc, opaque, ctx);
  }

  @Override
  public final R decode(ByteBuf response, KeyValueChannelContext ctx) {
    return PortableKeyValueRequest.super.decode(response, ctx);
  }
}
