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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;

/**
 * Represents a NOOP KV Request, doing nothing. really.
 *
 * @since 2.0.0
 */
public class NoopRequest extends BaseKeyValueRequest<NoopResponse> {

  public NoopRequest(final Duration timeout, final CoreContext ctx,
                     final RetryStrategy retryStrategy, CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier);
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.NOOP, noDatatype(), noPartition(),
      opaque, noCas(), noExtras(), noKey(), noBody());
  }

  @Override
  public NoopResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    return new NoopResponse(MemcacheProtocol.decodeStatus(response));
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "noop";
  }
}
