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
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;

public class UnlockRequest extends BaseKeyValueRequest<UnlockResponse> {

  public static final String OPERATION_NAME = "unlock";

  private final long cas;

  public UnlockRequest(Duration timeout, CoreContext ctx, CollectionIdentifier collectionIdentifier,
                       RetryStrategy retryStrategy, String key, long cas, InternalSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.cas = cas;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    try {
      key = encodedKeyWithCollection(alloc, ctx);
      return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.UNLOCK, noDatatype(),
        partition(), opaque, cas, noExtras(), key, noBody());
    } finally {
      ReferenceCountUtil.release(key);
    }
  }

  @Override
  public UnlockResponse decode(ByteBuf response, KeyValueChannelContext ctx) {
    return new UnlockResponse(decodeStatus(response), cas(response), Optional.empty());
  }
}
