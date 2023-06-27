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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.bodyAsString;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;

public class GetCollectionManifestRequest extends BaseKeyValueRequest<GetCollectionManifestResponse> {

  public GetCollectionManifestRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                                      final CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier);
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque, final KeyValueChannelContext ctx) {
    return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.COLLECTIONS_GET_MANIFEST, noDatatype(),
      noPartition(), opaque, noCas(), noExtras(), noKey(), noBody());
  }

  @Override
  public GetCollectionManifestResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    ResponseStatus status = MemcacheProtocol.decodeStatus(response);
    Optional<String> manifest = status.success()
      ? Optional.of(bodyAsString(response))
      : Optional.empty();
    return new GetCollectionManifestResponse(status, manifest);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "get_collection_manifest";
  }

}
