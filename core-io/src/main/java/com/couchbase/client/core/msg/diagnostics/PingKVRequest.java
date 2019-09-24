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

package com.couchbase.client.core.msg.diagnostics;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.kv.BaseKeyValueRequest;
import com.couchbase.client.core.msg.kv.NoopResponse;
import com.couchbase.client.core.msg.util.AssignChannelInfo;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;

public class PingKVRequest extends BaseKeyValueRequest<PingResponse> implements AssignChannelInfo {

  private String local;
  private String remote;

  public PingKVRequest(final Duration timeout, final CoreContext ctx,
                       final RetryStrategy retryStrategy, CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier);
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.NOOP, noDatatype(), noPartition(),
      opaque, noCas(), noExtras(), noKey(), noBody());
  }

  @Override
  public PingResponse decode(final ByteBuf response, ChannelContext ctx) {
    return new PingResponse(MemcacheProtocol.decodeStatus(response), null);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  public String local() {
    return local;
  }

  @Override
  public AssignChannelInfo local(String local) {
    this.local = local;
    return this;
  }

  public String remote() {
    return remote;
  }

  @Override
  public AssignChannelInfo remote(String remote) {
    this.remote = remote;
    return this;
  }

}
