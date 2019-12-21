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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

public class KvPingRequest extends NoopRequest implements TargetedRequest {

  private final NodeIdentifier target;

  public KvPingRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy, CollectionIdentifier identifier,
                       NodeIdentifier target) {
    super(timeout, ctx, retryStrategy, identifier);
    this.target = target;
  }

  @Override
  public KvPingResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    return new KvPingResponse(MemcacheProtocol.decodeStatus(response), ctx.channelId().asShortText());
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }

}
