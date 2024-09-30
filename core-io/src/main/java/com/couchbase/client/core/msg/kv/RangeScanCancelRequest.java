/*
 * Copyright (c) 2023 Couchbase, Inc.
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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.kv.CoreRangeScanId;
import com.couchbase.client.core.kv.CoreScanOptions;
import com.couchbase.client.core.msg.ResponseStatus;


import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode.RANGE_SCAN_CANCEL;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;

@Stability.Internal
public class RangeScanCancelRequest extends PredeterminedPartitionRequest<RangeScanCancelResponse> {

  private final CoreRangeScanId id;

  public RangeScanCancelRequest(CoreRangeScanId id,
                                CoreScanOptions options,
                                short partition,
                                CoreContext ctx,
                                CollectionIdentifier collectionIdentifier ) {

    super(
      partition,
      options.commonOptions().timeout().orElse(ctx.environment().timeoutConfig().kvScanTimeout()),
      ctx,
      options.commonOptions().retryStrategy().orElse(ctx.environment().retryStrategy()),
      null,
      collectionIdentifier,
      ctx.coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_RANGE_SCAN_CANCEL, options.commonOptions().parentSpan().orElse(null)));
    this.id = id;
  }


  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque, final KeyValueChannelContext ctx) {
    ByteBuf extras = alloc.buffer(id.bytes().length);
    try {
      extras.writeBytes(id.bytes());
      return MemcacheProtocol.request(alloc, RANGE_SCAN_CANCEL, noDatatype(), partition(), opaque,
        noCas(), extras, noKey(), noBody());
    } finally {
      extras.release();
    }
  }

  @Override
  public RangeScanCancelResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    return new RangeScanCancelResponse(status);
  }

}
