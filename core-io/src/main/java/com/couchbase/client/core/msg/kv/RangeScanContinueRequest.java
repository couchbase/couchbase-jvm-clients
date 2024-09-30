/*
 * Copyright (c) 2022 Couchbase, Inc.
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
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.kv.CoreRangeScanId;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.CoreScanOptions;
import com.couchbase.client.core.msg.ResponseStatus;
import reactor.core.publisher.Sinks;


import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode.RANGE_SCAN_CONTINUE;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;

public class RangeScanContinueRequest extends PredeterminedPartitionRequest<RangeScanContinueResponse> {

  private final CoreRangeScanId id;
  private final int itemLimit;
  private final int byteLimit;
  private final int timeLimit;
  private final Sinks.Many<CoreRangeScanItem> sink;

  private final boolean keysOnly;

  public RangeScanContinueRequest(CoreRangeScanId id,
                                  Sinks.Many<CoreRangeScanItem> sink,
                                  String key,
                                  CoreScanOptions options,
                                  short partition,
                                  CoreContext ctx,
                                  CollectionIdentifier collectionIdentifier ) {

    super(
      partition,
      options.commonOptions().timeout().orElse(ctx.environment().timeoutConfig().kvScanTimeout()),
      ctx,
      options.commonOptions().retryStrategy().orElse(ctx.environment().retryStrategy()),
      key,
      collectionIdentifier,
      ctx.coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_RANGE_SCAN_CONTINUE, options.commonOptions().parentSpan().orElse(null)));
    this.id = id;
    this.itemLimit = options.batchItemLimit();
    this.byteLimit = options.batchByteLimit();
    this.timeLimit = Math.toIntExact(timeout().toMillis());
    this.sink = sink;
    this.keysOnly = options.idsOnly();
  }


  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque, final KeyValueChannelContext ctx) {
    ByteBuf extras = alloc.buffer(Integer.BYTES * 3 + id.bytes().length);
    try {
      extras.writeBytes(id.bytes());
      extras.writeInt(itemLimit);
      extras.writeInt(timeLimit);
      extras.writeInt(byteLimit);
      return MemcacheProtocol.request(alloc, RANGE_SCAN_CONTINUE, noDatatype(), partition(), opaque,
        noCas(), extras, noKey(), noBody());
    } finally {
      extras.release();
    }
  }

  @Override
  public RangeScanContinueResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    return new RangeScanContinueResponse(status, sink, keysOnly);
  }

  public CoreRangeScanId rangeScanId() {
    return id;
  }
}
