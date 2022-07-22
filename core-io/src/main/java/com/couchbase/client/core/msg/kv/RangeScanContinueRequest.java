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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.kv.CoreRangeScanId;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.core.publisher.Sinks;

import java.time.Duration;

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

  public RangeScanContinueRequest(CoreRangeScanId id, int itemLimit, int byteLimit, Duration timeout, CoreContext ctx,
                                  RetryStrategy retryStrategy, String key, CollectionIdentifier collectionIdentifier,
                                  RequestSpan span, Sinks.Many<CoreRangeScanItem> sink, short partition, boolean keysOnly) {
    super(partition, timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.id = id;
    this.itemLimit = itemLimit;
    this.byteLimit = byteLimit;
    this.timeLimit = Math.toIntExact(timeout.toMillis());
    this.sink = sink;
    this.keysOnly = keysOnly;
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
