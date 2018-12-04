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
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.datatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.tryDecompression;

/**
 * Represents a KV delete operation.
 *
 * @since 2.0.0
 */
public class RemoveRequest extends BaseKeyValueRequest<RemoveResponse> {

  private final long cas;

  public RemoveRequest(final String key, final long cas, final Duration timeout,
                       final CoreContext ctx, final String bucket,
                       final RetryStrategy retryStrategy) {
    super(timeout, ctx, bucket, retryStrategy, key);
    this.cas = cas;
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque) {
    ByteBuf key = Unpooled.wrappedBuffer(key());
    ByteBuf r = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.DELETE, noDatatype(),
      partition(), opaque, cas, noExtras(), key, noBody());
    key.release();
    return r;
  }

  @Override
  public RemoveResponse decode(final ByteBuf response) {
    ResponseStatus status = decodeStatus(response);
    return new RemoveResponse(status);
  }

}
