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
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.io.netty.kv.EncodeContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.cas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;

public class PrependRequest extends BaseKeyValueRequest<PrependResponse> {

  private final byte[] content;
  private final long cas;

  public PrependRequest(Duration timeout, CoreContext ctx, String bucket,
                        RetryStrategy retryStrategy, String key, byte[] collection, byte[] content,
                        long cas) {
    super(timeout, ctx, bucket, retryStrategy, key, collection);
    this.content = content;
    this.cas = cas;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, EncodeContext ctx) {
    ByteBuf key = Unpooled.wrappedBuffer(ctx.collectionsEnabled() ? keyWithCollection() : key());

    byte datatype = 0;
    ByteBuf content;
    CompressionConfig config = ctx.compressionConfig();
    if (config != null && config.enabled() && this.content.length >= config.minSize()) {
      ByteBuf maybeCompressed = MemcacheProtocol.tryCompression(this.content, config.minRatio());
      if (maybeCompressed != null) {
        datatype |= MemcacheProtocol.Datatype.SNAPPY.datatype();
        content = maybeCompressed;
      } else {
        content = Unpooled.wrappedBuffer(this.content);
      }
    } else {
      content = Unpooled.wrappedBuffer(this.content);
    }
    ByteBuf r = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.PREPEND, datatype, partition(),
      opaque, cas, noExtras(), key, content);

    key.release();
    content.release();

    return r;
  }

  @Override
  public PrependResponse decode(ByteBuf response) {
    return new PrependResponse(decodeStatus(response), cas(response), Optional.empty());
  }
}
