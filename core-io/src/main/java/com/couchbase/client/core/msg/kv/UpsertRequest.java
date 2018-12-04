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
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;

/**
 * Uses the KV "set" command to unconditionally replace or insert documents regardless if they
 * exist or not.
 *
 * @since 2.0.0
 */
public class UpsertRequest extends BaseKeyValueRequest<UpsertResponse> implements Compressible {

  private final byte[] key;
  private final byte[] content;
  private final long expiration;
  private final int flags;

  public UpsertRequest(final String key, final byte[] content, final long expiration,
                       final int flags, final Duration timeout,
                       final CoreContext ctx, final RetryStrategy retryStrategy) {
    super(timeout, ctx, retryStrategy);
    this.key = encodeKey(key);
    this.content = content;
    this.expiration = expiration;
    this.flags = flags;
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque,
                        final CompressionConfig config) {
    ByteBuf key = Unpooled.wrappedBuffer(this.key);

    byte datatype = 0;
    ByteBuf content;
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

    ByteBuf extras = alloc.buffer(8);
    extras.writeInt(flags);
    extras.writeInt((int) expiration);

    ByteBuf r = MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.SET, datatype, partition(),
      opaque, noCas(), extras, key, content);

    key.release();
    extras.release();
    content.release();

    return r;
  }

  @Override
  public UpsertResponse decode(final ByteBuf response) {
    ResponseStatus status = MemcacheProtocol.decodeStatus(response);
    return new UpsertResponse(status);
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque) {
    return encode(alloc, opaque, null);
  }

}
