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
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;

public class ObserveViaCasRequest extends BaseKeyValueRequest<ObserveViaCasResponse> {

  public ObserveViaCasRequest(final Duration timeout, final CoreContext ctx, final String bucket,
                              final RetryStrategy retryStrategy, final String key,
                              final byte[] collection) {
    super(timeout, ctx, bucket, retryStrategy, key, collection);
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    byte[] key = ctx.collectionsEnabled() ? keyWithCollection() : key();
    int keyLength = key.length;
    ByteBuf content = alloc.buffer(keyLength + 4);
    content.writeShort(partition());
    content.writeShort(keyLength);
    content.writeBytes(key);

    ByteBuf request = request(alloc, MemcacheProtocol.Opcode.OBSERVE_CAS, noDatatype(),
      partition(), opaque, noCas(), noExtras(), noKey(), content);
    content.release();
    return request;
  }

  @Override
  public ObserveViaCasResponse decode(final ByteBuf response, ChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    byte observed = ObserveViaCasResponse.ObserveStatus.UNKNOWN.value();
    long observedCas = 0;
    if (status.success()) {
      ByteBuf content = body(response).get();
      short keyLength = content.getShort(2);
      observed = content.getByte(keyLength + 4);
      observedCas = content.getLong(keyLength + 5);
    }
    return new ObserveViaCasResponse(
      status,
      observedCas,
      ObserveViaCasResponse.ObserveStatus.valueOf(observed)
    );
  }


}
