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
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

public class ObserveViaCasRequest extends BaseKeyValueRequest<ObserveViaCasResponse> {

  private final int replica;
  private final boolean active;

  public ObserveViaCasRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                              final RetryStrategy retryStrategy, final String key,
                              boolean active, int replica) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier);
    this.active = active;
    this.replica = replica;
  }

  @Override
  public int replica() {
    return replica;
  }

  public boolean active() {
    return active;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf content = null;

    try {
      key = encodedKeyWithCollection(alloc, ctx);
      int keyLength = key.readableBytes();
      content = alloc.buffer(keyLength + (Short.BYTES * 2));
      content.writeShort(partition());
      content.writeShort(keyLength);
      content.writeBytes(key);

      return request(alloc, MemcacheProtocol.Opcode.OBSERVE_CAS, noDatatype(),
        partition(), opaque, noCas(), noExtras(), noKey(), content);
    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(content);
    }
  }

  @Override
  public ObserveViaCasResponse decode(final ByteBuf response, KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    byte observed = ObserveViaCasResponse.ObserveStatus.UNKNOWN.value();
    long observedCas = 0;
    ResponseStatusDetails statusDetails = null;
    if (status.success()) {
      ByteBuf content = body(response).get();
      short keyLength = content.getShort(2);
      observed = content.getByte(keyLength + 4);
      observedCas = content.getLong(keyLength + 5);
    } else {
      // TODO: implement once xerror is fully implemented
      statusDetails = null; //ResponseStatus.convertDetails(datatype(response), );
    }
    return new ObserveViaCasResponse(
      status,
      observedCas,
      ObserveViaCasResponse.ObserveStatus.valueOf(observed),
      active,
      statusDetails
    );
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "observe_via_cas";
  }

}
