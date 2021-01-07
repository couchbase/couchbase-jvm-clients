/*
 * Copyright (c) 2020 Couchbase, Inc.
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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.UnsignedLEB128;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;

/**
 * Special observe request implementation to handle more than one key at the same time.
 */
public class MultiObserveViaCasRequest
  extends BaseKeyValueRequest<MultiObserveViaCasResponse>
  implements TargetedRequest {

  private final NodeIdentifier target;
  private final Map<byte[], Short> keys;
  private final Predicate<ObserveViaCasResponse.ObserveStatus> responsePredicate;

  public MultiObserveViaCasRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                   CollectionIdentifier collectionIdentifier, NodeIdentifier target,
                                   Map<byte[], Short> keys,
                                   Predicate<ObserveViaCasResponse.ObserveStatus> responsePredicate) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier);
    this.target = target;
    this.keys = keys;
    this.responsePredicate = responsePredicate;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, KeyValueChannelContext ctx) {
    ByteBuf content = null;

    try {
      content = alloc.buffer(keys.size() * (Short.BYTES * 2));
      for (Map.Entry<byte[], Short> key : keys.entrySet()) {
        ByteBuf keyWithCollection = encodedExternalKeyWithCollection(alloc, ctx, key.getKey());
        try {
          content.writeShort(key.getValue());
          content.writeShort(keyWithCollection.readableBytes());
          content.writeBytes(keyWithCollection);
        } finally {
          ReferenceCountUtil.release(keyWithCollection);
        }
      }
      return request(alloc, MemcacheProtocol.Opcode.OBSERVE_CAS, noDatatype(),
        partition(), opaque, noCas(), noExtras(), noKey(), content);
    } finally {
      ReferenceCountUtil.release(content);
    }
  }

  @Override
  public MultiObserveViaCasResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    final ResponseStatus status = decodeStatus(response);

    final Map<byte[], ObserveViaCasResponse.ObserveStatus> observed = new HashMap<>();
    if (status.success()) {
      Optional<ByteBuf> maybeContent = body(response);
      if (maybeContent.isPresent()) {
        ByteBuf content = maybeContent.get();
        while (content.isReadable()) {
          content.skipBytes(Short.BYTES); // skip the vbid
          short keyLength = content.readShort();
          if (ctx.collectionsEnabled()) {
            int skipped = UnsignedLEB128.skip(content);
            keyLength = (short) (keyLength - skipped);
          }
          byte[] keyEncoded = new byte[keyLength];
          content.readBytes(keyEncoded, 0, keyLength);
          byte obs = content.readByte();
          content.skipBytes(Long.BYTES); // skip reading the cas
          ObserveViaCasResponse.ObserveStatus decoded = ObserveViaCasResponse.ObserveStatus.valueOf(obs);
          if (responsePredicate.test(decoded)) {
            observed.put(keyEncoded, decoded);
          }
        }
      }
    }

    return new MultiObserveViaCasResponse(status, observed);
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }

  @Override
  public boolean idempotent() {
    return true;
  }

}
