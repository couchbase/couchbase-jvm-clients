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
import com.couchbase.client.core.io.BodyReader;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestBuilder.SectionWriter;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.util.UnsignedLEB128;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;

/**
 * Special observe request implementation to handle more than one key at the same time.
 */
public class MultiObserveViaCasRequest
  extends BasePortableKeyValueRequest<MultiObserveViaCasResponse>
  implements TargetedRequest {

  private final NodeIdentifier target;
  private final Map<byte[], Short> keys;
  private final Predicate<ObserveViaCasResponse.ObserveStatus> responsePredicate;

  public MultiObserveViaCasRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                   CollectionIdentifier collectionIdentifier, NodeIdentifier target,
                                   Map<byte[], Short> keys,
                                   Predicate<ObserveViaCasResponse.ObserveStatus> responsePredicate) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier, null);
    this.target = target;
    this.keys = keys;
    this.responsePredicate = responsePredicate;
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    KeyValueChannelContext ctx = encoder.context();
    SectionWriter content = encoder.beginRequest(Opcode.OBSERVE_CAS)
      .partition(partition())
      .valueWriter();
    for (Map.Entry<byte[], Short> key : keys.entrySet()) {
      byte[] collection = collectionPrefix(ctx, key.getKey());
      content
        .putShort(key.getValue())
        .putShort(collection.length + key.getKey().length)
        .putBytes(collection)
        .putBytes(key.getKey());
    }
  }

  @Override
  public MultiObserveViaCasResponse decode(KeyValuePacket response, KeyValueChannelContext ctx) {
    final ResponseStatus status = response.responseStatus();

    final Map<byte[], ObserveViaCasResponse.ObserveStatus> observed = new HashMap<>();
    if (status.success()) {
      BodyReader content = response.body();
      while (content.remaining() > 0) {
        content.skip(Short.BYTES); // skip the vbid
        int keyLength = content.readUnsignedShort();
        byte[] keyEncoded = content.readBytes(keyLength);
        if (ctx.collectionsEnabled()) {
          int collectionIdLength = UnsignedLEB128.encodedLength(keyEncoded);
          keyEncoded = Arrays.copyOfRange(keyEncoded, collectionIdLength, keyEncoded.length);
        }
        byte obs = content.readByte();
        content.skip(Long.BYTES); // skip reading the cas
        ObserveViaCasResponse.ObserveStatus decoded = ObserveViaCasResponse.ObserveStatus.valueOf(obs);
        if (responsePredicate.test(decoded)) {
          observed.put(keyEncoded, decoded);
        }
      }
    }

    return new MultiObserveViaCasResponse(this, status, observed);
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "multi_observe_via_cas";
  }

  @Override
  public Map<String, Object> serviceContext() {
    final Map<String, Object> parentCtx = super.serviceContext();
    parentCtx.put("target", target);
    parentCtx.put("numKeys", keys.size());
    return parentCtx;
  }

}
