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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.io.SectionReader;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;

import java.time.Duration;

public class ObserveViaCasRequest extends BaseKeyValueRequest<ObserveViaCasResponse> {

  private final int replica;
  private final boolean active;

  public ObserveViaCasRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                              final RetryStrategy retryStrategy, final String key,
                              boolean active, int replica) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, null);
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
  public void encodeTo(KeyValueRequestEncoder encoder) {
    byte[] collection = collectionPrefix(encoder.context());
    encoder.beginRequest(Opcode.OBSERVE_CAS)
      .partition(partition())
      .valueWriter()
      .putShort(partition())
      .putShort(collection.length + key().length)
      .putBytes(collection)
      .putBytes(key());
  }

  @Override
  public ObserveViaCasResponse decode(KeyValuePacket response, KeyValueChannelContext ctx) {
    ResponseStatus status = response.responseStatus();
    byte observed = ObserveViaCasResponse.ObserveStatus.UNKNOWN.value();
    long observedCas = 0;
    ResponseStatusDetails statusDetails = null;
    if (status.success()) {
      SectionReader content = response.body();
      content.skip(Short.BYTES); // the partition
      int keyLength = content.readUnsignedShort();
      content.skip(keyLength);
      observed = content.readByte();
      observedCas = content.readLong();
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
