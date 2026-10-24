/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Fetches the collection ID from the cluster based on a {@link CollectionIdentifier}.
 */
public class GetCollectionIdRequest extends BasePortableKeyValueRequest<GetCollectionIdResponse> {

  public GetCollectionIdRequest(final Duration timeout, final CoreContext ctx,
                                final RetryStrategy retryStrategy,
                                final CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier, null);
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    CollectionIdentifier ci = collectionIdentifier();
    if (!ci.collection().isPresent()) {
      throw InvalidArgumentException.fromMessage("A collection name needs to be present");
    }

    // Note that the scope can be empty, according to spec it is the same as _default scope
    byte[] body = (ci.scope().orElse("") + "." + ci.collection().get()).getBytes(UTF_8);
    encoder.beginRequest(Opcode.COLLECTIONS_GET_CID).value(body);
  }

  @Override
  public GetCollectionIdResponse decode(KeyValuePacket response, KeyValueChannelContext ctx) {
    ResponseStatus status = response.responseStatus();
    Optional<Long> cid = Optional.empty();
    if (status.success()) {
      if (response.extrasLength() < 12) {
        throw new RuntimeException("Successful GET_CID response has no collection ID in its extras");
      }
      cid = Optional.of(Integer.toUnsignedLong(response.extrasAsInt(8, 0)));
    }
    return new GetCollectionIdResponse(status, cid);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "get_collection_id";
  }

}
