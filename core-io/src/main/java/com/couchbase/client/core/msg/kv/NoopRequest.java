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
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;

import java.time.Duration;

/**
 * Represents a NOOP KV Request, doing nothing. really.
 *
 * @since 2.0.0
 */
public class NoopRequest extends BaseKeyValueRequest<NoopResponse> {

  public NoopRequest(final Duration timeout, final CoreContext ctx,
                     final RetryStrategy retryStrategy, CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier, null);
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    encoder.beginRequest(Opcode.NOOP);
  }

  @Override
  public NoopResponse decode(KeyValuePacket response, KeyValueChannelContext ctx) {
    return new NoopResponse(response.responseStatus());
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "noop";
  }
}
