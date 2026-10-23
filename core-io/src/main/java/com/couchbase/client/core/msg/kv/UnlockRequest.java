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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;


public class UnlockRequest extends BasePortableKeyValueRequest<UnlockResponse> {
  private final long cas;

  public UnlockRequest(Duration timeout, CoreContext ctx, CollectionIdentifier collectionIdentifier,
                       RetryStrategy retryStrategy, String key, long cas, RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.cas = cas;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, TracingIdentifiers.SPAN_REQUEST_KV_UNLOCK);
    }
  }

  @Override
  public void encodeTo(
    KeyValueRequestEncoder encoder,
    KeyValueChannelContext ctx
  ) {
    beginDocumentRequest(encoder, MemcacheProtocol.Opcode.UNLOCK, ctx)
      .cas(cas);
  }

  @Override
  public UnlockResponse decode(
    KeyValuePacket response,
    KeyValueChannelContext ctx
  ) {
    return new UnlockResponse(
      response.responseStatus(),
      response.cas(),
      Optional.empty()
    );
  }

  @Override
  public String name() {
    return "unlock";
  }
}
