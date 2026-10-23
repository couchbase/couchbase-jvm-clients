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
import com.couchbase.client.core.api.kv.AbsentDocumentStrategy;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;

import static com.couchbase.client.core.api.kv.AbsentDocumentStrategy.THROW_EXCEPTION;

/**
 * Represents a KV Get (full document) operation.
 *
 * @since 2.0.0
 */
public class GetRequest extends BasePortableKeyValueRequest<GetResponse> {

  private final AbsentDocumentStrategy absentDocumentStrategy;

  public GetRequest(final String key, final Duration timeout, final CoreContext ctx,
                    final CollectionIdentifier collectionIdentifier, final RetryStrategy retryStrategy,
                    final RequestSpan span) {
    this(key, timeout, ctx, collectionIdentifier, retryStrategy, span, THROW_EXCEPTION);
  }

  public GetRequest(final String key, final Duration timeout, final CoreContext ctx,
                    final CollectionIdentifier collectionIdentifier, final RetryStrategy retryStrategy,
                    final RequestSpan span, final AbsentDocumentStrategy absentDocumentStrategy) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.absentDocumentStrategy = absentDocumentStrategy;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, absentDocumentStrategy.spanName);
    }
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    beginDocumentRequest(encoder, Opcode.GET);
  }

  @Override
  public GetResponse decode(KeyValuePacket response, KeyValueChannelContext ctx) {
    ResponseStatus status = response.responseStatus();
    long cas = response.cas();

    if (status.success()) {
      byte[] content = response.bodyAsBytes();
      int flags = response.extrasAsInt(0, 0);
      return new GetResponse(status, content, cas, flags);
    } else {
      return new GetResponse(status, null, cas, 0);
    }
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return absentDocumentStrategy == THROW_EXCEPTION ? "get" : "get_or_null";
  }

}
