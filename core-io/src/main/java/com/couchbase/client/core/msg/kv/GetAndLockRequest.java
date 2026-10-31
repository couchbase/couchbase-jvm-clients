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
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.kv.KeyValuePacket;
import com.couchbase.client.core.io.kv.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.CbDurations;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;

/**
 * Represents a KV GetAndTouch operation.
 *
 * @since 2.0.0
 */
public class GetAndLockRequest extends BaseKeyValueRequest<GetAndLockResponse> {

  private final Duration lockFor;

  public GetAndLockRequest(final String key, final Duration timeout, final CoreContext ctx,
                           final CollectionIdentifier collectionIdentifier, final RetryStrategy retryStrategy,
                           final Duration lockFor, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.lockFor = lockFor;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.OPERATION, span, TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_LOCK);
    }
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    long secondsToLock = CbDurations.getSecondsCeil(lockFor);
    beginDocumentRequest(encoder, Opcode.GET_AND_LOCK)
      .extras().putInt(Math.toIntExact(secondsToLock));
  }

  @Override
  public GetAndLockResponse decode(
    KeyValuePacket response,
    KeyValueChannelContext ctx
  ) {
    ResponseStatus status = response.responseStatus();
    long cas = response.cas();
    if (status.success()) {
      byte[] content = response.bodyAsBytes();
      int flags = response.extrasAsInt(0, 0);
      return new GetAndLockResponse(status, content, cas, flags);
    } else {
      return new GetAndLockResponse(status, null, cas, 0);
    }
  }

  @Override
  public String name() {
    return "get_and_lock";
  }
}
