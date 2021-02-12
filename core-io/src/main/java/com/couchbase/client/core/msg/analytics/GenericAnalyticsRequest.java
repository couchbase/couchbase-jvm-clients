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

package com.couchbase.client.core.msg.analytics;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.io.netty.HttpChannelContext;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static java.util.Objects.requireNonNull;

public class GenericAnalyticsRequest extends BaseRequest<GenericAnalyticsResponse>
  implements NonChunkedHttpRequest<GenericAnalyticsResponse> {

  private final Supplier<FullHttpRequest> requestSupplier;
  private final boolean idempotent;

  public GenericAnalyticsRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                                 final Supplier<FullHttpRequest> requestSupplier, boolean idempotent, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, span);
    this.requestSupplier = requireNonNull(requestSupplier);
    this.idempotent = idempotent;

    if (span != null) {
      span.setAttribute(TracingIdentifiers.ATTR_SERVICE, TracingIdentifiers.SERVICE_ANALYTICS);

      FullHttpRequest request = requestSupplier.get();
      span.setAttribute(TracingIdentifiers.ATTR_OPERATION, request.method().toString() + " " + request.uri());
    }
  }

  @Override
  public GenericAnalyticsResponse decode(final FullHttpResponse response, HttpChannelContext context) {
    byte[] dst = new byte[response.content().readableBytes()];
    response.content().readBytes(dst);
    return new GenericAnalyticsResponse(decodeStatus(response.status()), dst);
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = requestSupplier.get();
    context().authenticator().authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.ANALYTICS;
  }

  @Override
  public boolean idempotent() {
    return idempotent;
  }

}
