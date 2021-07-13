/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static java.util.Objects.requireNonNull;

/**
 * @deprecated in favor of issuing manager requests using
 * {@link com.couchbase.client.core.endpoint.http.CoreHttpClient}.
 * CAVEAT: the core HTTP client throws an exception if the response's
 * HTTP status code indicates failure. This is in contrast to
 * GenericManagerRequest, whose response completes "successfully"
 * regardless of HTTP status code.
 */
@Deprecated
public class GenericManagerRequest extends BaseManagerRequest<GenericManagerResponse> {

  private final Supplier<FullHttpRequest> requestSupplier;
  private final boolean idempotent;

  public GenericManagerRequest(CoreContext ctx, Supplier<FullHttpRequest> requestSupplier, boolean idempotent, RequestSpan span) {
    this(ctx.environment().timeoutConfig().managementTimeout(), ctx, ctx.environment().retryStrategy(), requestSupplier,
        idempotent, span);
  }

  public GenericManagerRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                               Supplier<FullHttpRequest> requestSupplier, boolean idempotent, RequestSpan span) {
    super(timeout, ctx, retryStrategy, span);
    this.requestSupplier = requireNonNull(requestSupplier);
    this.idempotent = idempotent;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      span.attribute(TracingIdentifiers.ATTR_SERVICE, TracingIdentifiers.SERVICE_MGMT);

      FullHttpRequest request = requestSupplier.get();
      span.attribute(TracingIdentifiers.ATTR_OPERATION, request.method().toString() + " " + request.uri());
    }
  }

  @Override
  public GenericManagerResponse decode(HttpResponse response, byte[] content) {
    return new GenericManagerResponse(decodeStatus(response.status()), content, response.status().code());
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = requestSupplier.get();
    context().authenticator().authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public boolean idempotent() {
    return idempotent;
  }
}
