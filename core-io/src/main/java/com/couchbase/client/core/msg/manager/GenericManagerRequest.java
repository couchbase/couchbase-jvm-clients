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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponse;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.core.io.netty.HttpProtocol.addHttpBasicAuth;
import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static java.util.Objects.requireNonNull;

public class GenericManagerRequest extends BaseManagerRequest<GenericManagerResponse> {

  private final Supplier<FullHttpRequest> requestSupplier;

  public GenericManagerRequest(CoreContext ctx, Supplier<FullHttpRequest> requestSupplier) {
    this(ctx.environment().timeoutConfig().managerTimeout(), ctx, ctx.environment().retryStrategy(), requestSupplier);
  }

  public GenericManagerRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy, Supplier<FullHttpRequest> requestSupplier) {
    super(timeout, ctx, retryStrategy);
    this.requestSupplier = requireNonNull(requestSupplier);
  }

  @Override
  public GenericManagerResponse decode(HttpResponse response, byte[] content) {
    return new GenericManagerResponse(decodeStatus(response.status()), content);
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = requestSupplier.get();
    addHttpBasicAuth(request, context().environment().credentials());
    return request;
  }
}
