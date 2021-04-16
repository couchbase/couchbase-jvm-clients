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

package com.couchbase.client.core.msg.search;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.io.netty.HttpChannelContext;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class GenericSearchRequest extends BaseRequest<GenericSearchResponse>
  implements NonChunkedHttpRequest<GenericSearchResponse> {

  private final Supplier<FullHttpRequest> requestSupplier;
  private final boolean idempotent;
  private final String indexName;
  private final String path;

  public GenericSearchRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                              final Supplier<FullHttpRequest> requestSupplier, boolean idempotent, RequestSpan span,
                              final String indexName, final String path) {
    super(timeout, ctx, retryStrategy);
    this.requestSupplier = requireNonNull(requestSupplier);
    this.idempotent = idempotent;
    this.indexName = indexName;
    this.path = path;

    if (span != null) {
      span.setAttribute(TracingIdentifiers.ATTR_SERVICE, TracingIdentifiers.SERVICE_SEARCH);

      FullHttpRequest request = requestSupplier.get();
      span.setAttribute(TracingIdentifiers.ATTR_OPERATION, request.method().toString() + " " + request.uri());
    }
  }

  @Override
  public GenericSearchResponse decode(final FullHttpResponse response, final HttpChannelContext  context) {
    byte[] dst = ByteBufUtil.getBytes(response.content());
    return new GenericSearchResponse(decodeStatus(response.status()), dst);
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = requestSupplier.get();
    context().authenticator().authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.SEARCH;
  }

  @Override
  public boolean idempotent() {
    return idempotent;
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = new TreeMap<>();
    ctx.put("type", serviceType().ident());
    ctx.put("path", redactMeta(path));
    if (!isNullOrEmpty(indexName)) {
      ctx.put("indexName", redactMeta(indexName));
    }
    return ctx;
  }

}
