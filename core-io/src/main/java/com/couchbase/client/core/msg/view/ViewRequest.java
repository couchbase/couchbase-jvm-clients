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

package com.couchbase.client.core.msg.view;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.ScopedRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

public class ViewRequest extends BaseRequest<ViewResponse>
  implements HttpRequest<ViewChunkHeader, ViewChunkRow, ViewChunkTrailer, ViewResponse>, ScopedRequest {

  private final Authenticator authenticator;
  private final String bucket;
  private final boolean development;
  private final String design;
  private final String view;
  private final String query;
  private final Optional<byte[]> keysJson;

  public ViewRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                     final Authenticator authenticator, final String bucket, final String design,
                     final String view, final String query, Optional<byte[]> keysJson,
                     final boolean development, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, span);
    this.authenticator = requireNonNull(authenticator);
    this.bucket = requireNonNull(bucket);
    this.design = requireNonNull(design);
    this.view = requireNonNull(view);
    this.development = development;
    this.query = requireNonNull(query);
    this.keysJson = requireNonNull(keysJson);

    if (span != null && !CbTracing.isInternalSpan(span)) {
      TracingDecorator tip = ctx.coreResources().tracingDecorator();
      tip.provideLowCardinalityAttr(TracingAttribute.SERVICE, span, TracingIdentifiers.SERVICE_VIEWS);
      tip.provideAttr(TracingAttribute.OPERATION, span, "/" + design + "/" + view);
      tip.provideLowCardinalityAttr(TracingAttribute.BUCKET_NAME, span, bucket);
    }
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.VIEWS;
  }

  @Override
  public FullHttpRequest encode() {

    String path = CoreHttpPath.formatPath("/{}/_design/{}/_view/{}?"+query, bucket, development ? "dev_" + design : design, view);

    ByteBuf content = keysJson.isPresent()
      ? Unpooled.copiedBuffer(keysJson.get())
      : Unpooled.EMPTY_BUFFER;
    HttpMethod method = keysJson.isPresent()
      ? HttpMethod.POST
      : HttpMethod.GET;

    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method,
      path, content);

    request.headers()
      .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
      .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
      .set(HttpHeaderNames.USER_AGENT, context().environment().userAgent().formattedLong());

    authenticator.authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public ViewResponse decode(final ResponseStatus status, final ViewChunkHeader header,
                             final Flux<ViewChunkRow> rows, final Mono<ViewChunkTrailer> trailer) {
    return new ViewResponse(status, header, rows, trailer);
  }


  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = new TreeMap<>();
    ctx.put("type", serviceType().ident());
    ctx.put("bucket", redactMeta(bucket));
    ctx.put("designDoc", redactMeta(design));
    ctx.put("viewName", redactMeta(view));
    ctx.put("development", development);
    return ctx;
  }

  @Override
  public String name() {
    return "views";
  }
}
