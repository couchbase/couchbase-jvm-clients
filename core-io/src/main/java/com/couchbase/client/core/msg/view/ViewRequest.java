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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.HttpProtocol.addHttpBasicAuth;

public class ViewRequest extends BaseRequest<ViewResponse>
  implements HttpRequest<ViewChunkHeader, ViewChunkRow, ViewChunkTrailer, ViewResponse> {

  private final Credentials credentials;
  private final String bucket;
  private final boolean development;
  private final String design;
  private final String view;
  private final String query;
  private final Optional<byte[]> keysJson;

  public ViewRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                     final Credentials credentials, final String bucket, final String design,
                     final String view, final String query, Optional<byte[]> keysJson,
                     final boolean development) {
    super(timeout, ctx, retryStrategy);
    this.credentials = credentials;
    this.bucket = bucket;
    this.design = design;
    this.view = view;
    this.development = development;
    this.query = query;
    this.keysJson = keysJson;
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.VIEWS;
  }

  @Override
  public FullHttpRequest encode() {
    StringBuilder path = new StringBuilder();
    path.append("/").append(bucket).append("/_design/");
    path.append(development ? "dev_" + design : design);
    path.append("/_view/");
    path.append(view);
    path.append("?").append(query);

    ByteBuf content = keysJson.isPresent()
      ? Unpooled.copiedBuffer(keysJson.get())
      : Unpooled.EMPTY_BUFFER;
    HttpMethod method = keysJson.isPresent()
      ? HttpMethod.POST
      : HttpMethod.GET;

    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method,
      path.toString(), content);

    request.headers()
      .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
      .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
      .set(HttpHeaderNames.USER_AGENT, context().environment().userAgent().formattedLong());

    addHttpBasicAuth(
      request,
      credentials.usernameForBucket(bucket),
      credentials.passwordForBucket(bucket)
    );
    return request;
  }

  @Override
  public ViewResponse decode(final ResponseStatus status, final ViewChunkHeader header,
                             final Flux<ViewChunkRow> rows, final Mono<ViewChunkTrailer> trailer) {
    return new ViewResponse(status, header, rows, trailer);
  }

}
