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

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class QueryRequest
  extends BaseRequest<QueryResponse>
  implements HttpRequest<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer, QueryResponse> {

  private static final String URI = "/query";
  private final byte[] query;
  private final String statement;
  private final boolean idempotent;
  private final Authenticator authenticator;
  private final String contextId;

  public QueryRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                      final Authenticator authenticator, final String statement, final byte[] query, boolean idempotent,
                      final String contextId) {
    super(timeout, ctx, retryStrategy);
    this.query = query;
    this.statement = statement;
    this.authenticator = authenticator;
    this.idempotent = idempotent;
    this.contextId = contextId;
  }

  @Override
  public FullHttpRequest encode() {
    ByteBuf content = Unpooled.wrappedBuffer(query);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      URI, content);
    request.headers()
      .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    request.headers()
      .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    request.headers()
      .set(HttpHeaderNames.USER_AGENT, context().environment().userAgent().formattedLong());
    authenticator.authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public QueryResponse decode(final ResponseStatus status, final QueryChunkHeader header,
                              final Flux<QueryChunkRow> rows,
                              final Mono<QueryChunkTrailer> trailer) {
    return new QueryResponse(status, header, rows, trailer);
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.QUERY;
  }

  public String statement() {
    return statement;
  }

  public Authenticator credentials() {
    return authenticator;
  }

  @Override
  public String operationId() {
    return contextId;
  }

  @Override
  public boolean idempotent() {
    return idempotent;
  }
}
