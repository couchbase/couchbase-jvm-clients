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
import java.util.Map;
import java.util.TreeMap;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

public class AnalyticsRequest
  extends BaseRequest<AnalyticsResponse>
  implements HttpRequest<AnalyticsChunkHeader, AnalyticsChunkRow, AnalyticsChunkTrailer, AnalyticsResponse> {

  public static final int NO_PRIORITY = 0;

  private static final String URI = "/analytics/service";
  private final byte[] query;
  private final int priority;
  private final boolean idempotent;
  private final String contextId;
  private final String statement;
  private final String bucket;
  private final String scope;

  private final Authenticator authenticator;

  public AnalyticsRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                          final Authenticator authenticator, final byte[] query, int priority, boolean idempotent,
                          final String contextId, final String statement, final RequestSpan span, final String bucket,
                          final String scope) {
    super(timeout, ctx, retryStrategy, span);
    this.query = query;
    this.authenticator = authenticator;
    this.priority = priority;
    this.idempotent = idempotent;
    this.contextId = contextId;
    this.statement = statement;
    this.bucket = bucket;
    this.scope = scope;

    if (span != null) {
      span.attribute(TracingIdentifiers.ATTR_SERVICE, TracingIdentifiers.SERVICE_ANALYTICS);
      span.attribute(TracingIdentifiers.ATTR_STATEMENT, statement);
      if (bucket != null) {
        span.attribute(TracingIdentifiers.ATTR_NAME, bucket);
      }
      if (scope != null) {
        span.attribute(TracingIdentifiers.ATTR_SCOPE, scope);
      }
    }
  }

  /**
   * Helper method to build the query context from bucket and scope.
   *
   * @param bucket the name of the bucket.
   * @param scope the name of the scope.
   * @return the build query context string.
   */
  public static String queryContext(final String bucket, final String scope) {
    return "default:`"  + bucket + "`.`" + scope +"`" ;
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
    if (priority != NO_PRIORITY) {
      request.headers().set("Analytics-Priority", priority);
    }
    authenticator.authHttpRequest(serviceType(), request);
    return request;
  }

  @Override
  public AnalyticsResponse decode(final ResponseStatus status, final AnalyticsChunkHeader header,
                                  final Flux<AnalyticsChunkRow> rows,
                                  final Mono<AnalyticsChunkTrailer> trailer) {
    return new AnalyticsResponse(status, header, rows, trailer);
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.ANALYTICS;
  }

  @Override
  public boolean idempotent() {
    return idempotent;
  }

  @Override
  public String operationId() {
    return contextId;
  }

  public String statement() {
    return statement;
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = new TreeMap<>();
    ctx.put("type", serviceType().ident());
    ctx.put("operationId", redactMeta(operationId()));
    ctx.put("statement", redactUser(statement()));
    ctx.put("priority", priority);
    if (bucket != null) {
      ctx.put("bucket", redactMeta(bucket));
    }
    if (scope != null) {
      ctx.put("scope", redactMeta(scope));
    }
    return ctx;
  }

  public String bucket() {
    return bucket;
  }

  public String scope() {
    return scope;
  }

  @Override
  public String name() {
    return "analytics";
  }

}
