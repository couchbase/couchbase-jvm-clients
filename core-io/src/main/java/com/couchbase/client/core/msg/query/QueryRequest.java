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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

public class QueryRequest
  extends BaseRequest<QueryResponse>
  implements HttpRequest<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer, QueryResponse> {

  private static final String URI = "/query";
  private final byte[] query;
  private final String statement;
  private final boolean idempotent;
  private final Authenticator authenticator;
  private final String contextId;
  private final String bucket;
  private final String scope;

  public QueryRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                      final Authenticator authenticator, final String statement, final byte[] query, boolean idempotent,
                      final String contextId, final RequestSpan span, final String bucket, final String scope) {
    super(timeout, ctx, retryStrategy, span);
    this.query = query;
    this.statement = statement;
    this.authenticator = authenticator;
    this.idempotent = idempotent;
    this.contextId = contextId;
    this.bucket = bucket;
    this.scope = scope;

    if (span != null && !CbTracing.isInternalSpan(span)) {
      span.attribute(TracingIdentifiers.ATTR_SERVICE, TracingIdentifiers.SERVICE_QUERY);
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
    return "`default`:`" + bucket + "`.`" + scope + "`";
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

  public String bucket() {
    return bucket;
  }

  public String scope() {
    return scope;
  }

  @Override
  public Map<String, Object> serviceContext() {
    Map<String, Object> ctx = new TreeMap<>();
    ctx.put("type", serviceType().ident());
    ctx.put("operationId", redactMeta(operationId()));
    ctx.put("statement", redactUser(statement()));
    if (bucket != null) {
      ctx.put("bucket", redactMeta(bucket));
    }
    if (scope != null) {
      ctx.put("scope", redactMeta(scope));
    }
    return ctx;
  }

  /**
   * Returns a new request that creates a prepared statement using this request as a template.
   */
  @Stability.Internal
  public QueryRequest toPrepareRequest(boolean autoExecute, RequestTracer requestTracer) {
    String newStatement = "PREPARE " + statement();

    byte[] newQuery = transformQuery(query -> {
      query.put("statement", newStatement);

      if (autoExecute) {
        query.put("auto_execute", true);
      } else {
        // Keep only the fields required for preparation.
        // Discard things like arguments, scan vectors, etc.
        query.retain("statement", "timeout", "client_context_id", "query_context");
      }
    });

    RequestSpan newSpan = requestTracer.requestSpan("prepare", requestSpan());

    boolean newIdempotent = !autoExecute || idempotent();

    return copy(newStatement, newQuery, newIdempotent, newSpan);
  }

  /**
   * Returns a copy of this request tailored to execute a prepared statement.
   *
   * @param preparedStatementName name of the prepared statement
   * @param encodedPlan (nullable) query plan, or null if enhanced prepared statements are enabled.
   */
  @Stability.Internal
  public QueryRequest toExecuteRequest(String preparedStatementName, String encodedPlan, RequestTracer requestTracer) {
    byte[] newQuery = transformQuery(query -> {
      query.remove("statement");
      query.put("prepared", preparedStatementName);
      if (encodedPlan != null) {
        query.put("encoded_plan", encodedPlan);
      }
    });

    RequestSpan newSpan = requestTracer.requestSpan("execute", requestSpan());

    return copy(statement(), newQuery, idempotent(), newSpan);
  }

  private QueryRequest copy(String newStatement, byte[] newQuery, boolean newIdempotent, RequestSpan newSpan) {
    return new QueryRequest(
        timeout(),
        context(),
        retryStrategy(),
        credentials(),
        newStatement,
        newQuery,
        newIdempotent,
        operationId(),
        newSpan,
        bucket(),
        scope()
    );
  }

  private byte[] transformQuery(Consumer<ObjectNode> editor) {
    return editObject(query, editor);
  }

  private static byte[] editObject(byte[] jsonObject, Consumer<ObjectNode> editor) {
    ObjectNode node = (ObjectNode) Mapper.decodeIntoTree(jsonObject);
    editor.accept(node);
    return Mapper.encodeAsBytes(node);
  }

  @Override
  public String toString() {
    return "QueryRequest{" +
      "query=" + redactUser(new String(query, StandardCharsets.UTF_8)) +
      ", statement='" + redactUser(statement) + '\'' +
      ", idempotent=" + idempotent +
      ", contextId='" + contextId + '\'' +
      ", bucket='" + redactMeta(bucket) + '\'' +
      ", scope='" + redactMeta(scope) + '\'' +
      '}';
  }

  @Override
  public String name() {
    return "query";
  }
}
