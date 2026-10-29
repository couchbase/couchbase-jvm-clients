/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.deps.okhttp3.HttpUrl;
import com.couchbase.client.core.deps.okhttp3.RequestBody;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpStatus;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.QuotaLimitedException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.context.SearchErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;

/**
 * The search service on one node, using OkHttp.
 * <p>
 * Handles {@link ServerSearchRequest}s here; everything else (admission, dispatch, failures,
 * {@link CoreHttpRequest}s, diagnostics) is in {@link AbstractOkHttpService}.
 */
@NullMarked
public class OkHttpSearchService extends AbstractOkHttpService {

  public OkHttpSearchService(
    SearchServiceConfig config,
    CoreContext context,
    HostAndPort address
  ) {
    super(
      ServiceType.SEARCH,
      context,
      address,
      config.maxEndpoints(),
      context.environment().ioConfig().searchCircuitBreakerConfig()
    );
  }

  @Override
  protected boolean supports(Request<?> request) {
    return request instanceof ServerSearchRequest;
  }

  @Override
  protected void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt) {
    ServerSearchRequest searchRequest = (ServerSearchRequest) request;

    // Search requests are idempotent, so OkHttp may retransmit them.
    RequestBody requestBody = RequestBody.create(searchRequest.content(), APPLICATION_JSON);

    com.couchbase.client.core.deps.okhttp3.Request.Builder requestBuilder = new com.couchbase.client.core.deps.okhttp3.Request.Builder()
      .url(searchUrl(searchRequest))
      .method("POST", requestBody);

    executeStreaming(searchRequest, requestBuilder, attempt, SearchResponseFormat.INSTANCE);
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    return errorToThrowable(httpStatus, responseBody, request);
  }

  /**
   * Returns the exception for an error response to a non-streaming HTTP request to the search service.
   */
  static Exception errorToThrowable(int status, String content, Request<?> request) {
    SearchErrorContext errorContext = new SearchErrorContext(
      HttpProtocol.decodeStatus(status),
      request.context(),
      status,
      content
    );

    if (status == CoreHttpStatus.BAD_REQUEST && content.contains("index missing for update")) {
      return IndexNotFoundException.withMessageAndErrorContext("The index has not been found during an update on upsert. " +
        "If you did not intend to perform an index update, remove (or null out) the UUID property on the index.", errorContext);
    } else if (status == CoreHttpStatus.BAD_REQUEST && content.contains("index not found")) {
      return IndexNotFoundException.withMessageAndErrorContext("Index not found", errorContext);
    } else if (status == CoreHttpStatus.BAD_REQUEST && content.contains("index with the same name already exists")) {
      return new IndexExistsException("The index already exists. If you meant to replace/update it, make sure " +
        "that the UUID property is set.", errorContext);
    } else if (status == CoreHttpStatus.BAD_REQUEST && content.contains("num_fts_indexes")) {
      return new QuotaLimitedException(errorContext);
    } else if (status == CoreHttpStatus.TOO_MANY_REQUESTS) {
      if (content.contains("num_concurrent_requests")
        || content.contains("num_queries_per_min")
        || content.contains("ingress_mib_per_min")
        || content.contains("egress_mib_per_min")) {
        return new RateLimitedException(errorContext);
      }
    } else if (status == CoreHttpStatus.BAD_REQUEST) {
      return new InvalidArgumentException("Search service reported an invalid argument: " + content, null, errorContext);
    }

    return new CouchbaseException("Unknown search error: " + content, errorContext);
  }

  private HttpUrl searchUrl(ServerSearchRequest request) {
    HttpUrl.Builder url = baseUrl().newBuilder().addPathSegment("api");
    CoreBucketAndScope scope = request.scope();
    if (scope != null) {
      url.addPathSegment("bucket").addPathSegment(scope.bucketName())
        .addPathSegment("scope").addPathSegment(scope.scopeName());
    }
    return url
      .addPathSegment("index").addPathSegment(request.indexName())
      .addPathSegment("query")
      .build();
  }
}
