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
import com.couchbase.client.core.deps.okhttp3.HttpUrl;
import com.couchbase.client.core.deps.okhttp3.RequestBody;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;

/**
 * The query service on one node, using OkHttp.
 * <p>
 * Handles {@link QueryRequest}s here; everything else (admission, dispatch, failures,
 * {@link CoreHttpRequest}s, diagnostics) is in {@link AbstractOkHttpService}.
 */
@NullMarked
public class OkHttpQueryService extends AbstractOkHttpService {

  private final HttpUrl queryServiceUrl;

  public OkHttpQueryService(
    QueryServiceConfig config,
    CoreContext context,
    HostAndPort address
  ) {
    super(
      ServiceType.QUERY,
      context,
      address,
      config.maxEndpoints(),
      context.environment().ioConfig().queryCircuitBreakerConfig()
    );

    this.queryServiceUrl = baseUrl().newBuilder()
      .addPathSegment("query")
      .addPathSegment("service")
      .build();
  }

  @Override
  protected boolean supports(Request<?> request) {
    return request instanceof QueryRequest;
  }

  @Override
  protected void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt) {
    QueryRequest queryRequest = (QueryRequest) request;

    com.couchbase.client.core.deps.okhttp3.Request.Builder requestBuilder = new com.couchbase.client.core.deps.okhttp3.Request.Builder()
      .url(queryServiceUrl)
      .post(RequestBody.create(queryRequest.query(), APPLICATION_JSON));

    executeStreaming(queryRequest, requestBuilder, attempt, QueryResponseFormat.INSTANCE);
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    return errorToThrowable(responseBody);
  }

  /**
   * Returns the exception for an error response to a non-streaming HTTP request to the query service.
   */
  static Exception errorToThrowable(String content) {
    // todo: this needs to be cleaned up with the management apis later
    return new CouchbaseException("Unknown query error: " + content);
  }
}
