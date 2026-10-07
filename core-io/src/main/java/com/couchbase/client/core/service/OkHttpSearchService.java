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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.io.netty.search.NonChunkedSearchMessageHandler;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.HttpUrl;
import okhttp3.RequestBody;
import org.jspecify.annotations.NullMarked;

import java.time.Duration;

/**
 * The search service on one node, using OkHttp.
 * <p>
 * Handles {@link ServerSearchRequest}s here; everything else (admission, dispatch, failures,
 * {@link CoreHttpRequest}s, diagnostics) is in {@link AbstractOkHttpService}.
 */
@NullMarked
public class OkHttpSearchService extends AbstractOkHttpService {

  /**
   * @param streamingReadTimeout socket read timeout for the rest of a search response,
   * once the SDK has completed the response future. See {@link StreamingJsonResponseHandler}.
   */
  public OkHttpSearchService(
    SearchServiceConfig config,
    CoreContext context,
    HostAndPort address,
    Duration streamingReadTimeout
  ) {
    super(
      ServiceType.SEARCH,
      context,
      address,
      config.maxEndpoints(),
      context.environment().ioConfig().searchCircuitBreakerConfig(),
      streamingReadTimeout
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

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
      .url(searchUrl(searchRequest))
      .method("POST", requestBody);

    executeStreaming(searchRequest, requestBuilder, attempt, SearchResponseFormat.INSTANCE);
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    return NonChunkedSearchMessageHandler.errorToThrowable(HttpResponseStatus.valueOf(httpStatus), responseBody, request);
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
