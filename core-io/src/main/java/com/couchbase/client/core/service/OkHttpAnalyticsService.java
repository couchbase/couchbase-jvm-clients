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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.io.netty.analytics.AnalyticsChunkResponseParser;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.util.HostAndPort;
import okhttp3.RequestBody;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The analytics service on one node, using OkHttp.
 * <p>
 * Handles {@link AnalyticsRequest}s here; everything else (admission, dispatch, failures,
 * {@link CoreHttpRequest}s, diagnostics) is in {@link AbstractOkHttpService}.
 */
@NullMarked
public class OkHttpAnalyticsService extends AbstractOkHttpService {

  /**
   * @param streamingReadTimeout socket read timeout for the rest of an analytics response,
   * once the SDK has completed the response future. See {@link StreamingJsonResponseHandler}.
   */
  public OkHttpAnalyticsService(
    AnalyticsServiceConfig config,
    CoreContext context,
    HostAndPort address,
    Duration streamingReadTimeout
  ) {
    super(
      ServiceType.ANALYTICS,
      context,
      address,
      config.maxEndpoints(),
      context.environment().ioConfig().analyticsCircuitBreakerConfig(),
      streamingReadTimeout
    );
  }

  @Override
  protected boolean supports(Request<?> request) {
    return request instanceof AnalyticsRequest;
  }

  @Override
  protected void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt) {
    AnalyticsRequest analyticsRequest = (AnalyticsRequest) request;

    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
      .url(url(analyticsRequest.httpPath()))
      .method(analyticsRequest.httpMethodName(), requestBody(analyticsRequest));

    if (analyticsRequest.priority() != AnalyticsRequest.NO_PRIORITY) {
      requestBuilder.header("Analytics-Priority", String.valueOf(analyticsRequest.priority()));
    }

    executeStreaming(analyticsRequest, requestBuilder, attempt, AnalyticsResponseFormat.INSTANCE);
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    // Like the Netty implementation, parses the whole body as errors (JSON or, for older servers, plain text).
    return AnalyticsChunkResponseParser.errorsToThrowable(responseBody.getBytes(UTF_8), request.context(), HttpResponseStatus.valueOf(httpStatus));
  }

  private static @Nullable RequestBody requestBody(AnalyticsRequest request) {
    byte[] content = request.query();
    String method = request.httpMethodName();
    if ((content == null || content.length == 0) && (method.equals("GET") || method.equals("HEAD"))) {
      return null; // OkHttp doesn't allow a body for these methods
    }
    byte[] nonNullContent = content == null ? new byte[0] : content;
    return RequestBody.create(nonNullContent, APPLICATION_JSON);
  }
}
