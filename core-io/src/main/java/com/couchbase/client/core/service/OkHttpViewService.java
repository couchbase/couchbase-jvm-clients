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
import com.couchbase.client.core.deps.okhttp3.RequestBody;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;

/**
 * The view service on one node, using OkHttp.
 * <p>
 * Handles {@link ViewRequest}s here; everything else (admission, dispatch, failures,
 * {@link CoreHttpRequest}s, diagnostics) is in {@link AbstractOkHttpService}.
 */
@NullMarked
public class OkHttpViewService extends AbstractOkHttpService {

  public OkHttpViewService(
    ViewServiceConfig config,
    CoreContext context,
    HostAndPort address
  ) {
    super(
      ServiceType.VIEWS,
      context,
      address,
      config.maxEndpoints(),
      context.environment().ioConfig().viewCircuitBreakerConfig()
    );
  }

  @Override
  protected boolean supports(Request<?> request) {
    return request instanceof ViewRequest;
  }

  @Override
  protected void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt) {
    ViewRequest viewRequest = (ViewRequest) request;

    // View requests are idempotent, so OkHttp may retransmit them.
    // POST if there are keys, otherwise GET.
    RequestBody requestBody = viewRequest.keysJson()
      .map(keys -> RequestBody.create(keys, APPLICATION_JSON))
      .orElse(null);

    com.couchbase.client.core.deps.okhttp3.Request.Builder requestBuilder = new com.couchbase.client.core.deps.okhttp3.Request.Builder()
      .url(url(viewRequest.pathAndQuery()))
      .method(requestBody == null ? "GET" : "POST", requestBody);

    executeStreaming(viewRequest, requestBuilder, attempt, ViewResponseFormat.INSTANCE);
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    return errorToThrowable(httpStatus, responseBody, request);
  }

  /**
   * Returns the exception for an error response to a non-streaming HTTP request to the view service.
   */
  static Exception errorToThrowable(int status, String content, Request<?> request) {
    HttpStatusCodeException cause = new HttpStatusCodeException(status, content, request, null);

    // SDK-46 wants HTTP status code 400 to be an InvalidArgumentException instead of CouchbaseException,
    // but that might break user code that catches ViewServiceException.
    // Views are deprecated anyway, so let's leave this one alone.
//    if (status == CoreHttpStatus.BAD_REQUEST) {
//      return new InvalidArgumentException("View service reported an invalid argument: " + content, cause, null);
//    }
    return new ViewServiceException(content, cause);
  }
}
