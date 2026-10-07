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
import com.couchbase.client.core.io.netty.eventing.NonChunkedEventingMessageHandler;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;

/**
 * The eventing service on one node, using OkHttp.
 * <p>
 * Like the Netty implementation, handles only {@link CoreHttpRequest}s (for example, eventing function
 * management), which {@link AbstractOkHttpService} takes care of. All this class adds is how
 * the service's error responses become exceptions.
 */
@NullMarked
public class OkHttpEventingService extends AbstractOkHttpService {

  /**
   * Like the Netty implementation, which has at most this many endpoints (connections) per node.
   */
  private static final int MAX_IN_FLIGHT = 16;

  public OkHttpEventingService(CoreContext context, HostAndPort address) {
    super(
      ServiceType.EVENTING,
      context,
      address,
      MAX_IN_FLIGHT,
      context.environment().ioConfig().eventingCircuitBreakerConfig()
    );
  }

  @Override
  protected boolean supports(Request<?> request) {
    return false; // only CoreHttpRequests, which every service supports
  }

  @Override
  protected void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt) {
    // Not called, since supports() is always false.
    throw new IllegalStateException("Unsupported request type: " + request);
  }

  @Override
  protected Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request) {
    return NonChunkedEventingMessageHandler.errorToThrowable(HttpResponseStatus.valueOf(httpStatus), responseBody, request);
  }
}
