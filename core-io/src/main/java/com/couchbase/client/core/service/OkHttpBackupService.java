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
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.util.HostAndPort;
import org.jspecify.annotations.NullMarked;

/**
 * The backup service on one node, using OkHttp.
 * <p>
 * Like the Netty implementation, handles only {@link CoreHttpRequest}s, which
 * {@link AbstractOkHttpService} takes care of, and reports every error response
 * as an {@link HttpStatusCodeException}.
 */
@NullMarked
public class OkHttpBackupService extends AbstractOkHttpService {

  /**
   * Like the Netty implementation, which has at most this many endpoints (connections) per node.
   */
  private static final int MAX_IN_FLIGHT = 16;

  public OkHttpBackupService(CoreContext context, HostAndPort address) {
    super(
      ServiceType.BACKUP,
      context,
      address,
      MAX_IN_FLIGHT,
      context.environment().ioConfig().backupCircuitBreakerConfig()
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
    // Like the Netty implementation: no service-specific translation.
    return new HttpStatusCodeException(httpStatus, responseBody, request, null);
  }
}
