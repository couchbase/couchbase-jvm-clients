/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import org.jspecify.annotations.Nullable;

/**
 * Parent interface for all requests going to the cluster manager (other than {@code CoreHttpRequest}s).
 * <p>
 * These are GET requests with no body. Independent of any HTTP library.
 */
public interface ManagerRequest<R extends Response> extends Request<R> {

  /**
   * Returns the HTTP request path, starting with a slash.
   */
  @Stability.Internal
  String path();

  /**
   * Decodes a manager response into its response entity.
   *
   * @param httpStatus the response's HTTP status code.
   * @param content the actual content of the response (null for a streaming response).
   * @return the decoded value.
   */
  @Stability.Internal
  R decode(int httpStatus, byte @Nullable [] content);
}
