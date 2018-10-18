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

package com.couchbase.client.core.msg;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a {@link Request} flowing through the client.
 *
 * @since 2.0.0
 */
public interface Request<RES extends Response> {

  /**
   * Holds the response which eventually completes.
   *
   * @return the future containing the response, eventually.
   */
  CompletableFuture<RES> response();

  /**
   * Completes this request successfully.
   *
   * @param result the result to complete with.
   */
  void succeed(RES result);

  /**
   * Fails this request and completes it.
   *
   * @param error the error to fail this request with.
   */
  void fail(Throwable error);

  /**
   * If attached, returns the context for this request.
   *
   * @return the request context if attached.
   */
  RequestContext context();

  /**
   * Returns the timeout for this rquest.
   *
   * @return the timeout for this request.
   */
  Duration timeout();

}
