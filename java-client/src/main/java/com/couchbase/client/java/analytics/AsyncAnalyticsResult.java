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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class AsyncAnalyticsResult {

  private final AnalyticsResponse response;

  @Stability.Internal
  AsyncAnalyticsResult(AnalyticsResponse response) {
    this.response = response;
  }

  /**
   * Returns the request identifier string of the query request
   */
  public String requestId() {
    return this.response.header().requestId();
  }

  /**
   * Returns the client context identifier string set on the query request, if it's available
   */
  public Optional<String> clientContextId() {
    return this.response.header().clientContextId();
  }

  /**
   * A {@link CompletableFuture} which completes with the query execution status as returned by the query engine
   *
   * The future can complete successfully or throw an {@link ExecutionException} wrapping
   * - {@link DecodingFailedException} when the decoding cannot be completed successfully
   *
   * @return {@link CompletableFuture}
   */
  public CompletableFuture<List<JsonObject>> rows() {
    return rows(JsonObject.class);
  }

  /**
   * A {@link CompletableFuture} which completes with the query execution status as returned by the query engine
   *
   * The future can complete successfully or throw an {@link ExecutionException} wrapping
   * - {@link DecodingFailedException} when the decoding cannot be completed successfully
   *
   * @return {@link CompletableFuture}
   */
  public <T> CompletableFuture<List<T>> rows(Class<T> target) {
    return this.response.rows().map(n -> {
      try {
        return JacksonTransformers.MAPPER.readValue(n.data(), target);
      } catch (IOException ex) {
        throw new DecodingFailedException(ex);
      }
    }).collectList().toFuture();
  }
}
