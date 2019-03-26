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

import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.Optional;

import static com.couchbase.client.java.AsyncUtils.block;

public class AnalyticsResult {

  private final AsyncAnalyticsResult asyncResult;

  public AnalyticsResult(AsyncAnalyticsResult asyncResult) {
    this.asyncResult = asyncResult;
  }

  /**
   * Get the request identifier of the query request
   *
   * @return request identifier
   */
  public String requestId() {
    return this.asyncResult.requestId();
  }

  /**
   * Get the client context identifier as set by the client
   *
   * @return client context identifier
   */
  public Optional<String> clientContextId() {
    return this.asyncResult.clientContextId();
  }

  /**
   * Get the list of rows that were fetched by the query which are then
   * decoded to the requested entity class
   *
   * @param target target class for converting the query row
   * @param <T> generic class
   * @return list of entities
   */
  public <T> List<T> rows(Class<T> target) {
    return block(this.asyncResult.rows(target));
  }

  /**
   * Get the list of rows that were fetched by the query which are then
   * decoded to {@link JsonObject}
   *
   * @return list of {@link JsonObject}
   */
  public List<JsonObject> rows() {
    return block(this.asyncResult.rows());
  }

}
