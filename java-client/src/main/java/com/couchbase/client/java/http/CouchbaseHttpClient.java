/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.java.http;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.Cluster;

import static com.couchbase.client.java.AsyncUtils.block;
import static java.util.Objects.requireNonNull;

/**
 * A specialized HTTP client for making requests to Couchbase Server.
 * <p>
 * Get an instance by calling {@link Cluster#httpClient()}.
 * <p>
 * Example usage:
 * <pre>
 * public static String getAutoFailoverSettings(Cluster cluster) {
 *   HttpResponse response = cluster.httpClient().get(
 *       HttpTarget.manager(),
 *       HttpPath.of("/settings/autoFailover"));
 *
 *   if (!response.success()) {
 *     throw new RuntimeException(
 *         "Failed to get auto-failover settings. HTTP status code " +
 *             response.statusCode() + "; " + response.contentAsString());
 *   }
 *
 *   return response.contentAsString();
 * }
 *
 * public static void disableAutoFailover(Cluster cluster) {
 *   HttpResponse response = cluster.httpClient().post(
 *       HttpTarget.manager(),
 *       HttpPath.of("/settings/autoFailover"),
 *       HttpPostOptions.httpPostOptions()
 *           .body(HttpBody.form(Map.of("enabled", "false"))));
 *
 *   if (!response.success()) {
 *     throw new RuntimeException(
 *         "Failed to disable auto-failover. HTTP status code " +
 *             response.statusCode() + "; " + response.contentAsString());
 *   }
 * }
 * </pre>
 *
 * @see AsyncCouchbaseHttpClient
 * @see ReactiveCouchbaseHttpClient
 */
public class CouchbaseHttpClient {
  private final AsyncCouchbaseHttpClient async;

  @Stability.Internal
  public CouchbaseHttpClient(AsyncCouchbaseHttpClient async) {
    this.async = requireNonNull(async);
  }

  /**
   * Issues a GET request with default options (no query parameters).
   * <p>
   * To specify query parameters, use the overload that takes {@link HttpGetOptions}
   * or include the query string in the path.
   */
  public HttpResponse get(HttpTarget target, HttpPath path) {
    return block(async.get(target, path));
  }

  /**
   * Issues a GET request with the given options.
   * <p>
   * Specify query parameters via the options:
   * <pre>
   * httpClient.get(target, path, HttpGetOptions.httpGetOptions()
   *     .queryString(Map.of("foo", "bar")));
   * </pre>
   */
  public HttpResponse get(HttpTarget target, HttpPath path, HttpGetOptions options) {
    return block(async.get(target, path, options));
  }

  /**
   * Issues a POST request with no body and default options.
   * <p>
   * To specify a request body, use the overload that takes {@link HttpPostOptions}.
   */
  public HttpResponse post(HttpTarget target, HttpPath path) {
    return block(async.post(target, path));
  }

  /**
   * Issues a POST request with the given options.
   * <p>
   * Specify a request body via the options:
   * <pre>
   * // form data
   * httpClient.post(target, path, HttpPostOptions.httpPostOptions()
   *     .body(HttpBody.form(Map.of("foo", "bar")));
   *
   * // JSON document
   * httpClient.post(target, path, HttpPostOptions.httpPostOptions()
   *     .body(HttpBody.json("{}")));
   * </pre>
   */
  public HttpResponse post(HttpTarget target, HttpPath path, HttpPostOptions options) {
    return block(async.post(target, path, options));
  }

  /**
   * Issues a PUT request with no body and default options.
   * <p>
   * To specify a request body, use the overload that takes {@link HttpPutOptions}.
   */
  public HttpResponse put(HttpTarget target, HttpPath path) {
    return block(async.put(target, path));
  }

  /**
   * Issues a PUT request with the given options.
   * <p>
   * Specify a request body via the options:
   * <pre>
   * // form data
   * httpClient.put(target, path, HttpPostOptions.httpPutOptions()
   *     .body(HttpBody.form(Map.of("foo", "bar")));
   *
   * // JSON document
   * httpClient.put(target, path, HttpPostOptions.httpPutOptions()
   *     .body(HttpBody.json("{}")));
   * </pre>
   */
  public HttpResponse put(HttpTarget target, HttpPath path, HttpPutOptions options) {
    return block(async.put(target, path, options));
  }

  /**
   * Issues a DELETE request with default options.
   */
  public HttpResponse delete(HttpTarget target, HttpPath path) {
    return block(async.delete(target, path));
  }

  /**
   * Issues a DELETE request with the given options.
   */
  public HttpResponse delete(HttpTarget target, HttpPath path, HttpDeleteOptions options) {
    return block(async.delete(target, path, options));
  }
}
