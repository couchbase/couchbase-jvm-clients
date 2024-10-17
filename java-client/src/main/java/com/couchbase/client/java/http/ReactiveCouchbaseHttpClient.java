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
import com.couchbase.client.core.util.ReactorOps;
import com.couchbase.client.java.ReactiveCluster;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.Reactor.toMono;
import static java.util.Objects.requireNonNull;

/**
 * A specialized HTTP client for making requests to Couchbase Server.
 * <p>
 * Get an instance by calling {@link ReactiveCluster#httpClient()}.
 *
 * @see CouchbaseHttpClient
 * @see AsyncCouchbaseHttpClient
 */
public class ReactiveCouchbaseHttpClient {
  private final ReactorOps reactor;
  private final AsyncCouchbaseHttpClient async;

  @Stability.Internal
  public ReactiveCouchbaseHttpClient(ReactorOps reactor, AsyncCouchbaseHttpClient async) {
    this.reactor = requireNonNull(reactor);
    this.async = requireNonNull(async);
  }

  /**
   * Returns a Mono that, when subscribed, issues a GET request with default options (no query parameters).
   * <p>
   * To specify query parameters, use the overload that takes {@link HttpGetOptions}
   * or include the query string in the path.
   */
  public Mono<HttpResponse> get(HttpTarget target, HttpPath path) {
    return reactor.publishOnUserScheduler(() -> async.get(target, path));
  }

  /**
   * Returns a Mono that, when subscribed, issues a GET request with the given options.
   * <p>
   * Specify query parameters via the options:
   * <pre>
   * httpClient.get(target, path, HttpGetOptions.httpGetOptions()
   *     .queryString(Map.of("foo", "bar")));
   * </pre>
   */
  public Mono<HttpResponse> get(HttpTarget target, HttpPath path, HttpGetOptions options) {
    return reactor.publishOnUserScheduler(() -> async.get(target, path, options));
  }

  /**
   * Returns a Mono that, when subscribed, issues a POST request with no body and default options.
   * <p>
   * To specify a request body, use the overload that takes {@link HttpPostOptions}.
   */
  public Mono<HttpResponse> post(HttpTarget target, HttpPath path) {
    return reactor.publishOnUserScheduler(() -> async.post(target, path));
  }

  /**
   * Returns a Mono that, when subscribed, issues a POST request with the given options.
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
  public Mono<HttpResponse> post(HttpTarget target, HttpPath path, HttpPostOptions options) {
    return reactor.publishOnUserScheduler(() -> async.post(target, path, options));
  }

  /**
   * Returns a Mono that, when subscribed, issues a PUT request with no body and default options.
   * <p>
   * To specify a request body, use the overload that takes {@link HttpPutOptions}.
   */
  public Mono<HttpResponse> put(HttpTarget target, HttpPath path) {
    return reactor.publishOnUserScheduler(() -> async.put(target, path));
  }

  /**
   * Returns a Mono that, when subscribed, issues a PUT request with the given options.
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
  public Mono<HttpResponse> put(HttpTarget target, HttpPath path, HttpPutOptions options) {
    return reactor.publishOnUserScheduler(() -> async.put(target, path, options));
  }

  /**
   * Returns a Mono that, when subscribed, issues a DELETE request with default options.
   */
  public Mono<HttpResponse> delete(HttpTarget target, HttpPath path) {
    return reactor.publishOnUserScheduler(() -> async.delete(target, path));
  }

  /**
   * Returns a Mono that, when subscribed, issues a DELETE request with given options.
   */
  public Mono<HttpResponse> delete(HttpTarget target, HttpPath path, HttpDeleteOptions options) {
    return reactor.publishOnUserScheduler(() -> async.delete(target, path, options));
  }
}
