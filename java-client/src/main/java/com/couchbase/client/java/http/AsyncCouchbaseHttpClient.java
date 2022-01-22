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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CommonOptions;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.CbThrowables.propagate;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.http.HttpDeleteOptions.httpDeleteOptions;
import static com.couchbase.client.java.http.HttpPostOptions.httpPostOptions;
import static com.couchbase.client.java.http.HttpPutOptions.httpPutOptions;
import static java.util.Objects.requireNonNull;

/**
 * A specialized HTTP client for making requests to Couchbase Server.
 * <p>
 * Get an instance by calling {@link Cluster#httpClient()}.
 *
 * @see CouchbaseHttpClient
 * @see ReactiveCouchbaseHttpClient
 */
public class AsyncCouchbaseHttpClient {
  private final AsyncCluster cluster;
  private final Core core;

  @Stability.Internal
  public AsyncCouchbaseHttpClient(AsyncCluster cluster) {
    this.cluster = requireNonNull(cluster);
    this.core = cluster.core();
  }

  /**
   * Issues a GET request with default options (no query parameters).
   * <p>
   * To specify query parameters, use the overload that takes {@link HttpGetOptions}
   * or include the query string in the path.
   */
  public CompletableFuture<HttpResponse> get(HttpTarget target, HttpPath path) {
    return get(target, path, HttpGetOptions.httpGetOptions());
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
  public CompletableFuture<HttpResponse> get(HttpTarget target, HttpPath path, HttpGetOptions options) {
    notNull(target, "target");
    notNull(path, "path");
    notNull(options, "options");

    HttpGetOptions.Built builtOpts = options.build();
    return exec(HttpMethod.GET, target, path, builtOpts, req -> {
      if (builtOpts.queryString() != null) {
        req.queryString(builtOpts.queryString().urlEncoded);
      }
    });
  }

  /**
   * Issues a POST request with no body and default options.
   * <p>
   * To specify a request body, use the overload that takes {@link HttpPostOptions}.
   */
  public CompletableFuture<HttpResponse> post(HttpTarget target, HttpPath path) {
    return post(target, path, httpPostOptions());
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
  public CompletableFuture<HttpResponse> post(HttpTarget target, HttpPath path, HttpPostOptions options) {
    notNull(target, "target");
    notNull(path, "path");
    notNull(options, "options");

    HttpPostOptions.Built builtOpts = options.build();
    return exec(HttpMethod.POST, target, path, builtOpts, req -> {
      if (builtOpts.body() != null) {
        req.content(builtOpts.body().content, builtOpts.body().contentType);
      }
    });
  }

  /**
   * Issues a PUT request with no body and default options.
   * <p>
   * To specify a request body, use the overload that takes {@link HttpPutOptions}.
   */
  public CompletableFuture<HttpResponse> put(HttpTarget target, HttpPath path) {
    return put(target, path, httpPutOptions());
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
  public CompletableFuture<HttpResponse> put(HttpTarget target, HttpPath path, HttpPutOptions options) {
    notNull(target, "target");
    notNull(path, "path");
    notNull(options, "options");

    HttpPutOptions.Built builtOpts = options.build();
    return exec(HttpMethod.PUT, target, path, builtOpts, req -> {
      if (builtOpts.body() != null) {
        req.content(builtOpts.body().content, builtOpts.body().contentType);
      }
    });
  }

  /**
   * Issues a DELETE request with default options.
   */
  public CompletableFuture<HttpResponse> delete(HttpTarget target, HttpPath path) {
    return delete(target, path, httpDeleteOptions());
  }

  /**
   * Issues a DELETE request with given options.
   */
  public CompletableFuture<HttpResponse> delete(HttpTarget target, HttpPath path, HttpDeleteOptions options) {
    notNull(target, "target");
    notNull(path, "path");
    notNull(options, "options");

    return exec(HttpMethod.DELETE, target, path, options.build(), req -> {
    });
  }

  private CompletableFuture<HttpResponse> exec(
      HttpMethod method,
      HttpTarget target,
      HttpPath path,
      CommonHttpOptions<?>.BuiltCommonHttpOptions options,
      Consumer<CoreHttpRequest.Builder> customizer
  ) {
    CoreHttpRequest.Builder builder = new CoreHttpClient(core, target.coreTarget)
        .newRequest(method, CoreHttpPath.path(path.formatted), options)
        .bypassExceptionTranslation(true);

    options.headers().forEach(it -> builder.header(it.name, it.value));

    customizer.accept(builder);

    CoreHttpRequest req = builder.build();

    // If the target has a bucket, make sure the SDK has loaded the bucket config so the
    // request doesn't mysteriously time out while the service locator twiddles its thumbs.
    if (req.bucket() != null) {
      cluster.bucket(req.bucket());
    }

    return req.exec(core)
        .thenApply(HttpResponse::new)
        .exceptionally(t -> {
          HttpStatusCodeException statusCodeException =
              findCause(t, HttpStatusCodeException.class).orElseThrow(() -> propagate(t));
          return new HttpResponse(statusCodeException);
        });
  }
}
