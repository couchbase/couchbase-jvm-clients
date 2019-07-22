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

package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.view.ViewAccessor;
import com.couchbase.client.java.view.ViewOptions;
import com.couchbase.client.java.view.ViewResult;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

import static com.couchbase.client.java.ReactiveBucket.DEFAULT_VIEW_OPTIONS;

/**
 * Provides access to a Couchbase bucket in an async fashion.
 */
public class AsyncBucket {

  /**
   * The name of the bucket.
   */
  private final String name;

  /**
   * The underlying attached environment.
   */
  private final ClusterEnvironment environment;

  /**
   * The core reference used.
   */
  private final Core core;

  /**
   * Creates a new {@link AsyncBucket}.
   *
   * @param name the name of the bucket.
   * @param core the underlying core.
   * @param environment the attached environment.
   */
  AsyncBucket(final String name, final Core core, final ClusterEnvironment environment) {
    this.core = core;
    this.environment = environment;
    this.name = name;
  }

  /**
   * Returns the name of the {@link AsyncBucket}.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the attached {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return environment;
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return core;
  }

  /**
   * Opens the {@link AsyncScope} with the given name.
   *
   * @param name the name of the scope.
   * @return the {@link AsyncScope} once opened.
   */
  @Stability.Volatile
  public CompletableFuture<AsyncScope> scope(final String name) {
    notNullOrEmpty(name, "Scope");
    return CompletableFuture.completedFuture(
      new AsyncScope(name, this.name, core, environment)
    );
  }

  /**
   * Opens the default collection for this {@link AsyncBucket}.
   *
   * @return the {@link AsyncCollection} once opened.
   */
  public CompletableFuture<AsyncCollection> defaultCollection() {
    return new AsyncScope(CollectionIdentifier.DEFAULT_SCOPE, name, core, environment).defaultCollection();
  }

  /**
   * Opens the collection with the given name for this {@link AsyncBucket}.
   *
   * @return the {@link AsyncCollection} once opened.
   */
  @Stability.Volatile
  public CompletableFuture<AsyncCollection> collection(final String collection) {
    notNullOrEmpty(collection, "Collection");
    return new AsyncScope(CollectionIdentifier.DEFAULT_SCOPE, name, core, environment).collection(collection);
  }

  public CompletableFuture<ViewResult> viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, DEFAULT_VIEW_OPTIONS);
  }

  public CompletableFuture<ViewResult> viewQuery(final String designDoc, final String viewName,
                                                 final ViewOptions options) {
    return ViewAccessor.viewQueryAsync(core, viewRequest(designDoc, viewName, options));
  }

  ViewRequest viewRequest(final String designDoc, final String viewName, final ViewOptions options) {
    notNullOrEmpty(designDoc, "DesignDoc");
    notNullOrEmpty(viewName, "ViewName");
    notNull(options, "ViewOptions");

    ViewOptions.Built opts = options.build();

    String query = opts.query();
    Optional<byte[]> keysJson = Optional.ofNullable(opts.keys()).map(s -> s.getBytes(StandardCharsets.UTF_8));
    boolean development = opts.development();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().analyticsTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    return new ViewRequest(timeout, core.context(), retryStrategy, environment.credentials(), name, designDoc,
      viewName, query, keysJson, development);
  }

}
