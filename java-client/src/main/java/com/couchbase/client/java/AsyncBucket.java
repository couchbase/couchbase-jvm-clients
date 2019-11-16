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
import com.couchbase.client.core.diag.HealthPinger;
import com.couchbase.client.core.diag.PingResult;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.error.ReducedKeyValueErrorContext;
import com.couchbase.client.core.error.ReducedViewErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.diagnostics.PingResponse;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.AsyncCollectionManager;
import com.couchbase.client.java.manager.view.AsyncViewIndexManager;
import com.couchbase.client.java.view.ViewAccessor;
import com.couchbase.client.java.view.ViewOptions;
import com.couchbase.client.java.view.ViewResult;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

import static com.couchbase.client.java.ReactiveBucket.DEFAULT_PING_OPTIONS;
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

  private final AsyncCollectionManager collectionManager;

  private final AsyncViewIndexManager viewManager;

  private final Authenticator authenticator;

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
    this.collectionManager = new AsyncCollectionManager(core, name);
    this.viewManager = new AsyncViewIndexManager(core, name);
    this.authenticator = core.context().authenticator();
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

  @Stability.Volatile
  public AsyncCollectionManager collections() {
    return collectionManager;
  }

  @Stability.Volatile
  public AsyncViewIndexManager viewIndexes() {
    return viewManager;
  }

  /**
   * Opens the {@link AsyncScope} with the given name.
   *
   * @param name the name of the scope.
   * @return the {@link AsyncScope} once opened.
   */
  @Stability.Volatile
  public AsyncScope scope(final String name) {
    return new AsyncScope(name, this.name, core, environment);
  }

  /**
   * Opens the default {@link AsyncScope}.
   *
   * @return the {@link AsyncScope} once opened.
   */
  @Stability.Volatile
  public AsyncScope defaultScope() {
    return new AsyncScope(CollectionIdentifier.DEFAULT_SCOPE, name, core, environment);
  }

  /**
   * Opens the default collection for this {@link AsyncBucket}.
   *
   * @return the {@link AsyncCollection} once opened.
   */
  public AsyncCollection defaultCollection() {
    return defaultScope().defaultCollection();
  }

  /**
   * Opens the collection with the given name for this {@link AsyncBucket}.
   *
   * @return the {@link AsyncCollection} once opened.
   */
  @Stability.Volatile
  public AsyncCollection collection(final String collection) {
    return defaultScope().collection(collection);
  }

  public CompletableFuture<ViewResult> viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, DEFAULT_VIEW_OPTIONS);
  }

  public CompletableFuture<ViewResult> viewQuery(final String designDoc, final String viewName, final ViewOptions options) {
    notNull(options, "ViewOptions", () -> new ReducedViewErrorContext(designDoc, viewName, name));
    ViewOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return ViewAccessor.viewQueryAsync(core, viewRequest(designDoc, viewName, opts), serializer);
  }

  ViewRequest viewRequest(final String designDoc, final String viewName, final ViewOptions.Built opts) {
    notNullOrEmpty(designDoc, "DesignDoc", () -> new ReducedViewErrorContext(designDoc, viewName, name));
    notNullOrEmpty(viewName, "ViewName", () -> new ReducedViewErrorContext(designDoc, viewName, name));

    String query = opts.query();
    Optional<byte[]> keysJson = Optional.ofNullable(opts.keys()).map(s -> s.getBytes(StandardCharsets.UTF_8));
    boolean development = opts.development();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().analyticsTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    ViewRequest request = new ViewRequest(timeout, core.context(), retryStrategy, authenticator, name, designDoc,
      viewName, query, keysJson, development);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Performs a diagnostic active "ping" call with custom options, on all services.
   *
   * Note that since each service has different timeouts, you need to provide a timeout that suits
   * your needs (how long each individual service ping should take max before it times out).
   *
   * @param options options controlling the final ping result
   * @return a ping report once created.
   */
  @Stability.Volatile
  public CompletableFuture<PingResult> ping(final PingOptions options) {
    PingOptions.Built built = options.build();
    return HealthPinger.ping(environment,
            name,
            core,
            built.reportId().orElse(UUID.randomUUID().toString()),
            // Don't want to add another default timeout just for ping, KV seems reasonable
            built.timeout().orElse(environment.timeoutConfig().kvTimeout()),
            // Fast fail makes more sense for ping than using cluster strategy, which is likely default BestEffort
            built.retryStrategy().orElse(FailFastRetryStrategy.INSTANCE),
            built.services()).toFuture();
  }

  /**
   * Performs a diagnostic active "ping" call on all services.
   *
   * Note that since each service has different timeouts, you need to provide a timeout that suits
   * your needs (how long each individual service ping should take max before it times out).
   *
   * @return a ping report once created.
   */
  @Stability.Volatile
  public CompletableFuture<PingResult> ping() {
    return ping(DEFAULT_PING_OPTIONS);
  }
}
