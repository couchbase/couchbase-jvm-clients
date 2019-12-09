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
import com.couchbase.client.core.diag.PingResult;
import com.couchbase.client.core.error.ReducedViewErrorContext;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.ReactiveCollectionManager;
import com.couchbase.client.java.view.ReactiveViewResult;
import com.couchbase.client.java.view.ViewAccessor;
import com.couchbase.client.java.view.ViewOptions;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.diagnostics.PingOptions.pingOptions;
import static com.couchbase.client.java.view.ViewOptions.viewOptions;

/**
 * Provides access to a Couchbase bucket in a reactive fashion.
 */
public class ReactiveBucket {

  static final ViewOptions DEFAULT_VIEW_OPTIONS = viewOptions();
  static final PingOptions DEFAULT_PING_OPTIONS = pingOptions();

  /**
   * Holds the underlying async bucket reference.
   */
  private final AsyncBucket asyncBucket;

  private final ReactiveCollectionManager collectionManager;

  /**
   * Constructs a new {@link ReactiveBucket}.
   *
   * @param asyncBucket the underlying async bucket.
   */
  ReactiveBucket(final AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.collectionManager = new ReactiveCollectionManager(asyncBucket.collections());
  }

  /**
   * Provides access to the underlying {@link AsyncBucket}.
   */
  public AsyncBucket async() {
    return asyncBucket;
  }

  /**
   * Returns the name of the {@link ReactiveBucket}.
   */
  public String name() {
    return asyncBucket.name();
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return asyncBucket.core();
  }

  @Stability.Volatile
  public ReactiveCollectionManager collections() {
    return collectionManager;
  }

  /**
   * Returns the attached {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return asyncBucket.environment();
  }

  /**
   * Opens the {@link ReactiveScope} with the given name.
   *
   * @param name the name of the scope.
   * @return the {@link ReactiveScope} once opened.
   */
  @Stability.Volatile
  public ReactiveScope scope(final String name) {
    return new ReactiveScope(asyncBucket.scope(name));
  }

  /**
   * Opens the default {@link ReactiveScope}.
   *
   * @return the {@link ReactiveScope} once opened.
   */
  @Stability.Volatile
  public ReactiveScope defaultScope() {
    return new ReactiveScope(asyncBucket.defaultScope());
  }

  /**
   * Opens the default collection for this {@link ReactiveBucket}.
   *
   * @return the {@link ReactiveCollection} once opened.
   */
  public ReactiveCollection defaultCollection() {
    return defaultScope().defaultCollection();
  }

  /**
   * Opens the collection with the given name for this {@link ReactiveBucket}.
   *
   * @return the {@link ReactiveCollection} once opened.
   */
  @Stability.Volatile
  public ReactiveCollection collection(final String collectionName) {
    return defaultScope().collection(collectionName);
  }

  public Mono<ReactiveViewResult> viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, DEFAULT_VIEW_OPTIONS);
  }

  public Mono<ReactiveViewResult> viewQuery(final String designDoc, final String viewName, final ViewOptions options) {
    return Mono.defer(() -> {
      notNull(options, "ViewOptions", () -> new ReducedViewErrorContext(designDoc, viewName, name()));
      ViewOptions.Built opts = options.build();
      JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
      return ViewAccessor.viewQueryReactive(asyncBucket.core(), asyncBucket.viewRequest(designDoc, viewName, opts), serializer);
    });
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
  public Mono<PingResult> ping(final PingOptions options) {
    return Mono.defer(() -> Mono.fromFuture(async().ping(options)));
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
  public Mono<PingResult> ping() {
    return ping(DEFAULT_PING_OPTIONS);
  }
}
