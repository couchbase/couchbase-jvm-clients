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
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.AsyncCollectionManager;
import com.couchbase.client.java.manager.collection.ReactiveCollectionManager;
import com.couchbase.client.java.view.ReactiveViewResult;
import com.couchbase.client.java.view.ViewAccessor;
import com.couchbase.client.java.view.ViewOptions;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.view.ViewOptions.viewOptions;

/**
 * Provides access to a Couchbase bucket in a reactive fashion.
 */
public class ReactiveBucket {

  static final ViewOptions DEFAULT_VIEW_OPTIONS = viewOptions();

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
    this.collectionManager = new ReactiveCollectionManager(asyncBucket.collectionManager);
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
  public Mono<ReactiveScope> scope(final String name) {
    return Mono.fromFuture(asyncBucket.scope(name)).map(ReactiveScope::new);
  }

  /**
   * Opens the default {@link ReactiveScope}.
   *
   * @return the {@link ReactiveScope} once opened.
   */
  @Stability.Volatile
  public Mono<ReactiveScope> defaultScope() {
    return Mono.fromFuture(asyncBucket.defaultScope()).map(ReactiveScope::new);
  }

  /**
   * Opens the default collection for this {@link ReactiveBucket}.
   *
   * @return the {@link ReactiveCollection} once opened.
   */
  public Mono<ReactiveCollection> defaultCollection() {
    return defaultScope().flatMap(ReactiveScope::defaultCollection);
  }

  /**
   * Opens the collection with the given name for this {@link ReactiveBucket}.
   *
   * @return the {@link ReactiveCollection} once opened.
   */
  @Stability.Volatile
  public Mono<ReactiveCollection> collection(final String name) {
    return defaultScope().flatMap(reactiveScope -> reactiveScope.collection(name));
  }

  public Mono<ReactiveViewResult> viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, DEFAULT_VIEW_OPTIONS);
  }

  public Mono<ReactiveViewResult> viewQuery(final String designDoc, final String viewName,
                                            final ViewOptions options) {
    return ViewAccessor.viewQueryReactive(
      asyncBucket.core(),
      asyncBucket.viewRequest(designDoc, viewName, options)
    );
  }

}
