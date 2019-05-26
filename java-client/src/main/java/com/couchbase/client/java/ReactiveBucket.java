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
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.view.ReactiveViewResult;
import com.couchbase.client.java.view.SpatialViewOptions;
import com.couchbase.client.java.view.ViewAccessor;
import com.couchbase.client.java.view.ViewOptions;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.view.SpatialViewOptions.spatialViewOptions;
import static com.couchbase.client.java.view.ViewOptions.viewOptions;

/**
 * Provides access to a Couchbase bucket in a reactive fashion.
 */
public class ReactiveBucket {

  static final ViewOptions DEFAULT_VIEW_OPTIONS = viewOptions();
  static final SpatialViewOptions DEFAULT_SPATIAL_VIEW_OPTIONS = spatialViewOptions();

  /**
   * Holds the underlying async bucket reference.
   */
  private final AsyncBucket asyncBucket;

  /**
   * Constructs a new {@link ReactiveBucket}.
   *
   * @param asyncBucket the underlying async bucket.
   */
  ReactiveBucket(final AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
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
  @Stability.Uncommitted
  public Core core() {
    return asyncBucket.core();
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
  public Mono<ReactiveScope> scope(final String name) {
    return Mono
      .fromFuture(asyncBucket.scope(name))
      .map(asyncScope -> new ReactiveScope(asyncScope, asyncBucket.name()));
  }

  /**
   * Opens the default collection for this {@link ReactiveBucket}.
   *
   * @return the {@link ReactiveCollection} once opened.
   */
  public Mono<ReactiveCollection> defaultCollection() {
    return scope(CollectionIdentifier.DEFAULT_SCOPE).flatMap(ReactiveScope::defaultCollection);
  }

  /**
   * Opens the collection with the given name for this {@link ReactiveBucket}.
   *
   * @return the {@link ReactiveCollection} once opened.
   */
  public Mono<ReactiveCollection> collection(final String name) {
    return scope(CollectionIdentifier.DEFAULT_SCOPE).flatMap(reactiveScope -> reactiveScope.collection(name));
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

  public Mono<ReactiveViewResult> spatialViewQuery(final String designDoc, final String viewName) {
    return spatialViewQuery(designDoc, viewName, DEFAULT_SPATIAL_VIEW_OPTIONS);
  }

  public Mono<ReactiveViewResult> spatialViewQuery(final String designDoc, final String viewName,
                                                   final SpatialViewOptions options) {
    return ViewAccessor.viewQueryReactive(
      asyncBucket.core(),
      asyncBucket.spatialViewRequest(designDoc, viewName, options)
    );
  }

}
