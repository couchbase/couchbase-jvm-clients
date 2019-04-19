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
import com.couchbase.client.java.view.SpatialViewOptions;
import com.couchbase.client.java.view.ViewOptions;
import com.couchbase.client.java.view.ViewResult;

import static com.couchbase.client.java.AsyncBucket.DEFAULT_SCOPE;
import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.ReactiveBucket.DEFAULT_SPATIAL_VIEW_OPTIONS;
import static com.couchbase.client.java.ReactiveBucket.DEFAULT_VIEW_OPTIONS;

/**
 * Provides access to a Couchbase bucket in a blocking fashion.
 */
public class Bucket {

  /**
   * Holds the underlying async bucket reference.
   */
  private final AsyncBucket asyncBucket;

  /**
   * Holds the adjacent reactive bucket reference.
   */
  private final ReactiveBucket reactiveBucket;

  /**
   * Constructs a new {@link Bucket}.
   *
   * @param asyncBucket the underlying async bucket.
   */
  Bucket(final AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.reactiveBucket = new ReactiveBucket(asyncBucket);
  }

  /**
   * Provides access to the underlying {@link AsyncBucket}.
   */
  public AsyncBucket async() {
    return asyncBucket;
  }

  /**
   * Provides access to the related {@link ReactiveBucket}.
   */
  public ReactiveBucket reactive() {
    return reactiveBucket;
  }

  /**
   * Returns the name of the {@link Bucket}.
   */
  public String name() {
    return asyncBucket.name();
  }

  /**
   * Returns the attached {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return asyncBucket.environment();
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
   * Opens the {@link Scope} with the given name.
   *
   * @param name the name of the scope.
   * @return the {@link Scope} once opened.
   */
  public Scope scope(final String name) {
    return block(asyncBucket.scope(name)
      .thenApply(Scope::new)
    );
  }

  /**
   * Opens the default collection for this {@link Bucket}.
   *
   * @return the {@link Collection} once opened.
   */
  public Collection defaultCollection() {
    return scope(DEFAULT_SCOPE).defaultCollection();
  }

  /**
   * Opens the collection with the given name for this {@link Bucket}.
   *
   * @return the {@link Collection} once opened.
   */
  public Collection collection(final String name) {
    return scope(DEFAULT_SCOPE).collection(name);
  }

  public ViewResult spatialViewQuery(final String designDoc, final String viewName) {
    return spatialViewQuery(designDoc, viewName, DEFAULT_SPATIAL_VIEW_OPTIONS);
  }

  public ViewResult spatialViewQuery(final String designDoc, final String viewName,
                                                        final SpatialViewOptions options) {
    return block(asyncBucket.spatialViewQuery(designDoc, viewName, options));
  }

  public ViewResult viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, DEFAULT_VIEW_OPTIONS);
  }

  public ViewResult viewQuery(final String designDoc, final String viewName,
                                                 final ViewOptions options) {
    return block(asyncBucket.viewQuery(designDoc, viewName, options));
  }

}
