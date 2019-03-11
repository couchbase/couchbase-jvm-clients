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

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Provides access to a Couchbase bucket in an async fashion.
 */
public class AsyncBucket {

  /**
   * Holds the name of the default scope, hardcoded.
   */
  static final String DEFAULT_SCOPE = "_default";

  /**
   * Holds the name of the default collection, hardcoded.
   */
  static final String DEFAULT_COLLECTION = "_default";

  /**
   * Holds the default collection ID, hardcoded.
   */
  static final long DEFAULT_COLLECTION_ID = 0;

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
  @Stability.Uncommitted
  public Core core() {
    return core;
  }

  /**
   * Opens the {@link AsyncScope} with the given name.
   *
   * @param name the name of the scope.
   * @return the {@link AsyncScope} once opened.
   */
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
    return new AsyncScope(DEFAULT_SCOPE, name, core, environment).defaultCollection();
  }

  /**
   * Opens the collection with the given name for this {@link AsyncBucket}.
   *
   * @return the {@link AsyncCollection} once opened.
   */
  public CompletableFuture<AsyncCollection> collection(final String collection) {
    notNullOrEmpty(collection, "Collection");
    return new AsyncScope(DEFAULT_SCOPE, name, core, environment).collection(collection);
  }

}
