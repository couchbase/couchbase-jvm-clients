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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The scope identifies a group of collections and allows high application
 * density as a result.
 *
 * <p>If no scope is explicitly provided, the default scope is used.</p>
 *
 * @since 3.0.0
 */
public class AsyncScope {

  /**
   * Holds the inner core reference to pass on.
   */
  private final Core core;

  /**
   * The name of the bucket at which this scope belongs.
   */
  private final String bucketName;

  /**
   * The actual name of this scope.
   */
  private final String scopeName;

  /**
   * The attached environment to pass on and use.
   */
  private final ClusterEnvironment environment;

  /**
   * Stores already opened collections for reuse.
   */
  private final Map<String, AsyncCollection> collectionCache = new ConcurrentHashMap<>();

  /**
   * Creates a new {@link AsyncScope}.
   *
   * @param scopeName the name of the scope.
   * @param bucketName the name of the bucket.
   * @param core the attached core.
   * @param environment the attached environment.
   */
  AsyncScope(final String scopeName, final String bucketName, final Core core,
             final ClusterEnvironment environment) {
    this.scopeName = scopeName;
    this.bucketName = bucketName;
    this.core = core;
    this.environment = environment;
  }

  /**
   * The name of the scope.
   */
  public String name() {
    return scopeName;
  }

  /**
   * The name of the bucket this scope is attached to.
   */
  public String bucketName() {
    return bucketName;
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
   * Provides access to the configured {@link ClusterEnvironment} for this scope.
   */
  public ClusterEnvironment environment() {
    return environment;
  }

  /**
   * Opens the default collection for this scope.
   *
   * <p>Note that this method is package private because it is called from the bucket class only!</p>
   *
   * @return the default collection once opened.
   */
  AsyncCollection defaultCollection() {
    return maybeCreateAsyncCollection(
      CollectionIdentifier.DEFAULT_COLLECTION,
      !scopeName.equals(CollectionIdentifier.DEFAULT_SCOPE)
    );
  }

  /**
   * Opens a collection for this scope with an explicit name.
   *
   * @param collectionName the collection name.
   * @return the requested collection if successful.
   */
  @Stability.Volatile
  public AsyncCollection collection(final String collectionName) {
    return maybeCreateAsyncCollection(collectionName, true);
  }

  /**
   * Helper method to maybe create a new collection or load it from the cache.
   *
   * @param collectionName the name of the collection.
   * @param refreshMap if the collection map should be refreshed on the config provider.
   * @return a collection, either from the cache or a freshly populated one.
   */
  private AsyncCollection maybeCreateAsyncCollection(final String collectionName, final boolean refreshMap) {
    return collectionCache.computeIfAbsent(collectionName, name -> {
      if (refreshMap) {
        core.configurationProvider().refreshCollectionMap(bucketName, false);
      }
      return new AsyncCollection(name, scopeName, bucketName, core, environment);
    });
  }

}
