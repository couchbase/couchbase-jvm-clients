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
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.ViewNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.view.ViewIndexManager;
import com.couchbase.client.java.view.ViewOptions;
import com.couchbase.client.java.view.ViewResult;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.java.AsyncUtils.block;
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

  private final CollectionManager collectionManager;

  private final ViewIndexManager viewManager;

  /**
   * Stores already opened scopes for reuse.
   */
  private final Map<String, Scope> scopeCache = new ConcurrentHashMap<>();

  /**
   * Constructs a new {@link Bucket}.
   *
   * @param asyncBucket the underlying async bucket.
   */
  Bucket(final AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.reactiveBucket = new ReactiveBucket(asyncBucket);
    this.collectionManager = new CollectionManager(asyncBucket.collections());
    this.viewManager = new ViewIndexManager(asyncBucket.viewIndexes());
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


  @Stability.Volatile
  public CollectionManager collections() {
    return collectionManager;
  }

  @Stability.Volatile
  public ViewIndexManager viewIndexes() {
    return viewManager;
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

  /**
   * Opens the {@link Scope} with the given name.
   *
   * @param name the name of the scope.
   * @return the {@link Scope} once opened.
   */
  @Stability.Volatile
  public Scope scope(final String name) {
    return scopeCache.computeIfAbsent(name, n -> new Scope(asyncBucket.scope(n)));
  }

  /**
   * Opens the default {@link Scope}.
   *
   * @return the {@link Scope} once opened.
   */
  @Stability.Volatile
  public Scope defaultScope() {
    return scopeCache.computeIfAbsent(
      CollectionIdentifier.DEFAULT_SCOPE,
      n -> new Scope(asyncBucket.defaultScope())
    );
  }

  /**
   * Opens the default collection for this {@link Bucket}.
   *
   * @return the {@link Collection} once opened.
   */
  public Collection defaultCollection() {
    return defaultScope().defaultCollection();
  }

  /**
   * Opens the collection with the given name for this {@link Bucket}.
   *
   * @return the {@link Collection} once opened.
   */
  @Stability.Volatile
  public Collection collection(final String collectionName) {
    return defaultScope().collection(collectionName);
  }

  /**
   * Queries a view on the bucket.
   *
   * @param designDoc the name of the design document in which the view resides.
   * @param viewName the name of the view to query.
   * @return a {@link ViewResult} once completed.
   * @throws ViewNotFoundException if the view or design document is not found on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public ViewResult viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, DEFAULT_VIEW_OPTIONS);
  }

  /**
   * Queries a view on the bucket with custom options.
   *
   * @param designDoc the name of the design document in which the view resides.
   * @param viewName the name of the view to query.
   * @param options allows to customize view options.
   * @return a {@link ViewResult} once completed.
   * @throws ViewNotFoundException if the view or design document is not found on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public ViewResult viewQuery(final String designDoc, final String viewName, final ViewOptions options) {
    return block(asyncBucket.viewQuery(designDoc, viewName, options));
  }

  /**
   * Performs application-level ping requests against services in the couchbase cluster.
   *
   * @return the {@link PingResult} once complete.
   */
  public PingResult ping() {
    return block(asyncBucket.ping());
  }

  /**
   * Performs application-level ping requests with custom options against services in the couchbase cluster.
   *
   * @return the {@link PingResult} once complete.
   */
  public PingResult ping(final PingOptions options) {
    return block(asyncBucket.ping(options));
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
   * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on.
   *
   * @param timeout the maximum time to wait until readiness.
   */
  public void waitUntilReady(final Duration timeout) {
    block(asyncBucket.waitUntilReady(timeout));
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online" by default, or the timeout is reached. Since the
   * SDK is bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on. You can tune the properties through {@link WaitUntilReadyOptions}.
   *
   * @param timeout the maximum time to wait until readiness.
   * @param options the options to customize the readiness waiting.
   */
  public void waitUntilReady(final Duration timeout, final WaitUntilReadyOptions options) {
    block(asyncBucket.waitUntilReady(timeout, options));
  }

}
