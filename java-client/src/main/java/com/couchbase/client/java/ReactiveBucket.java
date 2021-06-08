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
import com.couchbase.client.core.error.context.ReducedViewErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.collection.ReactiveCollectionManager;
import com.couchbase.client.java.manager.view.ReactiveViewIndexManager;
import com.couchbase.client.java.view.ReactiveViewResult;
import com.couchbase.client.java.view.ViewAccessor;
import com.couchbase.client.java.view.ViewOptions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_PING_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_WAIT_UNTIL_READY_OPTIONS;
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

  private final ReactiveViewIndexManager viewIndexManager;

  /**
   * Stores already opened scopes for reuse.
   */
  private final Map<String, ReactiveScope> scopeCache = new ConcurrentHashMap<>();

  /**
   * Constructs a new {@link ReactiveBucket}.
   *
   * @param asyncBucket the underlying async bucket.
   */
  ReactiveBucket(final AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.collectionManager = new ReactiveCollectionManager(asyncBucket.collections());
    this.viewIndexManager = new ReactiveViewIndexManager(asyncBucket.viewIndexes());
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

  public ReactiveCollectionManager collections() {
    return collectionManager;
  }

  public ReactiveViewIndexManager viewIndexes() {
    return viewIndexManager;
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
  public ReactiveScope scope(final String name) {
    return scopeCache.computeIfAbsent(name, n -> new ReactiveScope(asyncBucket.scope(n)));
  }

  /**
   * Opens the default {@link ReactiveScope}.
   *
   * @return the {@link ReactiveScope} once opened.
   */
  public ReactiveScope defaultScope() {
    return scopeCache.computeIfAbsent(
      CollectionIdentifier.DEFAULT_SCOPE,
      n -> new ReactiveScope(asyncBucket.defaultScope())
    );
  }

  /**
   * Opens the default collection for this {@link ReactiveBucket} using the default scope.
   * <p>
   * This method does not block and the client will try to establish all needed resources in the background. If you
   * need to eagerly await until all resources are established before performing an operation, use the
   * {@link #waitUntilReady(Duration)} method on the {@link ReactiveBucket}.
   *
   * @return the opened default {@link ReactiveCollection}.
   */
  public ReactiveCollection defaultCollection() {
    return defaultScope().defaultCollection();
  }

  /**
   * Provides access to the collection with the given name for this {@link ReactiveBucket} using the default scope.
   * <p>
   * This method does not block and the client will try to establish all needed resources in the background. If you
   * need to eagerly await until all resources are established before performing an operation, use the
   * {@link #waitUntilReady(Duration)} method on the {@link ReactiveBucket}.
   *
   * @return the opened named {@link ReactiveCollection}.
   */
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
   * Performs application-level ping requests against services in the couchbase cluster.
   *
   * @return the {@link PingResult} once complete.
   */
  public Mono<PingResult> ping() {
    return ping(DEFAULT_PING_OPTIONS);
  }

  /**
   * Performs application-level ping requests with custom options against services in the couchbase cluster.
   *
   * @return the {@link PingResult} once complete.
   */
  public Mono<PingResult> ping(final PingOptions options) {
    return Mono.defer(() -> Mono.fromFuture(asyncBucket.ping(options)));
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
   * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on.
   *
   * @param timeout the maximum time to wait until readiness.
   * @return a mono that completes either once ready or timeout.
   */
  public Mono<Void> waitUntilReady(final Duration timeout) {
    return waitUntilReady(timeout, DEFAULT_WAIT_UNTIL_READY_OPTIONS);
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
   * @return a mono that completes either once ready or timeout.
   */
  public Mono<Void> waitUntilReady(final Duration timeout, final WaitUntilReadyOptions options) {
    return Mono.defer(() -> Mono.fromFuture(asyncBucket.waitUntilReady(timeout, options)));
  }

}
