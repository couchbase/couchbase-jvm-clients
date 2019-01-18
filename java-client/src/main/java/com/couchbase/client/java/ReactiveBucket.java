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
import com.couchbase.client.core.util.Validators;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.view.ReactiveViewResult;
import com.couchbase.client.java.view.SpatialViewOptions;
import com.couchbase.client.java.view.ViewOptions;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.AsyncBucket.DEFAULT_SCOPE;

public class ReactiveBucket {

  private final AsyncBucket asyncBucket;
  private final Core core;
  private final ClusterEnvironment environment;

  ReactiveBucket(AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.core = asyncBucket.core();
    this.environment = asyncBucket.environment();
  }

  public AsyncBucket async() {
    return asyncBucket;
  }

  public Mono<ReactiveScope> scope(String name) {
    return Mono.fromFuture(asyncBucket.scope(name)).map(asyncScope -> new ReactiveScope(asyncScope, asyncBucket.name()));
  }

  public Mono<ReactiveCollection> defaultCollection() {
    return scope(DEFAULT_SCOPE).flatMap(ReactiveScope::defaultCollection);
  }

  public Mono<ReactiveCollection> collection(final String name) {
    return scope(DEFAULT_SCOPE).flatMap(reactiveScope -> reactiveScope.collection(name));
  }

  public Mono<ReactiveViewResult> viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, ViewOptions.DEFAULT);
  }

  public Mono<ReactiveViewResult> viewQuery(final String designDoc, final String viewName,
                                            final ViewOptions options) {
    notNullOrEmpty(designDoc, "Design Doc");
    notNullOrEmpty(viewName, "View Name");
    notNull(options, "ViewOptions");

    return null;
  }

  public Mono<ReactiveViewResult> spatialViewQuery(final String designDoc, final String viewName) {
    return spatialViewQuery(designDoc, viewName, SpatialViewOptions.DEFAULT);
  }

  public Mono<ReactiveViewResult> spatialViewQuery(final String designDoc, final String viewName,
                                            final SpatialViewOptions options) {
    notNullOrEmpty(designDoc, "Design Doc");
    notNullOrEmpty(viewName, "View Name");
    notNull(options, "SpatialViewOptions");

    return null;
  }

}
