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
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.SpatialViewOptions;
import com.couchbase.client.java.view.ViewOptions;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;

public class AsyncBucket {

  static final String DEFAULT_SCOPE = "_default";
  static final String DEFAULT_COLLECTION = "_default";
  static final long DEFAULT_COLLECTION_ID = 0;

  private final String name;
  private final ClusterEnvironment environment;
  private final Core core;

  AsyncBucket(String name, Core core, ClusterEnvironment environment) {
    this.core = core;
    this.environment = environment;
    this.name = name;
  }

  public String name() {
    return name;
  }

  public CompletableFuture<AsyncScope> scope(final String scope) {
    CompletableFuture<AsyncScope> s = new CompletableFuture<>();
    s.complete(new AsyncScope(scope, name, core, environment));
    return s;
  }

  public CompletableFuture<AsyncCollection> defaultCollection() {
    return new AsyncScope(DEFAULT_SCOPE, name, core, environment).defaultCollection();
  }

  public CompletableFuture<AsyncCollection> collection(final String collection) {
    return new AsyncScope(DEFAULT_SCOPE, name, core, environment).collection(collection);
  }

  /*
  public CompletableFuture<AsyncViewResult> viewQuery(final String designDoc, final String viewName) {
    return viewQuery(designDoc, viewName, ViewOptions.DEFAULT);
  }

  public CompletableFuture<AsyncViewResult> viewQuery(final String designDoc, final String viewName,
                                                      final ViewOptions options) {
    Validators.notNullOrEmpty(designDoc, "Design Doc");
    Validators.notNullOrEmpty(viewName, "View Name");
    notNull(options, "ViewOptions");

    return null;
  }

  public CompletableFuture<AsyncViewResult> spatialViewQuery(final String designDoc, final String viewName) {
    return spatialViewQuery(designDoc, viewName, SpatialViewOptions.DEFAULT);
  }

  public CompletableFuture<AsyncViewResult> spatialViewQuery(final String designDoc, final String viewName,
                                                      final SpatialViewOptions options) {
    Validators.notNullOrEmpty(designDoc, "DesignDoc");
    Validators.notNullOrEmpty(viewName, "ViewName");
    notNull(options, "SpatialViewOptions");

    return null;
  }*/

  ClusterEnvironment environment() {
    return environment;
  }

  Core core() {
    return core;
  }
}
