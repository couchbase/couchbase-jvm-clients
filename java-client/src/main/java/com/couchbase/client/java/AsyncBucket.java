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
import com.couchbase.client.core.msg.kv.GetCollectionIdRequest;
import com.couchbase.client.core.msg.kv.GetCollectionIdResponse;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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

  ClusterEnvironment environment() {
    return environment;
  }

  Core core() {
    return core;
  }
}
