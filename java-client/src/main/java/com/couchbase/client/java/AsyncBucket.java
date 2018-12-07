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

  public CompletableFuture<AsyncCollection> defaultCollection() {
    return collection("_default");
  }

  public CompletableFuture<AsyncCollection> collection(final String collection) {
    return collection(collection, "_default");
  }

  public CompletableFuture<AsyncCollection> collection(final String collection, final String scope) {
    GetCollectionIdRequest request = new GetCollectionIdRequest(Duration.ofSeconds(1), core.context(), name, environment.retryStrategy(), scope, collection);
    core.send(request);
    return request
      .response()
      .thenApply(res -> new AsyncCollection(collection, res.collectionId(), scope, name, core, environment));
  }

  ClusterEnvironment environment() {
    return environment;
  }

  Core core() {
    return core;
  }
}
