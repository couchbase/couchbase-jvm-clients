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

  private static final String DEFAULT_SCOPE = "_default";
  private static final String DEFAULT_COLLECTION = "_default";
  private static final long DEFAULT_COLLECTION_ID = 0;

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
    return collection(DEFAULT_COLLECTION);
  }

  public CompletableFuture<AsyncCollection> collection(final String collection) {
    return collection(collection, DEFAULT_SCOPE);
  }

  public CompletableFuture<AsyncCollection> collection(final String collection, final String scope) {
    if (DEFAULT_COLLECTION.equals(collection) && DEFAULT_SCOPE.equals(scope)) {
      CompletableFuture<AsyncCollection> future = new CompletableFuture<>();
      future.complete(new AsyncCollection(collection, DEFAULT_COLLECTION_ID, scope, name, core, environment));
      return future;
    } else {
      GetCollectionIdRequest request = new GetCollectionIdRequest(Duration.ofSeconds(1), core.context(), name, environment.retryStrategy(), scope, collection);
      core.send(request);
      return request
        .response()
        .thenApply(res -> {
          if (res.status().success()) {
            return new AsyncCollection(collection, res.collectionId().get(), scope, name, core, environment);
          } else {
            // TODO: delay into collection!
            throw new IllegalStateException("Do not raise me.. propagate into collection.. collection error");
          }
        });
    }
  }

  ClusterEnvironment environment() {
    return environment;
  }

  Core core() {
    return core;
  }
}
