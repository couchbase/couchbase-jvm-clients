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
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.OwnedSupplier;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class AsyncCluster {

  private final Supplier<ClusterEnvironment> environment;
  private final Core core;


  public static AsyncCluster connect(final String username, final String password) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(username, password)));
  }

  public static AsyncCluster connect(final Credentials credentials) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(credentials)));
  }

  public static AsyncCluster connect(final String connectionString, final String username, final String password) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(connectionString, username, password)));
  }

  public static AsyncCluster connect(final String connectionString, final Credentials credentials) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(connectionString, credentials)));
  }

  public static AsyncCluster connect(final ClusterEnvironment environment) {
    return new AsyncCluster(() -> environment);
  }

  AsyncCluster(final Supplier<ClusterEnvironment> environment) {
    this.environment = environment;
    this.core = Core.create(environment.get());
  }

  public CompletableFuture<QueryResult> query(final String statement) {
    return query(statement, QueryOptions.DEFAULT);
  }

  public CompletableFuture<QueryResult> query(final String statement, final QueryOptions options) {
    return null;
  }

  public AsyncBucket bucket(String name) {
    return new AsyncBucket(name, core, environment.get());
  }

  public CompletableFuture<Void> shutdown() {
    // TODO: implement me!
    return null;
  }

}
