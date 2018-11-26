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
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import java.util.concurrent.CompletableFuture;

public class AsyncCluster {

  private final String connectionString;
  private final String username;
  private final String password;
  private final CouchbaseEnvironment environment;
  private final Core core;

  public static AsyncCluster connect(final String connectionString, final String username,
                                final String password, final CouchbaseEnvironment environment) {
    return new AsyncCluster(connectionString, username, password, environment);
  }

  private AsyncCluster(final String connectionString, final String username,
               final String password, final CouchbaseEnvironment environment) {
    this.connectionString = connectionString;
    this.username = username;
    this.password = password;
    this.environment = environment;
    this.core = Core.create(environment, null);
  }

  public CompletableFuture<QueryResult> query(final String statement) {
    return query(statement, QueryOptions.DEFAULT);
  }

  public CompletableFuture<QueryResult> query(final String statement, final QueryOptions options) {
    return null;
  }

  public AsyncBucket bucket(String name) {
    return new AsyncBucket(name, core, environment);
  }

}
