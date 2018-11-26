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

import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import static com.couchbase.client.java.AsyncUtils.block;

public class Cluster {

  private final AsyncCluster asyncCluster;
  private final ReactiveCluster reactiveCluster;

  public static Cluster connect(final String connectionString, final String username,
                                final String password, final CouchbaseEnvironment environment) {
    return new Cluster(connectionString, username, password, environment);
  }

  private Cluster(final String connectionString, final String username,
                  final String password, final CouchbaseEnvironment environment) {
    this.asyncCluster = AsyncCluster.connect(connectionString, username, password, environment);
    this.reactiveCluster = new ReactiveCluster(asyncCluster);
  }

  public AsyncCluster async() {
    return asyncCluster;
  }

  public ReactiveCluster reactive() {
    return reactiveCluster;
  }

  public QueryResult query(final String statement) {
    return block(async().query(statement));
  }

  public QueryResult query(final String statement, final QueryOptions options) {
    return block(async().query(statement, options));
  }

  public Bucket bucket(String name) {
    return new Bucket(asyncCluster.bucket(name));
  }

}
