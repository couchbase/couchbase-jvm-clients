/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.client.java;


import com.couchbase.client.core.api.manager.CoreBucketAndScope;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class Scope implements Queryable {
  private final Cluster cluster;
  private final Database database;

  private final String name;

  Scope(
    Cluster cluster,
    Database database,
    String name
  ) {
    this.cluster = requireNonNull(cluster);
    this.database = requireNonNull(database);
    this.name = requireNonNull(name);
  }

  /**
   * Returns the database this scope belongs to.
   */
  public Database database() {
    return database;
  }

  public String name() {
    return name;
  }

  @Override
  public QueryResult executeQuery(String statement, Consumer<QueryOptions> options) {
    return cluster.queryExecutor.queryBuffered(
      statement,
      options,
      new CoreBucketAndScope(database.name(), name)
    );
  }

  @Override
  public QueryMetadata executeStreamingQuery(
    String statement,
    Consumer<Row> rowAction,
    Consumer<QueryOptions> options
  ) {
    return cluster.queryExecutor.queryStreaming(
      statement,
      options,
      new CoreBucketAndScope(database.name(), name),
      rowAction
    );
  }


  @Override
  public String toString() {
    return "Scope{" +
      "name='" + name + '\'' +
      ", databaseName='" + database.name() + '\'' +
      '}';
  }
}
