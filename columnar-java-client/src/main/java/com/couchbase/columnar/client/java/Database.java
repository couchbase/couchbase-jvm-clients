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

import com.couchbase.columnar.client.java.internal.ThreadSafe;

import static java.util.Objects.requireNonNull;

/**
 * Contains {@link Scope}s.
 */
@ThreadSafe
public final class Database {
  private final Cluster cluster;
  private final String name;

  Database(
    Cluster cluster,
    String name
  ) {
    this.cluster = requireNonNull(cluster);
    this.name = requireNonNull(name);
  }

  public String name() {
    return name;
  }

  /**
   * Returns the cluster this database belongs to.
   * <p>
   * <b>Note:</b> Your IDE might warn you that the returned
   * object is auto-closable. If so, please ignore or suppress this warning.
   * The code that calls this method is not responsible for closing
   * the returned cluster instance.
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the scope in this database with the given name.
   * <p>
   * If the scope does not exist, this method still returns a
   * non-null object, but operations using that object fail with
   * an exception indicating the scope does not exist.
   */
  public Scope scope(String name) {
    return new Scope(cluster, this, name);
  }

  @Override
  public String toString() {
    return "Database{" +
      "name='" + name + '\'' +
      '}';
  }
}
