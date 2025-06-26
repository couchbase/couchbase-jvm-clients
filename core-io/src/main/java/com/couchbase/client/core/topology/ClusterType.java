/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.topology;

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Sourced from the "prodName" (product name) field of the cluster topology JSON.
 */
@Stability.Internal
public class ClusterType {
  private static final String COUCHBASE_SERVER_NAME = "Couchbase Server";

  public static ClusterType COUCHBASE_SERVER = new ClusterType(COUCHBASE_SERVER_NAME);

  private final String name;
  private final boolean couchbaseServer;

  private ClusterType(String name) {
    this.name = requireNonNull(name);
    this.couchbaseServer = name.startsWith(COUCHBASE_SERVER_NAME);
  }

  public boolean isCouchbaseServer() {
    return couchbaseServer;
  }

  public static ClusterType from(@Nullable ClusterIdentifier id) {
    return id == null ? of(null) : id.clusterType();
  }

  public static ClusterType of(@Nullable String name) {
    return name == null || name.equals(COUCHBASE_SERVER_NAME)
      ? COUCHBASE_SERVER
      : new ClusterType(name);
  }

  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }
}
