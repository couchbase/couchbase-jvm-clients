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

package com.couchbase.client.java.util;

import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Extends the {@link ClusterAwareIntegrationTest} with java-client specific code.
 *
 * @since 3.0.0
 */
@Timeout(value = 1, unit = TimeUnit.MINUTES) // Safety timer so tests can't block CI executors
public class JavaIntegrationTest extends ClusterAwareIntegrationTest {

  /**
   * Creates a {@link ClusterEnvironment.Builder} which already has the seed nodes and
   * credentials plugged and ready to use depending on the environment.
   *
   * @return the builder, ready to be further modified or used directly.
   */
  protected static ClusterEnvironment.Builder environment() {
    Set<SeedNode> seeds = config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.of(cfg.ports().get(Services.KV)),
      Optional.of(cfg.ports().get(Services.MANAGER))
    )).collect(Collectors.toSet());

    return ClusterEnvironment
      .builder(config().adminUsername(), config().adminPassword())
      .seedNodes(seeds);
  }

  /**
   * Helper method to create a primary index if it does not exist.
   */
  protected static void createPrimaryIndex(final Cluster cluster, final String bucketName) {
    try {
      QueryResult result = cluster.query("create primary index on " + bucketName);
      if (result.meta().status() != QueryStatus.SUCCESS) {
        throw new IllegalStateException("Could not create primary index for " +
          "query integration test!");
      }
    } catch (QueryException ex) {
      if (ex.msg().contains("Index #primary already exists")) {
        return;
      }
      throw ex;
    }
  }

}
