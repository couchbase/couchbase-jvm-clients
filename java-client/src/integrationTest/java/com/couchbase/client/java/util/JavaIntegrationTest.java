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

import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.diagnostics.PingState;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;

/**
 * Extends the {@link ClusterAwareIntegrationTest} with java-client specific code.
 *
 * @since 3.0.0
 */
// Temporarily increased timeout to (possibly) workaround MB-37011 when Developer Preview enabled
@Timeout(value = 10, unit = TimeUnit.MINUTES) // Safety timer so tests can't block CI executors
public class JavaIntegrationTest extends ClusterAwareIntegrationTest {

  /**
   * Creates a {@link ClusterEnvironment.Builder} which already has the seed nodes and
   * credentials plugged and ready to use depending on the environment.
   *
   * @return the builder, ready to be further modified or used directly.
   */
  protected static ClusterEnvironment.Builder environment() {
    return ClusterEnvironment.builder();
  }

  /**
   * Creates the right connection string out of the seed nodes in the config.
   *
   * @return the connection string to connect.
   */
  protected static String connectionString() {
    return seedNodes().stream().map(s -> {
      if (s.kvPort().isPresent()) {
        return s.address() + ":" + s.kvPort().get();
      } else {
        return s.address();
      }
    }).collect(Collectors.joining(","));
  }

  /**
   * Returns the pre-set cluster options with the environment and authenticator configured.
   *
   * @return the cluster options ready to be used.
   */
  protected static ClusterOptions clusterOptions() {
    return ClusterOptions
      .clusterOptions(authenticator())
      .environment(environment().build());
  }

  protected static Authenticator authenticator() {
    return PasswordAuthenticator.create(config().adminUsername(), config().adminPassword());
  }

  protected static Set<SeedNode> seedNodes() {
    return config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.ofNullable(cfg.ports().get(Services.KV)),
      Optional.ofNullable(cfg.ports().get(Services.MANAGER))
    )).collect(Collectors.toSet());
  }

  /**
   * Helper method to create a primary index if it does not exist.
   */
  protected static void createPrimaryIndex(final Cluster cluster, final String bucketName) {
    cluster.queryIndexes()
        .createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions()
            .ignoreIfExists(true));
  }

  protected static void waitForQueryIndexerToHaveBucket(final Cluster cluster, final String bucketName) {
    boolean ready = false;
    int guard = 100;

    while (!ready && guard != 0) {
      guard -= 1;
      String statement =
              "SELECT COUNT(*) > 0 as present FROM system:keyspaces where name = '" + bucketName + "';";

      QueryResult queryResult = cluster.query(statement);
      List<JsonObject> rows = queryResult.rowsAsObject();
      if (rows.size() == 1 && rows.get(0).getBoolean("present")) {
        ready = true;
      }

      if (!ready) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
        }
      }
    }

    if (guard == 0) {
      throw new IllegalStateException("Query indexer is still not aware of bucket " + bucketName);
    }
  }

  /**
   * Improve test stability by waiting for a given service to report itself ready.
   */
  protected static void waitForService(final Bucket bucket, final ServiceType serviceType) {
    bucket.waitUntilReady(Duration.ofSeconds(30));

    Util.waitUntilCondition(() -> {
      PingResult pingResult = bucket.ping(PingOptions.pingOptions().serviceTypes(Collections.singleton(serviceType)));

      return pingResult.endpoints().containsKey(serviceType)
              && pingResult.endpoints().get(serviceType).size() > 0
              && pingResult.endpoints().get(serviceType).get(0).state() == PingState.OK;
    });
  }
}
