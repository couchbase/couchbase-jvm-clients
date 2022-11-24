/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.DISABLE_QUERY_TESTS_FOR_CLUSTER;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.REQUIRE_MB_50132;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Disabling against 5.5.  See comment on QueryIndexManagerIntegrationTest for details.
@IgnoreWhen(
  missesCapabilities = Capabilities.QUERY,
  clusterVersionEquals = DISABLE_QUERY_TESTS_FOR_CLUSTER,
  clusterVersionIsBelow = REQUIRE_MB_50132,
  clusterTypes = ClusterType.CAVES
)
class QueryConcurrencyIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static String bucketName;

  private static final int numDocsInserted = 1000;

  /**
   * Holds sample content for simple assertions.
   */
  private static final JsonObject FOO_CONTENT = JsonObject
    .create()
    .put("foo", "bar");

  @BeforeAll
  static void setup() {
    cluster = createCluster(env -> env.ioConfig(IoConfig.enableMutationTokens(true)));
    Bucket bucket = cluster.bucket(config().bucketname());
    Collection collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.QUERY);
    waitForQueryIndexerToHaveKeyspace(cluster, config().bucketname());

    bucketName = "`" + config().bucketname() + "`";
    createPrimaryIndex(cluster, config().bucketname());

    for (int i = 0; i < numDocsInserted; i++) {
      collection.insert("doc-"+i, FOO_CONTENT);
    }

    Util.waitUntilCondition(() -> { QueryResult countResult = cluster.query(
        "select count(*) as count from " + bucketName,
        queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
        return countResult.rowsAsObject().get(0).getInt("count") >= numDocsInserted;},
        Duration.ofSeconds(10),
        Duration.ofSeconds(1));

    QueryResult countResult = cluster.query(
      "select count(*) as count from " + bucketName,
      queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS)
    );
    assertTrue(numDocsInserted <= countResult.rowsAsObject().get(0).getInt("count"));
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void shouldServiceConcurrentAccess() throws Exception {
    int numConcurrentThreads = 16;
    ExecutorService executorService = Executors.newFixedThreadPool(numConcurrentThreads);

    CountDownLatch servicesDone = new CountDownLatch(numConcurrentThreads);
    for (int i = 0; i < numConcurrentThreads; i++) {
      executorService.submit(() -> {
        try {
          for (int runs = 0; runs < 20; runs++) {
            QueryResult result = cluster.query("select * from " + bucketName + " limit " + numConcurrentThreads);
            assertEquals(numDocsInserted, result.rowsAsObject().size());
          }
        } finally {
          servicesDone.countDown();
        }
      });
    }

    servicesDone.await(30, TimeUnit.SECONDS);
    executorService.shutdownNow();
  }

}
