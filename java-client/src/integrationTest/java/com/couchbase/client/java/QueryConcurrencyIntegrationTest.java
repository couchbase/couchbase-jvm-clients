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
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen( missesCapabilities = { Capabilities.QUERY })
class QueryConcurrencyIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static ClusterEnvironment environment;
  private static String bucketName;

  private static int numDocsInserted = 1000;

  /**
   * Holds sample content for simple assertions.
   */
  private static final JsonObject FOO_CONTENT = JsonObject
    .create()
    .put("foo", "bar");

  @BeforeAll
  static void setup() {
    environment = environment().ioConfig(IoConfig.enableMutationTokens(true)).build();
    cluster = Cluster.connect(seedNodes(), ClusterOptions.clusterOptions(authenticator()).environment(environment));
    Bucket bucket = cluster.bucket(config().bucketname());
    Collection collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(5));
    waitForService(bucket, ServiceType.QUERY);
    waitForQueryIndexerToHaveBucket(cluster, config().bucketname());

    bucketName = "`" + config().bucketname() + "`";
    createPrimaryIndex(cluster, config().bucketname());

    for (int i = 0; i < numDocsInserted; i++) {
      collection.insert("doc-"+i, FOO_CONTENT);
    }

    QueryResult countResult = cluster.query(
      "select count(*) as count from " + bucketName,
      queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS)
    );
    assertTrue(numDocsInserted <= countResult.rowsAsObject().get(0).getInt("count"));
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
    environment.shutdown();
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
