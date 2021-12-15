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

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.java.search.SearchOptions.searchOptions;
import static com.couchbase.client.java.search.SearchQuery.queryString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the basic functionality of analytics queries in an end-to-end fashion.
 */
@IgnoreWhen(missesCapabilities = Capabilities.SEARCH, clusterTypes = ClusterType.CAVES)
class SearchIntegrationTest extends JavaIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SearchIntegrationTest.class);

  private static Cluster cluster;
  private static Collection collection;
  private static final String indexName = "idx-" + config().bucketname();

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(5));
    waitForService(bucket, ServiceType.SEARCH);
    cluster.searchIndexes().upsertIndex(new SearchIndex(indexName, config().bucketname()));
  }

  @AfterAll
  static void tearDown() {
    cluster.searchIndexes().dropIndex(indexName);
    cluster.disconnect();
  }

  @Test
  void simpleSearch() throws Throwable {
    String docId = UUID.randomUUID().toString();
    MutationResult insertResult = collection.insert(docId, mapOf("name", "michael"));

    try {
      // should not have to retry here, but newly-created index is flaky
      runWithRetry(Duration.ofSeconds(30), () -> {
        SearchResult result = cluster.searchQuery(indexName, queryString("michael"), searchOptions()
            .consistentWith(MutationState.from(insertResult.mutationToken().get())));

        List<String> actualDocIds = result.rows().stream()
            .map(SearchRow::id)
            .collect(toList());

        // make assertion inside the retry, because newly-created index sometimes returns
        // no rows even though we specified mutation tokens for consistency :-/
        assertEquals(listOf(docId), actualDocIds);
      });

    } finally {
      collection.remove(docId);
    }
  }

  private static void runWithRetry(Duration timeout, Runnable task) throws Throwable {
    long startNanos = System.nanoTime();
    Throwable deferred = null;
    do {
      if (deferred != null) {
        // Don't spam the server ...
        MILLISECONDS.sleep(2000);
      }
      try {
        task.run();
        return;
      } catch (Throwable t) {
        LOGGER.warn("Retrying due to {}", t.toString()); // don't need stack trace
        deferred = t;
      }
    } while (System.nanoTime() - startNanos < timeout.toNanos());
    throw deferred;
  }
}
