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
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.search.queries.MatchOperator;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.result.SearchRowLocations;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.Flaky;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.java.search.SearchOptions.searchOptions;
import static com.couchbase.client.java.search.SearchQuery.match;
import static com.couchbase.client.java.search.SearchQuery.queryString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the basic functionality of search queries in an end-to-end fashion.
 */
@Disabled
@Flaky
@IgnoreWhen(missesCapabilities = Capabilities.SEARCH,
  clusterTypes = ClusterType.CAVES,
  clusterVersionIsBelow = ConsistencyUtil.CLUSTER_VERSION_MB_50101,
  isProtostellarWillWorkLater = true
)
class SearchIntegrationTest extends JavaIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SearchIntegrationTest.class);

  private static Cluster cluster;
  private static Collection collection;
  private static final String indexName = "idx-" + config().bucketname();

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.SEARCH);
    cluster.searchIndexes().upsertIndex(new SearchIndex(indexName, config().bucketname()));
    ConsistencyUtil.waitUntilSearchIndexPresent(cluster.core(), indexName);
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

  @Test
  void searchIncludeLocations() throws Throwable {
    String docId = UUID.randomUUID().toString();
    MutationResult insertResult = collection.insert(docId, mapOf("name", "billy"));

    try {
      runWithRetry(Duration.ofSeconds(30), () -> {
        SearchResult result = cluster.searchQuery(indexName, queryString("billy"), searchOptions()
          .consistentWith(MutationState.from(insertResult.mutationToken().get()))
          .includeLocations(true));

        List<SearchRowLocations> locationsList = result.rows().stream()
          .map(SearchRow::locations)
          .filter(opt -> opt.isPresent())
          .map(Optional::get)
          .collect(toList());

        assertTrue(!locationsList.isEmpty());
      });

    } finally {
      collection.remove(docId);
    }
  }

  @Test
  void searchMatchOperatorOr() throws Throwable {
    String docId1 = UUID.randomUUID().toString();
    String docId2 = UUID.randomUUID().toString();

    MutationResult insertResult1 = collection.insert(docId1, mapOf("name", "milly"));
    MutationResult insertResult2 = collection.insert(docId2, mapOf("name", "tilly"));

    try {
      runWithRetry(Duration.ofSeconds(30), () -> {
        SearchResult result = cluster.searchQuery(indexName, match("milly tilly").operator(MatchOperator.OR),
          searchOptions().consistentWith(MutationState.from(
            insertResult1.mutationToken().get(),
            insertResult2.mutationToken().get()))
        );

        List<String> actualDocIds = result.rows().stream()
          .map(SearchRow::id)
          .collect(toList());

        assertEquals(listOf(docId1, docId2), actualDocIds);
      });

    } finally {
      collection.remove(docId1);
      collection.remove(docId2);
    }
  }

  @Test
  void searchMatchOperatorAndMiss() throws Throwable {
    String docId1 = UUID.randomUUID().toString();
    String docId2 = UUID.randomUUID().toString();

    MutationResult insertResult1 = collection.insert(docId1, mapOf("name", "billy"));
    MutationResult insertResult2 = collection.insert(docId2, mapOf("name", "silly"));

    try {
      runWithRetry(Duration.ofSeconds(10), () -> {
        SearchResult result = cluster.searchQuery(indexName, match("silly billy").operator(MatchOperator.AND),
          searchOptions().consistentWith(MutationState.from(
            insertResult1.mutationToken().get(),
            insertResult2.mutationToken().get()))
        );

        List<String> actualDocIds = result.rows().stream()
          .map(SearchRow::id)
          .collect(toList());

        assertTrue(actualDocIds.isEmpty());
      });

    } finally {
      collection.remove(docId1);
      collection.remove(docId2);
    }
  }

  @Test
  void searchMatchOperatorAndHit() throws Throwable {
    String missId = UUID.randomUUID().toString();
    String hitId = UUID.randomUUID().toString();

    MutationResult insertResult1 = collection.insert(missId, mapOf("fields", mapOf("name", "billy")));
    MutationResult insertResult2 = collection.insert(hitId, mapOf("fields", mapOf("name", "billy",
      "surname","kid")));

    try {
      runWithRetry(Duration.ofSeconds(30), () -> {
        SearchResult result = cluster.searchQuery(indexName, match("billy kid").operator(MatchOperator.AND),
          searchOptions().consistentWith(MutationState.from(
            insertResult1.mutationToken().get(),
            insertResult2.mutationToken().get()))
        );

        List<String> actualDocIds = result.rows().stream()
          .map(SearchRow::id)
          .collect(toList());

        assertEquals(listOf(hitId), actualDocIds);
      });

    } finally {
      collection.remove(missId);
      collection.remove(hitId);
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
