/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.query;

import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.CreateQueryIndexOptions.createQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropPrimaryQueryIndexOptions.dropPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropQueryIndexOptions.dropQueryIndexOptions;
import static com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions.getAllQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.DISABLE_QUERY_TESTS_FOR_CLUSTER;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.REQUIRE_MB_50132;
import static com.couchbase.client.java.manager.query.WatchQueryIndexesOptions.watchQueryIndexesOptions;
import static com.couchbase.client.test.Capabilities.QUERY;
import static com.couchbase.client.test.ClusterType.CAVES;
import static com.couchbase.client.test.ClusterType.MOCKED;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(clusterTypes = {CAVES, MOCKED},
  missesCapabilities = QUERY,
  clusterVersionEquals = DISABLE_QUERY_TESTS_FOR_CLUSTER,
  clusterVersionIsBelow = REQUIRE_MB_50132,
  isProtostellar = true
)
public class QueryIndexManagerIntegrationTest extends JavaIntegrationTest {
  // Disabling against 5.5 as there appear to be several query bugs (SCBC-246, SCBC-251).  Hardcoding 5.5.6 as that's
  // the current 5.5-release and it's unlikely to change.
  // This is now often redundant with REQUIRE_MB_50132 - leaving for documentation purposes.
  public static final String DISABLE_QUERY_TESTS_FOR_CLUSTER = "5.5.6";

  // Any tests that are creating collections and require the indexer to be aware of those collections, needs MB-50132.
  public static final String REQUIRE_MB_50132 = "7.1.0";

  // time to allow for watch operations that are expected to succeed eventually
  private static final Duration watchTimeout = Duration.ofSeconds(15);

  private static Cluster cluster;

  private static QueryIndexManager indexes;
  private static String bucketName;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    bucketName = config().bucketname();
    indexes = cluster.queryIndexes();

    // required for pre-GCCCP servers (< 6.5)
    Bucket bucket = cluster.bucket(bucketName);
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.QUERY);
    waitForQueryIndexerToHaveKeyspace(cluster, bucketName);
  }

  @AfterAll
  static void tearDown() {
    cleanupIndexes();
    cluster.disconnect();
  }

  @BeforeEach
  void cleanup() {
    cleanupIndexes();
  }

  static void cleanupIndexes() {
    indexes.getAllIndexes(bucketName).forEach(idx -> {
      if (idx.scopeName().isPresent()) {
        indexes.dropIndex(bucketName, idx.name(), DropQueryIndexOptions.dropQueryIndexOptions()
        .scopeName(idx.scopeName().get())
        .collectionName(idx.collectionName().get())
        );
      } else {
        indexes.dropIndex(bucketName, idx.name());
      }
    });
    assertEquals(emptyList(), indexes.getAllIndexes(bucketName));
  }

  @Test
  void createDuplicatePrimaryIndex() {
    indexes.createPrimaryIndex(bucketName);
    assertThrows(IndexExistsException.class, () -> indexes.createPrimaryIndex(bucketName));

    // but this should succeed
    indexes.createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions()
        .ignoreIfExists(true));
  }

  @Test
  void createDuplicateSecondaryIndex() {
    final String indexName = "foo" + UUID.randomUUID().toString();
    final Set<String> fields = setOf("fieldA", "fieldB");

    indexes.createIndex(bucketName, indexName, fields);
    assertThrows(IndexExistsException.class,
        () -> indexes.createIndex(bucketName, indexName, fields));

    // but this should succeed
    indexes.createIndex(bucketName, indexName, fields, createQueryIndexOptions()
        .ignoreIfExists(true));
  }

  @Test
  void createPrimaryIndex() {
    try {
      indexes.createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions().numReplicas(0));
    } catch (IndexExistsException ex) {
      // this is fine, might happen if some other tests race
    }

    QueryIndex index = getIndex("#primary");
    assertTrue(index.primary());
  }

  @Test
  void createIndex() {
    final String indexName = "myIndex";
    final Set<String> fields = setOf("fieldB.foo", "`fieldB`.`bar`");
    indexes.createIndex(bucketName, indexName, fields);

    final QueryIndex index = getIndex(indexName);
    assertFalse(index.primary());
    assertEquals("gsi", index.raw().getString("using"));
    assertFalse(index.partition().isPresent());

    Set<String> roundTripFields = index.indexKey().toList().stream()
        .map(Object::toString)
        .collect(toSet());

    Set<String> expectedRoundTripFields = setOf("(`fieldB`.`foo`)", "(`fieldB`.`bar`)");
    assertEquals(expectedRoundTripFields, roundTripFields);
  }

  @Test
  void getAllIndexesReturnsIndexesOnDefaultCollection() {
    indexes.createPrimaryIndex(bucketName);

    assertEquals(1, indexes.getAllIndexes(bucketName).size());

    assertEquals(1, indexes.getAllIndexes(bucketName, getAllQueryIndexesOptions()
            .scopeName("_default"))
        .size());

    assertEquals(1, indexes.getAllIndexes(bucketName, getAllQueryIndexesOptions()
            .scopeName("_default")
            .collectionName("_default"))
        .size());
  }

  private static QueryIndex getIndex(String name) {
    return indexes.getAllIndexes(bucketName).stream()
        .filter(index -> name.equals(index.name()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Index '" + name + "' not found."));
  }

  @Test
  void dropPrimaryIndex() {
    assertThrows(IndexNotFoundException.class, () ->
        indexes.dropPrimaryIndex(bucketName));

    indexes.dropPrimaryIndex(bucketName, dropPrimaryQueryIndexOptions().ignoreIfNotExists(true));

    indexes.createPrimaryIndex(bucketName);
    assertTrue(getIndex("#primary").primary());

    indexes.dropPrimaryIndex(bucketName, dropPrimaryQueryIndexOptions());
    assertNoIndexesPresent();
  }

  @Test
  void dropIndex() {
    assertThrows(IndexNotFoundException.class, () -> indexes.dropIndex(bucketName, "foo"));

    indexes.dropIndex(bucketName, "foo", dropQueryIndexOptions()
        .ignoreIfNotExists(true));

    indexes.createIndex(bucketName, "foo", setOf("a", "b"));
    assertFalse(getIndex("foo").primary());

    indexes.dropIndex(bucketName, "foo");
    assertNoIndexesPresent();
  }

  @Test
  void dropNamedPrimaryIndex() {
    indexes.createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions()
        .indexName("namedPrimary").timeout(Duration.ofSeconds(120)));
    assertTrue(getIndex("namedPrimary").primary());
    indexes.dropIndex(bucketName, "namedPrimary");
    assertNoIndexesPresent();
  }

  @Test
  void buildZeroDeferredIndexes() {
    // nothing to do, but shouldn't fail
    indexes.buildDeferredIndexes(bucketName);
  }

  @Test
  void buildOneDeferredIndex() {
    createDeferredIndex("hyphenated-index-name");
    assertEquals("deferred", getIndex("hyphenated-index-name").state());

    indexes.buildDeferredIndexes(bucketName);
    assertAllIndexesComeOnline(bucketName);
  }

  @Test
  void buildTwoDeferredIndexes() {
    createDeferredIndex("indexOne");
    createDeferredIndex("indexTwo");
    assertEquals("deferred", getIndex("indexOne").state());
    assertEquals("deferred", getIndex("indexTwo").state());

    indexes.buildDeferredIndexes(bucketName);
    assertAllIndexesComeOnline(bucketName);
  }

  @Test
  void buildDeferredIndexOnAbsentBucket() {
    indexes.buildDeferredIndexes("noSuchBucket");
  }

  @Test
  void canWatchZeroIndexes() {
    indexes.watchIndexes(bucketName, listOf(), Duration.ofSeconds(3));
  }

  @Test
  void watchingAbsentIndexThrowsException() {
    assertThrows(IndexNotFoundException.class, () ->
        indexes.watchIndexes(bucketName, listOf("doesNotExist"), Duration.ofSeconds(3)));
  }

  @Test
  void watchingAbsentPrimaryIndexThrowsException() {
    assertThrows(IndexNotFoundException.class, () ->
        indexes.watchIndexes(bucketName, listOf(), Duration.ofSeconds(3), watchQueryIndexesOptions()
            .watchPrimary(true)));
  }

  @Test
  void canWatchAlreadyBuiltIndex() {
    indexes.createIndex(bucketName, "myIndex", setOf("someField"));
    assertAllIndexesComeOnline(bucketName);

    indexes.watchIndexes(bucketName, listOf("myIndex"), watchTimeout);
  }

  @Test
  void watchTimesOutIfOneIndexStaysDeferred() {
    indexes.createIndex(bucketName, "indexOne", setOf("someField"));
    indexes.watchIndexes(bucketName, listOf("indexOne"), watchTimeout);

    createDeferredIndex("indexTwo");

    TimeoutException e = assertThrowsCause(TimeoutException.class, () ->
        indexes.watchIndexes(bucketName, listOf("indexOne", "indexTwo"), Duration.ZERO));
    assertTrue(e.getMessage().contains("indexTwo=deferred"));
  }

  private static <T extends Throwable> T assertThrowsCause(Class<T> expectedType, Executable executable) {
    Throwable t = assertThrows(Throwable.class, executable);
    return findCause(t, expectedType).orElseThrow(() ->
        new AssertionError("Expected throwable's causal chain to have an instance of "
            + expectedType + " but no such instance was present; top-level exception is " + t));
  }

  private static void createDeferredIndex(String indexName) {
    indexes.createIndex(bucketName, indexName, setOf("someField"), createQueryIndexOptions()
        .deferred(true));
  }

  private static void createDeferredPrimaryIndex(String indexName) {
    indexes.createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions()
        .indexName(indexName)
        .deferred(true));
  }

  @Test
  void watchRetriesUntilIndexesComeOnline() {
    createDeferredPrimaryIndex("indexOne");
    createDeferredIndex("indexTwo");
    createDeferredIndex("indexThree");

    new Thread(() -> {
      try {
        // sleep first so the watch operation needs to poll more than once.
        SECONDS.sleep(1);
        indexes.buildDeferredIndexes(bucketName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).start();

    Set<String> indexNames = setOf("indexOne", "indexTwo", "indexThree");
    indexes.watchIndexes(bucketName, indexNames, watchTimeout, watchQueryIndexesOptions()
        .watchPrimary(true)); // redundant, since the primary index was explicitly named; make sure it works anyway

    assertAllIndexesAlreadyOnline(bucketName);
  }

  @Test
  void reactiveSmokeTest() {
    ReactiveQueryIndexManager reactive = indexes.reactive();

    reactive.createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions().deferred(true).timeout(Duration.ofSeconds(120)))
        .then(reactive.buildDeferredIndexes(bucketName))
        .then(reactive.watchIndexes(bucketName, setOf("#primary"), watchTimeout))
        .block();

    assertEquals("online", getIndex("#primary").state());
  }

  @Test
  void reactiveErrorPropagationSmokeTest() {
    assertThrows(IndexNotFoundException.class, () ->
        indexes.reactive().dropIndex(bucketName, "doesNotExist")
            .block());

    createDeferredPrimaryIndex("myIndex");
    assertThrowsCause(TimeoutException.class, () ->
        indexes.reactive()
            .watchIndexes(bucketName, setOf("myIndex"), Duration.ZERO)
            .block());
  }

  @Test
  void reactiveMonosAreColdAndRepeatable() throws InterruptedException {
    // This should NOT result in the index being created.
    Mono<Void> notSubscribingToThis = indexes.reactive().createPrimaryIndex(bucketName);
    MILLISECONDS.sleep(500);

    Mono<Void> createPrimary = indexes.reactive().createPrimaryIndex(bucketName);
    createPrimary.block(); // subscribe; this should trigger index creation

    // subscribe again and expect the Mono to try creating the index again.
    assertThrows(IndexExistsException.class, createPrimary::block);
  }

  private static void assertNoIndexesPresent() {
    assertEquals(emptyList(), indexes.getAllIndexes(bucketName));
  }

  private static void assertAllIndexesComeOnline(String bucketName) {
    Util.waitUntilCondition(() -> indexes.getAllIndexes(bucketName).stream()
        .map(QueryIndex::state)
        .collect(toSet())
        .equals(setOf("online")));
  }

  private static void assertAllIndexesAlreadyOnline(String bucketName) {
    assertEquals(setOf("online"), indexes.getAllIndexes(bucketName).stream()
        .map(QueryIndex::state)
        .collect(toSet()));
  }
}
