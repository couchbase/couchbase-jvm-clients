/*
 * Copyright 2022 Couchbase, Inc.
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
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.java.manager.query.BuildQueryIndexOptions.buildDeferredQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.CreateQueryIndexOptions.createQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropPrimaryQueryIndexOptions.dropPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropQueryIndexOptions.dropQueryIndexOptions;
import static com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions.getAllQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.REQUIRE_MB_50132;
import static com.couchbase.client.java.manager.query.WatchQueryIndexesOptions.watchQueryIndexesOptions;
import static com.couchbase.client.test.Capabilities.COLLECTIONS;
import static com.couchbase.client.test.Capabilities.QUERY;
import static com.couchbase.client.test.Capabilities.SUBDOC_REVIVE_DOCUMENT;
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

/**
 * Very similar to the {@link QueryIndexManagerIntegrationTest}, but this one tests with scope and collection support.
 */
@IgnoreWhen(clusterTypes = { CAVES, MOCKED },
  missesCapabilities = { QUERY, COLLECTIONS },
  clusterVersionIsBelow = REQUIRE_MB_50132
)
public class QueryCollectionsIndexManagerIntegrationTest extends JavaIntegrationTest {

  // time to allow for watch operations that are expected to succeed eventually
  private static final Duration watchTimeout = Duration.ofSeconds(15);
  private static final String scopeName = UUID.randomUUID().toString();
  private static final String collectionName = UUID.randomUUID().toString();

  private static Cluster cluster;
  private static QueryIndexManager indexes;
  private static String bucketName;

  @BeforeAll
  static void setup() throws Exception {
    cluster = createCluster();
    bucketName = config().bucketname();
    indexes = cluster.queryIndexes();

    Bucket bucket = cluster.bucket(bucketName);
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.QUERY);
    waitForQueryIndexerToHaveKeyspace(cluster, bucketName);

    bucket.collections().createScope(scopeName);
    ConsistencyUtil.waitUntilScopePresent(cluster.core(), config().bucketname(), scopeName);
    bucket.collections().createCollection(CollectionSpec.create(collectionName, scopeName));
    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), config().bucketname(), scopeName, collectionName);
    waitForQueryIndexerToHaveKeyspace(cluster, collectionName);
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
    indexes
      .getAllIndexes(bucketName, enrich(getAllQueryIndexesOptions()))
      .forEach(idx ->
        indexes.dropIndex(bucketName, idx.name(), enrich(dropQueryIndexOptions()))
      );
    assertEquals(
      emptyList(),
      indexes.getAllIndexes(bucketName, enrich(getAllQueryIndexesOptions()))
    );
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // needs fixed STG query error handling
  @Test
  void createDuplicatePrimaryIndex() {
    CreatePrimaryQueryIndexOptions options = enrich(createPrimaryQueryIndexOptions());

    indexes.createPrimaryIndex(bucketName, options);
    assertThrows(IndexExistsException.class, () -> indexes.createPrimaryIndex(bucketName, options));

    // but this should succeed
    indexes.createPrimaryIndex(bucketName, options.ignoreIfExists(true));
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // needs fixed STG query error handling
  @Test
  void createDuplicateSecondaryIndex() {
    final String indexName = "foo" + UUID.randomUUID();
    final Set<String> fields = setOf("fieldA", "fieldB");

    CreateQueryIndexOptions options = enrich(createQueryIndexOptions());

    indexes.createIndex(bucketName, indexName, fields, options);
    assertThrows(IndexExistsException.class, () -> indexes.createIndex(bucketName, indexName, fields, options));

    // but this should succeed
    indexes.createIndex(bucketName, indexName, fields, options.ignoreIfExists(true));
  }

  @Test
  void createAndListIndexes() {
    CreatePrimaryQueryIndexOptions createOptions = enrich(createPrimaryQueryIndexOptions());
    indexes.createPrimaryIndex(bucketName, createOptions.indexName("myPrimary"));

    final Set<String> fields = setOf("fieldB.foo", "`fieldB`.`bar`");
    CreateQueryIndexOptions createIndexOptions = enrich(createQueryIndexOptions());
    indexes.createIndex(bucketName, "mySecondary", fields, createIndexOptions);

    GetAllQueryIndexesOptions getOptions = enrich(getAllQueryIndexesOptions());
    assertTrue(indexes.getAllIndexes(bucketName).stream().anyMatch(i -> i.name().equals("myPrimary")));
    assertTrue(indexes.getAllIndexes(bucketName, getOptions).stream().anyMatch(i -> i.name().equals("myPrimary")));
    assertTrue(indexes.getAllIndexes(bucketName).stream().anyMatch(i -> i.name().equals("mySecondary")));
    assertTrue(indexes.getAllIndexes(bucketName, getOptions).stream().anyMatch(i -> i.name().equals("mySecondary")));
  }

  @Test
  void createPrimaryIndex() {
    try {
      indexes.createPrimaryIndex(bucketName, enrich(createPrimaryQueryIndexOptions().numReplicas(0)));
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
    indexes.createIndex(bucketName, indexName, fields, enrich(createQueryIndexOptions()));

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

  private static QueryIndex getIndex(String indexName) {
    return getIndex(indexName, scopeName, collectionName);
  }

  private static QueryIndex getIndex(String indexName, String scope, String collection) {
    return indexes.getAllIndexes(bucketName, getAllQueryIndexesOptions()
        .scopeName(scope)
        .collectionName(collection))
      .stream()
      .filter(it -> it.name().equals(indexName))
      .findFirst()
      .orElseThrow(() -> new AssertionError("Index '" + indexName + "' not found in scope '" + scope + "' collection '" + collection + "'"));
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // needs fixed STG query error handling
  @Test
  void dropPrimaryIndex() {
    assertThrows(IndexNotFoundException.class, () ->
      indexes.dropPrimaryIndex(bucketName, enrich(dropPrimaryQueryIndexOptions())));

    indexes.dropPrimaryIndex(bucketName, enrich(dropPrimaryQueryIndexOptions().ignoreIfNotExists(true)));

    indexes.createPrimaryIndex(bucketName, enrich(createPrimaryQueryIndexOptions()));
    assertTrue(getIndex("#primary").primary());

    indexes.dropPrimaryIndex(bucketName, enrich(dropPrimaryQueryIndexOptions()));
    assertNoIndexesPresent();
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // needs fixed STG query error handling
  @Test
  void dropIndex() {
    assertThrows(IndexNotFoundException.class, () -> indexes.dropIndex(bucketName, "foo", enrich(dropQueryIndexOptions())));

    indexes.dropIndex(bucketName, "foo", enrich(dropQueryIndexOptions()
      .ignoreIfNotExists(true)));

    indexes.createIndex(bucketName, "foo", setOf("a", "b"), enrich(createQueryIndexOptions()));
    assertFalse(getIndex("foo").primary());

    indexes.dropIndex(bucketName, "foo", enrich(dropQueryIndexOptions()));
    assertNoIndexesPresent();
  }

  @Test
  void dropNamedPrimaryIndex() {
    indexes.createPrimaryIndex(bucketName, enrich(createPrimaryQueryIndexOptions()
      .indexName("namedPrimary").timeout(Duration.ofSeconds(120))));
    assertTrue(getIndex("namedPrimary").primary());
    indexes.dropIndex(bucketName, "namedPrimary", enrich(dropQueryIndexOptions()));
    assertNoIndexesPresent();
  }

  @Test
  void buildZeroDeferredIndexes() {
    // nothing to do, but shouldn't fail
    indexes.buildDeferredIndexes(bucketName, enrich(buildDeferredQueryIndexesOptions()));
  }

  @Test
  void buildOneDeferredIndex() {
    createDeferredIndex("hyphenated-index-name");
    assertEquals("deferred", getIndex("hyphenated-index-name").state());

    indexes.buildDeferredIndexes(bucketName, enrich(buildDeferredQueryIndexesOptions()));
    assertAllIndexesComeOnline(bucketName);
  }

  @Test
  void buildTwoDeferredIndexes() {
    createDeferredIndex("indexOne");
    createDeferredIndex("indexTwo");
    assertEquals("deferred", getIndex("indexOne").state());
    assertEquals("deferred", getIndex("indexTwo").state());

    indexes.buildDeferredIndexes(bucketName, enrich(buildDeferredQueryIndexesOptions()));
    assertAllIndexesComeOnline(bucketName);
  }

  @Test
  void buildDeferredIndexOnAbsentBucket() {
    indexes.buildDeferredIndexes("noSuchBucket", enrich(buildDeferredQueryIndexesOptions()));
  }

  @Test
  void buildDeferredIndexesInDifferentCollections() {
    // deferred index in default collection
    indexes.createIndex(bucketName, "indexOne", setOf("someField"), createQueryIndexOptions().deferred(true));

    // deferred index in scope/collection managed by this test suite
    createDeferredIndex("indexTwo");

    // both should initially be deferred
    assertEquals("deferred", getIndex("indexOne", "_default", "_default").state());
    assertEquals("deferred", getIndex("indexTwo").state());

    // build in default collection
    indexes.buildDeferredIndexes(bucketName);
    assertAllIndexesComeOnline(bucketName, "_default", "_default");

    // index in other collection should still be deferred
    assertEquals("deferred", getIndex("indexTwo").state());

    // build the index in the scope/collection managed by this test suite
    indexes.buildDeferredIndexes(bucketName, enrich(buildDeferredQueryIndexesOptions()));
    assertAllIndexesComeOnline(bucketName);
  }

  @Test
  void canWatchZeroIndexes() {
    indexes.watchIndexes(bucketName, listOf(), Duration.ofSeconds(3), enrich(watchQueryIndexesOptions()));
  }

  @Test
  void watchingAbsentIndexThrowsException() {
    assertThrows(IndexNotFoundException.class, () ->
      indexes.watchIndexes(bucketName, listOf("doesNotExist"), Duration.ofSeconds(3), enrich(watchQueryIndexesOptions())));
  }

  @Test
  void watchingAbsentPrimaryIndexThrowsException() {
    assertThrows(IndexNotFoundException.class, () ->
      indexes.watchIndexes(bucketName, listOf(), Duration.ofSeconds(3), enrich(watchQueryIndexesOptions()
        .watchPrimary(true))));
  }

  @Test
  void canWatchAlreadyBuiltIndex() {
    indexes.createIndex(bucketName, "myIndex", setOf("someField"), enrich(createQueryIndexOptions()));
    assertAllIndexesComeOnline(bucketName);

    indexes.watchIndexes(bucketName, listOf("myIndex"), watchTimeout, enrich(watchQueryIndexesOptions()));
  }

  @Test
  void watchTimesOutIfOneIndexStaysDeferred() {
    indexes.createIndex(bucketName, "indexOne", setOf("someField"), enrich(createQueryIndexOptions()));
    indexes.watchIndexes(bucketName, listOf("indexOne"), watchTimeout, enrich(watchQueryIndexesOptions()));

    createDeferredIndex("indexTwo");

    TimeoutException e = assertThrowsCause(TimeoutException.class, () ->
      indexes.watchIndexes(bucketName, listOf("indexOne", "indexTwo"), Duration.ZERO, enrich(watchQueryIndexesOptions())));
    assertTrue(e.getMessage().contains("indexTwo=deferred"));
  }

  private static <T extends Throwable> T assertThrowsCause(Class<T> expectedType, Executable executable) {
    Throwable t = assertThrows(Throwable.class, executable);
    return findCause(t, expectedType).orElseThrow(() ->
      new AssertionError("Expected throwable's causal chain to have an instance of "
        + expectedType + " but no such instance was present; top-level exception is " + t));
  }

  private static void createDeferredIndex(String indexName) {
    indexes.createIndex(bucketName, indexName, setOf("someField"), enrich(createQueryIndexOptions()
      .deferred(true)));
  }

  private static void createDeferredPrimaryIndex(String indexName) {
    indexes.createPrimaryIndex(bucketName, enrich(createPrimaryQueryIndexOptions()
      .indexName(indexName)
      .deferred(true)));
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
        indexes.buildDeferredIndexes(bucketName, enrich(buildDeferredQueryIndexesOptions()));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).start();

    Set<String> indexNames = setOf("indexOne", "indexTwo", "indexThree");
    indexes.watchIndexes(bucketName, indexNames, watchTimeout, enrich(watchQueryIndexesOptions()
      .watchPrimary(true))); // redundant, since the primary index was explicitly named; make sure it works anyway

    assertAllIndexesAlreadyOnline(bucketName);
  }

  @Test
  void reactiveSmokeTest() {
    ReactiveQueryIndexManager reactive = indexes.reactive();

    reactive.createPrimaryIndex(bucketName, enrich(createPrimaryQueryIndexOptions().deferred(true).timeout(Duration.ofSeconds(120))))
      .then(reactive.buildDeferredIndexes(bucketName, enrich(buildDeferredQueryIndexesOptions())))
      .then(reactive.watchIndexes(bucketName, setOf("#primary"), watchTimeout, enrich(watchQueryIndexesOptions())))
      .block();

    assertEquals("online", getIndex("#primary").state());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // needs fixed STG query error handling
  @Test
  void reactiveErrorPropagationSmokeTest() {
    assertThrows(IndexNotFoundException.class, () ->
      indexes.reactive().dropIndex(bucketName, "doesNotExist", enrich(dropQueryIndexOptions()))
        .block());

    createDeferredPrimaryIndex("myIndex");
    assertThrowsCause(TimeoutException.class, () ->
      indexes.reactive()
        .watchIndexes(bucketName, setOf("myIndex"), Duration.ZERO, enrich(watchQueryIndexesOptions()))
        .block());
  }

  @IgnoreWhen(isProtostellarWillWorkLater = true) // needs fixed STG query error handling
  @Test
  void reactiveMonosAreColdAndRepeatable() throws InterruptedException {
    // This should NOT result in the index being created.
    Mono<Void> notSubscribingToThis = indexes.reactive().createPrimaryIndex(bucketName, enrich(createPrimaryQueryIndexOptions()));
    MILLISECONDS.sleep(500);

    Mono<Void> createPrimary = indexes.reactive().createPrimaryIndex(bucketName, enrich(createPrimaryQueryIndexOptions()));
    createPrimary.block(); // subscribe; this should trigger index creation

    // subscribe again and expect the Mono to try creating the index again.
    assertThrows(IndexExistsException.class, createPrimary::block);
  }

  private static void assertNoIndexesPresent() {
    assertEquals(emptyList(), indexes.getAllIndexes(bucketName, enrich(getAllQueryIndexesOptions())));
  }

  private static void assertAllIndexesComeOnline(String bucketName) {
    assertAllIndexesComeOnline(bucketName, scopeName, collectionName);
  }

  private static void assertAllIndexesComeOnline(String bucketName, String scopeName, String collectionName) {
    Util.waitUntilCondition(() -> indexStates(bucketName, scopeName, collectionName).equals(setOf("online")));
  }

  private static void assertAllIndexesAlreadyOnline(String bucketName) {
    assertEquals(setOf("online"), indexStates(bucketName, scopeName, collectionName));
  }

  private static Set<String> indexStates(String bucketName, String scopeName, String collectionName) {
    return indexes.getAllIndexes(bucketName, getAllQueryIndexesOptions()
            .scopeName(scopeName)
            .collectionName(collectionName))
        .stream()
        .map(QueryIndex::state)
        .collect(toSet());
  }

  private static <T> T enrich(T options) {
    try {
      Field scopeField = options.getClass().getDeclaredField("scopeName");
      scopeField.setAccessible(true);
      scopeField.set(options, scopeName);

      Field collectionField = options.getClass().getDeclaredField("collectionName");
      collectionField.setAccessible(true);
      collectionField.set(options, collectionName);

      return options;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
