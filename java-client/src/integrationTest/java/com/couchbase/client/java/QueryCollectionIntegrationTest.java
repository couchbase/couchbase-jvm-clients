/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.QueryStatus;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbThrowables.throwIfUnchecked;
import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.DISABLE_QUERY_TESTS_FOR_CLUSTER;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.REQUIRE_MB_50132;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the end-to-end functionality of Collection queries
 * <p>
 * Disabling against 5.5.  See comment on QueryIndexManagerIntegrationTest for details.
 * <p>
 * @author Michael Reiche
 */
@IgnoreWhen(
  missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS },
  clusterTypes = { ClusterType.MOCKED, ClusterType.CAVES },
  clusterVersionIsBelow = REQUIRE_MB_50132,
  clusterVersionEquals = DISABLE_QUERY_TESTS_FOR_CLUSTER,
  isProtostellarWillWorkLater = true
)
class QueryCollectionIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static CollectionManager collectionManager;

  private final static String SCOPE_NAME = "scope_" + randomString();
  private final static String COLLECTION_NAME = "collection_" + randomString();

  /**
   * Holds sample content for simple assertions.
   */
  private static final JsonObject FOO_CONTENT = JsonObject.create().put("foo", "bar");

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.QUERY);
    waitForQueryIndexerToHaveKeyspace(cluster, config().bucketname());
    collectionManager = bucket.collections();

    // Create the scope.collection (borrowed from CollectionManagerIntegrationTest )
    CollectionSpec collSpec = CollectionSpec.create(COLLECTION_NAME, SCOPE_NAME);

    collectionManager.createScope(SCOPE_NAME);
    ConsistencyUtil.waitUntilScopePresent(cluster.core(), bucket.name(), SCOPE_NAME);
    waitUntilCondition(() -> scopeExists(collectionManager, SCOPE_NAME));

    collectionManager.createCollection(collSpec);
    ConsistencyUtil.waitUntilCollectionPresent(cluster.core(), bucket.name(), collSpec.scopeName(), collSpec.name());
    waitUntilCondition(() -> collectionExists(collectionManager, collSpec));

    waitForQueryIndexerToHaveKeyspace(cluster, COLLECTION_NAME);

    // the call to createPrimaryIndex takes about 60 seconds
    block(createPrimaryIndex(config().bucketname(), SCOPE_NAME, COLLECTION_NAME));
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  void performsAdhocQuery() {
    Scope scope = cluster.bucket(config().bucketname()).scope(SCOPE_NAME);
    Collection collection = scope.collection(COLLECTION_NAME);

    String id = insertDoc(collection);

    QueryOptions options = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
    QueryResult result = scope.query("select * from `" + COLLECTION_NAME + "` where meta().id=\"" + id + "\"", options);
    assertEquals(QueryStatus.SUCCESS, result.metaData().status());
    assertEquals(1, result.rowsAsObject().size());

    ReactiveScope reactiveScope = cluster.bucket(config().bucketname()).reactive().scope(SCOPE_NAME);
    ReactiveQueryResult reactiveResult = reactiveScope
        .query("select * from `" + COLLECTION_NAME + "` where meta().id=\"" + id + "\"", options).block();
    assertEquals(QueryStatus.SUCCESS, reactiveResult.metaData().block().status());
    assertEquals(1, reactiveResult.rowsAsObject().blockLast().size());
  }

  @Test
  void performsNonAdhocQuery() {
    Scope scope = cluster.bucket(config().bucketname()).scope(SCOPE_NAME);
    Collection collection = scope.collection(COLLECTION_NAME);

    String id = insertDoc(collection);
    QueryResult result = scope.query(
      "select meta().id as id from `" + COLLECTION_NAME + "`",
      queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS).adhoc(false)
    );

    boolean hasDoc = false;
    for (JsonObject row : result.rowsAsObject()) {
      if (row.getString("id").equals(id)) {
        hasDoc = true;
      }
    }
    assertTrue(hasDoc);
  }

  //Test for MB-46876
  @Test
  void consistentWith() {
    String id = UUID.randomUUID().toString();
    Scope scope = cluster.bucket(config().bucketname()).scope(SCOPE_NAME);
    Collection collection = scope.collection(COLLECTION_NAME);
    MutationResult mr = collection.insert(id, FOO_CONTENT);

    QueryOptions options = queryOptions()
      .consistentWith(MutationState.from(mr.mutationToken().get()))
      .parameters(JsonArray.from(id));
    QueryResult result = scope.query(
      "select * from `" + COLLECTION_NAME + "` where meta().id=$1",
      options
    );
    List<JsonObject> rows = result.rowsAs(JsonObject.class);
    assertEquals(1, rows.size());
    assertEquals(FOO_CONTENT, rows.get(0).getObject(COLLECTION_NAME));
  }

  /**
   * Inserts a document into the collection and returns the ID of it. It inserts {@link #FOO_CONTENT}.
   */
  private static String insertDoc(Collection collection) {
    String id = UUID.randomUUID().toString();
    collection.insert(id, FOO_CONTENT);
    return id;
  }

  private static String randomString() {
    return UUID.randomUUID().toString().substring(0, 10);
  }

  private static CompletableFuture<Void> createPrimaryIndex(String bucketName, String scopeName, String collectionName) {
    CreatePrimaryQueryIndexOptions options = CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions();
    options.timeout(Duration.ofSeconds(300));
    final CreatePrimaryQueryIndexOptions.Built builtOpts = options.build();
    final String indexName = builtOpts.indexName().orElse(null);

    String keyspace = "`default`:`" + bucketName + "`.`" + scopeName + "`.`" + collectionName + "`";
    String statement = "CREATE PRIMARY INDEX ";
    if (indexName != null) {
      statement += (indexName) + " ";
    }
    statement += "ON " + (keyspace); // do not quote, this might be "default:bucketName.scopeName.collectionName"

    return exec(false, statement, builtOpts.with(), builtOpts).exceptionally(t -> {
      if (builtOpts.ignoreIfExists() && hasCause(t, IndexExistsException.class)) {
        return null;
      }
      throwIfUnchecked(t);
      throw new RuntimeException(t);
    }).thenApply(result -> null);
  }

  private static CompletableFuture<QueryResult> exec(boolean queryType,
      CharSequence statement, Map<String, Object> with, CommonOptions<?>.BuiltCommonOptions options) {
    return with.isEmpty() ? exec(queryType, statement, options)
        : exec(queryType, statement + " WITH " + Mapper.encodeAsString(with), options);
  }

  private static CompletableFuture<QueryResult> exec(boolean queryType,
      CharSequence statement, CommonOptions<?>.BuiltCommonOptions options) {
    QueryOptions queryOpts = toQueryOptions(options).readonly(queryType);

    return cluster.async().query(statement.toString(), queryOpts).exceptionally(t -> {
      throw translateException(t);
    });
  }

  private static QueryOptions toQueryOptions(CommonOptions<?>.BuiltCommonOptions options) {
    QueryOptions result = queryOptions();
    options.timeout().ifPresent(result::timeout);
    options.retryStrategy().ifPresent(result::retryStrategy);
    return result;
  }

  private static final Map<Predicate<QueryException>, Function<QueryException, ? extends QueryException>> errorMessageMap = new LinkedHashMap<>();

  private static RuntimeException translateException(Throwable t) {
    if (t instanceof QueryException) {
      final QueryException e = ((QueryException) t);

      for (Map.Entry<Predicate<QueryException>, Function<QueryException, ? extends QueryException>> entry : errorMessageMap
          .entrySet()) {
        if (entry.getKey().test(e)) {
          return entry.getValue().apply(e);
        }
      }
    }
    return (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
  }
}
