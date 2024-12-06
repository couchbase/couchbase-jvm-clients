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

import com.couchbase.client.core.classic.query.ClassicCoreQueryOps;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.ParsingFailureException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.QueryErrorContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.query.QueryMetaData;
import com.couchbase.client.java.query.QueryMetrics;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.QueryStatus;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.DISABLE_QUERY_TESTS_FOR_CLUSTER;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.REQUIRE_MB_50132;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the end-to-end functionality of the Query service.
 * <p>
 * Disabling against 5.5.  See comment on QueryIndexManagerIntegrationTest for details.
 */
@IgnoreWhen(
  missesCapabilities = {Capabilities.QUERY, Capabilities.CLUSTER_LEVEL_QUERY},
  clusterVersionEquals = DISABLE_QUERY_TESTS_FOR_CLUSTER,
  clusterVersionIsBelow = REQUIRE_MB_50132
)
class QueryIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection collection;
    private static String bucketName;

    /**
     * Holds sample content for simple assertions.
     */
    private static final JsonObject FOO_CONTENT = JsonObject
      .create()
      .put("foo", "bar");

    @BeforeAll
    static void setup() {
        cluster = createCluster(env -> env.ioConfig(io -> io.enableMutationTokens(true)));
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
        waitForService(bucket, ServiceType.QUERY);
        waitForQueryIndexerToHaveKeyspace(cluster, config().bucketname());

        bucketName = "`" + config().bucketname() + "`";
        createPrimaryIndex(cluster, config().bucketname());
    }

    @AfterAll
    static void tearDown() {
        cluster.disconnect();
    }

    @Test
    void simpleBlockingSelect() {
        QueryResult result = cluster.query("select 'hello world' as Greeting", queryOptions().metrics(true));

        assertNotNull(result.metaData().requestId());
        assertFalse(result.metaData().clientContextId().isEmpty());
        assertEquals(QueryStatus.SUCCESS, result.metaData().status());
        assertTrue(result.metaData().warnings().isEmpty());
        assertEquals(1, result.rowsAs(JsonObject.class).size());
        assertTrue(result.metaData().signature().isPresent());

        QueryMetrics metrics = result.metaData().metrics().get();
        assertEquals(0, metrics.errorCount());
        assertEquals(0, metrics.warningCount());
        assertEquals(1, metrics.resultCount());
    }

    private static class GreetingHolder {
        public String greeting;
    }

    @Test
    void simpleBlockingStreamingSelect() {
        List<JsonObject> jsonObjects = new ArrayList<>();
        List<Map<String, Object>> maps = new ArrayList<>();
        List<GreetingHolder> pojos = new ArrayList<>();

        QueryMetaData metaData = cluster.queryStreaming(
            "SELECT 'hello world' AS greeting",
            queryOptions().metrics(true),
            row -> {
                jsonObjects.add(row.contentAsObject());
                maps.add(row.contentAs(new TypeRef<Map<String, Object>>() {}));
                pojos.add(row.contentAs(GreetingHolder.class));
            }
        );

        assertEquals(
            listOf(JsonObject.create().put("greeting", "hello world")),
            jsonObjects
        );
        assertEquals(
            listOf(mapOf("greeting", "hello world")),
            maps
        );
        assertEquals(
            listOf("hello world"),
            transform(pojos, pojo -> pojo.greeting)
        );

        assertNotNull(metaData.requestId());
        assertFalse(metaData.clientContextId().isEmpty());
        assertEquals(QueryStatus.SUCCESS, metaData.status());
        assertEquals(emptyList(), metaData.warnings());
        assertTrue(metaData.signature().isPresent());

        QueryMetrics metrics = metaData.metrics().get();
        assertEquals(0, metrics.errorCount());
        assertEquals(0, metrics.warningCount());
        assertEquals(1, metrics.resultCount());
    }

    /**
     * The blocking streaming implementation processes rows in batches.
     * Make sure it works when the result row count is greater than the batch size.
     */
    @Test
    void blockingStreamingCanReturnManyRows() {
        int numRows = 10_000;
        List<Integer> resultIds = new ArrayList<>();
        cluster.queryStreaming(
            "SELECT RAW i FROM ARRAY_RANGE(0," + numRows + ") AS i",
            row -> resultIds.add(row.contentAs(Integer.class))
        );

        assertEquals(
            IntStream.range(0, numRows).boxed().collect(toList()),
            resultIds
        );
    }

    private static final class FakeException extends RuntimeException {}

    @Test
    void blockingStreamingPropagatesException() {
        assertThrows(
            FakeException.class,
            () -> cluster.queryStreaming(
                "SELECT 1",
                row -> {
                    throw new FakeException();
                }
            )
        );
    }

    @Test
    void blockingStreamingRowAsAllowsNull() {
        List<String> results = new ArrayList<>();
        cluster.queryStreaming(
            "SELECT RAW null",
            row -> results.add(row.contentAs(String.class))
        );
        assertEquals(
            singletonList(null),
            results
        );
    }

    @Test
    void simpleBlockingStreamingRowAsThrowsDecodingFailure() {
        assertThrows(
            DecodingFailureException.class,
            () -> cluster.queryStreaming(
                "SELECT 1",
                row -> row.contentAs(GreetingHolder.class)
            )
        );
    }

    @Test
    void blockingStreamingRowActionRunsInCallerThread() {
        AtomicReference<Thread> rowActionThread = new AtomicReference<>();
        cluster.queryStreaming(
            "SELECT 1",
            row -> rowActionThread.set(Thread.currentThread())
        );
        assertSame(Thread.currentThread(), rowActionThread.get());
    }

    @Test
    void blockingStreamingCanTimeOut() {
        // Thanks vsr1! https://www.couchbase.com/forums/t/how-to-force-a-sql-query-to-take-a-long-time/39658
        String verySlowStatement = "SELECT COUNT (1) AS c FROM" +
            " ARRAY_RANGE(0,100000) AS d1," +
            " ARRAY_RANGE(0,100000) AS d2," +
            " ARRAY_RANGE(0,100000) AS d3";

        assertThrows(
            TimeoutException.class,
            () -> cluster.queryStreaming(
                verySlowStatement,
                queryOptions()
                    .timeout(Duration.ofMillis(1)),
                row -> fail("Did not expect to receive result row")
            )
        );
    }

    @Test
    void blockingStreamingThrowsCancellationWhenThreadAlreadyInterrupted() {
        Thread.currentThread().interrupt();
        assertThrows(
            CancellationException.class,
            () -> cluster.queryStreaming(
                "SELECT 1",
                row -> fail("Did not expect to receive result row")
          )
        );
        assertTrue(Thread.interrupted());
    }

    @Test
    void blockingStreamingThrowsCancellationWhenInterruptedInCallback() {
        assertThrows(
            CancellationException.class,
            () -> cluster.queryStreaming(
                "SELECT 1",
                row -> Thread.currentThread().interrupt()
            )
        );
        assertTrue(Thread.interrupted());
    }

    @Test
    void exerciseAllOptions() {
        String id = insertDoc();
        QueryOptions options = queryOptions()
            .adhoc(true)
            .clientContextId("123")
            .maxParallelism(3)
            .metrics(true)
            .pipelineBatch(1)
            .pipelineCap(1)
            .readonly(true)
            .scanCap(10)
            .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
            .scanWait(Duration.ofMillis(50));
        QueryResult result = cluster.query(
            "select * from " + bucketName + " where meta().id=\"" + id + "\"",
            options
        );

        assertEquals(QueryStatus.SUCCESS, result.metaData().status());
        assertEquals("123", result.metaData().clientContextId());
        assertTrue(result.metaData().metrics().isPresent());
    }

    @Test
    void readOnlyViolation() {
        QueryOptions options = queryOptions().readonly(true);
        CouchbaseException e = assertThrows(CouchbaseException.class, () ->
            cluster.query(
                "INSERT INTO " + bucketName + " (KEY, VALUE) values (\"foo\", \"bar\")",
                options
            ));
        if (!config().isProtostellar()) assertEquals(1000, ((QueryErrorContext) e.context()).errors().get(0).code());
    }

    @Test
    void blockingSelect() {
        String id = insertDoc();

        QueryOptions options = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        QueryResult result = cluster.query(
          "select * from " + bucketName + " where meta().id=\"" + id + "\"",
          options
        );

        assertNotNull(result.metaData().requestId());
        assertFalse(result.metaData().clientContextId().isEmpty());
        assertEquals(QueryStatus.SUCCESS, result.metaData().status());
        assertTrue(result.metaData().warnings().isEmpty());
        assertEquals(1, result.rowsAs(JsonObject.class).size());
        assertTrue(result.metaData().signature().isPresent());
    }

    @Test
    void asyncSelect() throws Exception {
        String id = insertDoc();

        QueryOptions options = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        CompletableFuture<QueryResult> result = cluster.async().query(
          "select * from " + bucketName + " where meta().id=\"" + id + "\"",
          options
        );
        List<JsonObject> rows = result.get().rowsAs(JsonObject.class);
        assertEquals(1, rows.size());
    }

    @Test
    void reactiveSelect() {
        String id = insertDoc();

        QueryOptions options = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        Mono<ReactiveQueryResult> result = cluster.reactive().query(
          "select * from " + bucketName + " where meta().id=\"" + id + "\"",
          options
        );
        List<JsonObject> rows = result
          .flux()
          .flatMap(ReactiveQueryResult::rowsAsObject)
          .collectList()
          .block();
        assertNotNull(rows);
        assertEquals(1, rows.size());
    }

    @Test
    void noProfileRequestedGivesEmptyProfile() {
        String id = insertDoc();

        QueryOptions options = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
        QueryResult result = cluster.query(
          "select * from " + bucketName + " where meta().id=\"" + id + "\"",
          options
        );
        assertFalse(result.metaData().profile().isPresent());
    }

    @IgnoreWhen(missesCapabilities = {Capabilities.ENTERPRISE_EDITION})
    @Test
    void getProfileWhenRequested() {
        String id = insertDoc();

        QueryOptions options = queryOptions().profile(QueryProfile.TIMINGS);
        QueryResult result = cluster.query(
          "select * from " + bucketName + " where meta().id=\"" + id +"\"",
          options
        );
        JsonObject profile = result.metaData().profile().get();
        assertTrue(profile.size() > 0);
    }

    @Test
    void failOnSyntaxError() {
      if (config().isProtostellar()) {
        assertThrows(InvalidArgumentException.class, () -> cluster.query("invalid export"));
      }
      else {
        assertThrows(ParsingFailureException.class, () -> cluster.query("invalid export"));
      }
    }

    @Test
    void blockingNamedParameterizedSelectQuery() {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
          .parameters(JsonObject.create().put("id", id));
        QueryResult result = cluster.query(
          "select " + bucketName + ".* from " + bucketName + " where meta().id=$id",
          options
        );
        List<JsonObject> rows = result.rowsAs(JsonObject.class);
        assertEquals(1, rows.size());
        assertEquals(FOO_CONTENT, rows.get(0));
    }

    @Test
    void asyncNamedParameterizedSelectQuery() throws Exception {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
          .parameters(JsonObject.create().put("id", id));
        CompletableFuture<QueryResult> result = cluster.async().query(
          "select * from " + bucketName + " where meta().id=$id",
          options
        );
        List<JsonObject> rows = result.get().rowsAs(JsonObject.class);
        assertEquals(1, rows.size());
    }

    @Test
    void reactiveNamedParameterizedSelectQuery() {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
          .parameters(JsonObject.create().put("id", id));
        Mono<ReactiveQueryResult> result = cluster.reactive().query(
          "select * from " + bucketName + " where meta().id=$id",
          options
        );
        List<JsonObject> rows = result
          .flux()
          .flatMap(ReactiveQueryResult::rowsAsObject)
          .collectList()
          .block();
        assertNotNull(rows);
        assertEquals(1, rows.size());
    }

    @Test
    void blockingPositionalParameterizedSelectQuery() {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
          .parameters(JsonArray.from(id));
        QueryResult result = cluster.query(
          "select  " + bucketName + ".* from " + bucketName + " where meta().id=$1",
          options
        );
        List<JsonObject> rows = result.rowsAs(JsonObject.class);
        assertEquals(1, rows.size());
        assertEquals(FOO_CONTENT, rows.get(0));
    }

    @Test
    void asyncPositionalParameterizedSelectQuery() throws Exception {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
          .parameters(JsonArray.from(id));
        CompletableFuture<QueryResult> result = cluster.async().query(
          "select * from " + bucketName+ " where meta().id=$1",
          options
        );
        List<JsonObject> rows = result.get().rowsAs(JsonObject.class);
        assertEquals(1, rows.size());
    }

    @Test
    void reactivePositionalParameterizedSelectQuery() {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
          .parameters(JsonArray.from(id));
        Mono<ReactiveQueryResult> result =  cluster.reactive().query(
          "select * from " + bucketName + " where meta().id=$1",
          options
        );
        List<JsonObject> rows = result
          .flux()
          .flatMap(ReactiveQueryResult::rowsAsObject)
          .collectList()
          .block();
        assertNotNull(rows);
        assertEquals(1, rows.size());
    }

    @IgnoreWhen(isProtostellarWillWorkLater = true) // Needs ING-540
    @Test
    void consistentWith() {
        String id = UUID.randomUUID().toString();
        MutationResult mr = collection.insert(id, FOO_CONTENT);

        QueryOptions options = queryOptions()
                .consistentWith(MutationState.from(mr.mutationToken().get()))
                .parameters(JsonArray.from(id));
        QueryResult result = cluster.query(
                "select  " + bucketName + ".* from " + bucketName + " where meta().id=$1",
                options
        );
        List<JsonObject> rows = result.rowsAs(JsonObject.class);
        assertEquals(1, rows.size());
        assertEquals(FOO_CONTENT, rows.get(0));
    }

    /**
     * This test is intentionally kept generic, since we want to make sure with every query version
     * that we run against we have a version that works. Also, we perform the same query multiple times
     * to make sure a primed and non-primed cache both work out of the box.
     */
    @Test
    void handlesPreparedStatements() {
        String id = insertDoc();

        for (int i = 0; i < 10; i++) {
            QueryOptions options = queryOptions()
              .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
              .adhoc(false);
            QueryResult result = cluster.query(
              "select " + bucketName + ".* from " + bucketName + " where meta().id=\"" + id + "\"",
              options
            );

            List<JsonObject> rows = result.rowsAs(JsonObject.class);
            assertEquals(1, rows.size());
            assertEquals(FOO_CONTENT, rows.get(0));
        }
    }

    @Test
    void handlesPreparedStatementsWithNamedArgs() {
        String id = insertDoc();

        for (int i = 0; i < 10; i++) {
            QueryOptions options = queryOptions()
              .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
              .parameters(JsonObject.create().put("id", id))
              .adhoc(false);
            QueryResult result = cluster.query(
              "select " + bucketName + ".* from " + bucketName + " where meta().id=$id",
              options
            );

            List<JsonObject> rows = result.rowsAs(JsonObject.class);
            assertEquals(1, rows.size());
            assertEquals(FOO_CONTENT, rows.get(0));
        }
    }

    @Test
    void handlesPreparedStatementsWithPositionalArgs() {
        String id = insertDoc();

        for (int i = 0; i < 10; i++) {
            QueryOptions options = queryOptions()
              .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
              .parameters(JsonArray.from(id))
              .adhoc(false);
            QueryResult result = cluster.query(
              "select " + bucketName + ".* from " + bucketName + " where meta().id=$1",
              options
            );

            List<JsonObject> rows = result.rowsAs(JsonObject.class);
            assertEquals(1, rows.size());
            assertEquals(FOO_CONTENT, rows.get(0));
        }
    }

    /**
     * We need to make sure that if a scope-level query is performed on an older cluster a proper exception
     * is thrown and not an "unknown query error".
     */
    @Test
    @IgnoreWhen(hasCapabilities = Capabilities.COLLECTIONS)
    void failsIfScopeLevelIsNotAvailable() {
        Scope scope = cluster.bucket(bucketName).scope("myscope");
        assertThrows(FeatureNotAvailableException.class, () ->  scope.query("select * from mycollection"));
    }

    @Test
    @IgnoreWhen(missesCapabilities = Capabilities.QUERY_PRESERVE_EXPIRY)
    void preserveExpiry() {
        String id = UUID.randomUUID().toString();
        collection.insert(id, FOO_CONTENT, InsertOptions.insertOptions()
          .expiry(Duration.ofDays(1L)));

        Instant expectedExpiry = collection.get(id, GetOptions.getOptions().withExpiry(true)).expiryTime().get();

        cluster.query(
          "UPDATE " + bucketName + " AS content USE KEYS '" + id + "' SET content.foo = 'updated'",
          queryOptions().preserveExpiry(true)
        );

        GetResult result = collection.get(id, GetOptions.getOptions().withExpiry(true));
        assertEquals("updated", result.contentAsObject().get("foo"));
        assertEquals(expectedExpiry, result.expiryTime().get());
    }

  @Test
  @IgnoreWhen(clusterVersionIsBelow="7.6")
  void useReplica() {
    String id = UUID.randomUUID().toString();
    try {
      collection.insert(id, FOO_CONTENT);

      QueryOptions options = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
      assertEquals(null, ClassicCoreQueryOps.convertOptions(options.build()).get("use_replica"));
      QueryResult queryResultNone = cluster.query(
        "SELECT * from " + bucketName + " where meta().id = \"" + id + "\"",
        options
      );
      assertEquals(1, queryResultNone.rowsAsObject().size());

      options.useReplica(false);
      assertEquals("off", ClassicCoreQueryOps.convertOptions(options.build()).get("use_replica").asText());
      QueryResult queryResultFalse = cluster.query(
        "SELECT * from " + bucketName + " where meta().id = \"" + id + "\"",
        options
      );
      assertEquals(1, queryResultFalse.rowsAsObject().size());

      options.useReplica(true);
      assertEquals("on", ClassicCoreQueryOps.convertOptions(options.build()).get("use_replica").asText());
      QueryResult queryResultTrue = cluster.query(
        "SELECT * from " + bucketName + " where meta().id = \"" + id + "\"",
        options
      );
      assertEquals(1, queryResultTrue.rowsAsObject().size());
    } finally {
      collection.remove(id);
    }
  }

  @Test
  @IgnoreWhen(clusterVersionIsEqualToOrAbove="7.1")
  void useReplicaThrowsFeatureNotAvailable() {
    String id = UUID.randomUUID().toString();
    try {
      collection.insert(id, FOO_CONTENT);

      QueryOptions options = queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS);
      options.useReplica(true);
      assertEquals("on", ClassicCoreQueryOps.convertOptions(options.build()).get("use_replica").asText());
      assertThrows( FeatureNotAvailableException.class, () -> cluster.query(
        "SELECT * from " + bucketName + " where meta().id = \"" + id + "\"",
        options
      ));
    } finally {
      collection.remove(id);
    }
  }

    @Test
    @IgnoreWhen(hasCapabilities = Capabilities.QUERY_PRESERVE_EXPIRY)
    void preserveExpiryThrowsFeatureNotAvailable() {
        FeatureNotAvailableException ex = assertThrows(FeatureNotAvailableException.class,
          () -> cluster.query("select 1=1", queryOptions().preserveExpiry(true)));
        assertTrue(ex.getMessage().contains("Preserving expiry for the query service is not supported"));
    }

    @Test
    void metricsAreAbsentUnlessRequested() {
        QueryResult result = cluster.query("select 'hello world' as Greeting", queryOptions().metrics(false));
        assertFalse(result.metaData().metrics().isPresent());

        result = cluster.query("select 'hello world' as Greeting");
        assertFalse(result.metaData().metrics().isPresent());
    }

    /**
     * Inserts a document into the collection and returns the ID of it.
     *
     * It inserts {@link #FOO_CONTENT}.
     */
    private String insertDoc() {
        String id = UUID.randomUUID().toString();
        collection.insert(id, FOO_CONTENT);
        return id;
    }

}
