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
import com.couchbase.client.core.error.QueryException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationState;
import com.couchbase.client.java.query.QueryMetrics;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the end-to-end functionality of the Query service.
 */
@IgnoreWhen( missesCapabilities = { Capabilities.QUERY })
class QueryIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static ClusterEnvironment environment;
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
        environment = environment()
                .ioConfig(IoConfig.enableMutationTokens(true).captureTraffic(ServiceType.QUERY))
                .build();
        cluster = Cluster.connect(connectionString(), ClusterOptions.clusterOptions(authenticator()).environment(environment).seedNodes(seedNodes()));
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        bucketName = "`" + config().bucketname() + "`";
        createPrimaryIndex(cluster, config().bucketname());
    }

    @AfterAll
    static void tearDown() {
        cluster.disconnect();
        environment.shutdown();
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
        QueryException e = assertThrows(QueryException.class, () ->
            cluster.query(
                "INSERT INTO " + bucketName + " (KEY, VALUE) values (\"foo\", \"bar\")",
                options
            ));
        assertEquals(1000, e.code());
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
        try {
            cluster.query("invalid export");
        }
        catch (QueryException err) {
            assertEquals("syntax error - at export", err.msg());
            assertEquals(3000, err.code());
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
