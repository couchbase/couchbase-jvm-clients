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

import com.couchbase.client.core.error.QueryServiceException;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.List;
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
        environment = environment().build();
        cluster = Cluster.connect(environment);
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        QueryResult result = cluster.query(
          "create primary index on `" + config().bucketname() + "`"
        );
        if (result.meta().status() != QueryStatus.SUCCESS) {
            throw new IllegalStateException("Could not create primary index for " +
              "query integration test!");
        }
        bucketName = "`" + config().bucketname() + "`";
    }

    @AfterAll
    static void tearDown() {
        cluster.shutdown();
        environment.shutdown();
    }

    @Test
    void simpleBlockingSelect() {
        QueryResult result = cluster.query("select 'hello world' as Greeting");

        assertNotNull(result.meta().requestId());
        assertTrue(result.meta().clientContextId().isPresent());
        assertEquals(QueryStatus.SUCCESS, result.meta().status());
        assertFalse(result.meta().warnings().isPresent());
        assertEquals(1, result.allRowsAs(JsonObject.class).size());
        assertTrue(result.meta().signature().isPresent());

        QueryMetrics metrics = result.meta().metrics().get();
        assertEquals(0, metrics.errorCount());
        assertEquals(0, metrics.warningCount());
        assertEquals(1, metrics.resultCount());
    }

    @Test
    void blockingSelect() {
        String id = insertDoc();

        QueryOptions options = queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        QueryResult result = cluster.query(
          "select * from " + bucketName + " where meta().id=\"" + id + "\"",
          options
        );

        assertNotNull(result.meta().requestId());
        assertTrue(result.meta().clientContextId().isPresent());
        assertEquals(QueryStatus.SUCCESS, result.meta().status());
        assertFalse(result.meta().warnings().isPresent());
        assertEquals(1, result.allRowsAs(JsonObject.class).size());
        assertTrue(result.meta().signature().isPresent());

        QueryMetrics metrics = result.meta().metrics().get();
        assertEquals(0, metrics.errorCount());
        assertEquals(0, metrics.warningCount());
        assertEquals(1, metrics.resultCount());
    }

    @Test
    void asyncSelect() throws Exception {
        String id = insertDoc();

        QueryOptions options = queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        CompletableFuture<QueryResult> result = cluster.async().query(
          "select * from " + bucketName + " where meta().id=\"" + id + "\"",
          options
        );
        List<JsonObject> rows = result.get().allRowsAs(JsonObject.class);
        assertEquals(1, rows.size());
    }

    @Test
    void reactiveSelect() {
        String id = insertDoc();

        QueryOptions options = queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
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

        QueryOptions options = queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        QueryResult result = cluster.query(
          "select * from " + bucketName + " where meta().id=\"" + id + "\"",
          options
        );
        assertFalse(result.meta().profileInfo().isPresent());
    }

    @Test
    void getProfileWhenRequested() {
        String id = insertDoc();

        QueryOptions options = queryOptions().withProfile(QueryProfile.TIMINGS);
        QueryResult result = cluster.query(
          "select * from " + bucketName + " where meta().id=\"" + id +"\"",
          options
        );
        JsonObject profile = result.meta().profileInfo().get();
        assertTrue(profile.size() > 0);
    }

    @Test
    void failOnSyntaxError() {
        assertThrows(QueryServiceException.class, () -> {
            QueryResult result = cluster.query("invalid export");
        });
    }

    @Test
    void blockingNamedParameterizedSelectQuery() {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .withScanConsistency(ScanConsistency.REQUEST_PLUS)
          .withParameters(JsonObject.create().put("id", id));
        QueryResult result = cluster.query(
          "select " + bucketName + ".* from " + bucketName + " where meta().id=$id",
          options
        );
        List<JsonObject> rows = result.allRowsAs(JsonObject.class);
        assertEquals(1, rows.size());
        assertEquals(FOO_CONTENT, rows.get(0));
    }

    @Test
    void asyncNamedParameterizedSelectQuery() throws Exception {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .withScanConsistency(ScanConsistency.REQUEST_PLUS)
          .withParameters(JsonObject.create().put("id", id));
        CompletableFuture<QueryResult> result = cluster.async().query(
          "select * from " + bucketName + " where meta().id=$id",
          options
        );
        List<JsonObject> rows = result.get().allRowsAs(JsonObject.class);
        assertEquals(1, rows.size());
    }

    @Test
    void reactiveNamedParameterizedSelectQuery() {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .withScanConsistency(ScanConsistency.REQUEST_PLUS)
          .withParameters(JsonObject.create().put("id", id));
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
          .withScanConsistency(ScanConsistency.REQUEST_PLUS)
          .withParameters(JsonArray.from(id));
        QueryResult result = cluster.query(
          "select  " + bucketName + ".* from " + bucketName + " where meta().id=$1",
          options
        );
        List<JsonObject> rows = result.allRowsAs(JsonObject.class);
        assertEquals(1, rows.size());
        assertEquals(FOO_CONTENT, rows.get(0));
    }

    @Test
    void asyncPositionalParameterizedSelectQuery() throws Exception {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .withScanConsistency(ScanConsistency.REQUEST_PLUS)
          .withParameters(JsonArray.from(id));
        CompletableFuture<QueryResult> result = cluster.async().query(
          "select * from " + bucketName+ " where meta().id=$1",
          options
        );
        List<JsonObject> rows = result.get().allRowsAs(JsonObject.class);
        assertEquals(1, rows.size());
    }

    @Test
    void reactivePositionalParameterizedSelectQuery() {
        String id = insertDoc();

        QueryOptions options = queryOptions()
          .withScanConsistency(ScanConsistency.REQUEST_PLUS)
          .withParameters(JsonArray.from(id));
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
