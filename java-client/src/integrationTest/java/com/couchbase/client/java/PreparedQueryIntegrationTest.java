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

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.query.options.ScanConsistency;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * Integration tests for testing prepared query
 */
class PreparedQueryIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static ClusterEnvironment environment;
    private static Collection collection;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();
        // TODO support running this only when not mocked
//        cluster.query("create primary index on `" + config().bucketname() + "`");
    }

    @AfterAll
    static void tearDown() {
        environment.shutdown();
        cluster.shutdown();
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testSimplePreparedSelect() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testSimplePreparedSelect", content);
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).prepared(true);
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "` " +
                "where meta().id=\"testSimplePreparedSelect\"", options);
        List<JsonObject> rows = result.rows();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testSimplePreparedNamedParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testSimplePreparedNamedParameterizedSelectQuery", content);
        JsonObject parameters = JsonObject.create().put("id", "testSimplePreparedNamedParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS)
                .withParameters(parameters).prepared(true);;
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "`" +
                " where meta().id=$id", options);
        List<JsonObject> rows = result.rows();
        assertEquals(1, rows.size());

    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testSimplePreparedPositionalParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testSimplePreparedPositionalParameterizedSelectQuery", content);
        JsonArray parameters = JsonArray.create().add("testSimplePreparedPositionalParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS)
                .withParameters(parameters).prepared(true);
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "` " +
                "where meta().id=$1", options);
        List<JsonObject> rows = result.rows();
        assertEquals(1, rows.size());

    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testAsyncPreparedSelect() throws Exception {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testAsyncPreparedSelect", content);
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS)
                .prepared(true);
        CompletableFuture<AsyncQueryResult> result = cluster.async().query("select * from `" + config().bucketname() + "` " +
                "where meta().id=\"testAsyncPreparedSelect\"", options);
        List<JsonObject> rows = result.get().rows().get();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testAsyncPreparedNamedParameterizedSelectQuery() throws Exception {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testAsyncPreparedNamedParameterizedSelectQuery", content);
        JsonObject parameters = JsonObject.create().put("id", "testAsyncPreparedNamedParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS)
                .withParameters(parameters).prepared(true);
        CompletableFuture<AsyncQueryResult> result = cluster.async().query("select * from `" + config().bucketname() + "`" +
                " where meta().id=$id", options);
        List<JsonObject> rows = result.get().rows().get();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testAsyncPreparedPositionalParameterizedSelectQuery() throws Exception {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testAsyncPreparedPositionalParameterizedSelectQuery", content);
        JsonArray parameters = JsonArray.create().add("testAsyncPreparedPositionalParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS)
                .withParameters(parameters).prepared(true);
        CompletableFuture<AsyncQueryResult> result = cluster.async().query("select * from `" + config().bucketname() + "`" +
                " where meta().id=$1", options);
        List<JsonObject> rows = result.get().rows().get();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testReactivePreparedSelect() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testReactivePreparedSelect", content);
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).prepared(true);
        Mono<ReactiveQueryResult> result = cluster.reactive().query("select * from `" + config().bucketname() + "` " +
                "where meta().id=\"testReactivePreparedSelect\"", options);
        List<JsonObject> rows = result.flux().flatMap(ReactiveQueryResult::rows).collectList().block();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testReactivePreparedNamedParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testReactivePreparedNamedParameterizedSelectQuery", content);
        JsonObject parameters = JsonObject.create().put("id", "testReactivePreparedNamedParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS)
                .withParameters(parameters).prepared(true);
        Mono<ReactiveQueryResult> result = cluster.reactive().query("select * from `" + config().bucketname() + "` " +
                "where meta().id=$id", options);
        List<JsonObject> rows = result.flux().flatMap(ReactiveQueryResult::rows).collectList().block();
        assertEquals(1, rows.size());

    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testReactivePreparedPositionalParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testReactivePreparedPositionalParameterizedSelectQuery", content);
        JsonArray parameters = JsonArray.create().add("testReactivePreparedPositionalParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS)
                .withParameters(parameters).prepared(true);
        Mono<ReactiveQueryResult> result =  cluster.reactive().query("select * from `" + config().bucketname() + "` " +
                "where meta().id=$1", options);
        List<JsonObject> rows = result.flux().flatMap(ReactiveQueryResult::rows).collectList().block();
        assertEquals(1, rows.size());
    }
}
