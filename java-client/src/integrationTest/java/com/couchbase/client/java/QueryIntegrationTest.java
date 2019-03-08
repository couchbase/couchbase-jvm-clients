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

import com.couchbase.client.core.error.QueryServiceException;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.query.options.QueryProfile;
import com.couchbase.client.java.query.options.ScanConsistency;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * Integration tests for testing query
 */
class QueryIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static ClusterEnvironment environment;
    private static Collection collection;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();
        cluster.query("create primary index on `" + config().bucketname() + "`");
    }

    @AfterAll
    static void tearDown() {
        environment.shutdown();
        cluster.shutdown();
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testSimpleSelect() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testSimpleSelect", content);
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "` where meta().id=\"testSimpleSelect\"", options);
        List<JsonObject> rows = result.rows();
        assertEquals(1, rows.size());
        assertNotNull(result.requestId());
        assertFalse(result.clientContextId().isPresent());
        JsonObject signature = result.signature();
        assertTrue(signature.size() > 0);
        assertEquals("success", result.queryStatus());
        assertEquals(0, result.metrics().errorCount());
        assertEquals(0, result.metrics().warningCount());
        assertEquals(1, result.metrics().resultCount());
        assertEquals(0, result.warnings().size());
    }

    // TODO this should return IllegalStateException
    @Ignore
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testNoProfileRequestedReturnsNoProfile() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testNoProfileRequestedReturnsNoProfile", content);
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "` where meta().id=\"testNoProfileRequestedReturnsNoProfile\"", options);
        assertThrows(IllegalStateException.class, result::profileInfo);
    }


    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testGettingProfile() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testGettingProfile", content);
        QueryOptions options = QueryOptions.queryOptions().withProfile(QueryProfile.TIMINGS);
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "` where meta().id=\"testGettingProfile\"", options);
        JsonObject profile = result.profileInfo();
        assertTrue(profile.size() > 0);
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testSyntaxError() {
        QueryResult result = cluster.query("invalid n1ql");
        assertThrows(QueryServiceException.class, () -> {
            result.rows();
        });
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testSimpleNamedParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testSimpleParameterizedSelectQuery", content);
        JsonObject parameters = JsonObject.create().put("id", "testSimpleParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).withParameters(parameters);
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "` where meta().id=$id", options);
        List<JsonObject> rows = result.rows();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testSimplePositionalParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testSimplePositionalParameterizedSelectQuery", content);
        JsonArray parameters = JsonArray.create().add("testSimplePositionalParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).withParameters(parameters);
        QueryResult result = cluster.query("select * from `" + config().bucketname() + "` where meta().id=$1", options);
        List<JsonObject> rows = result.rows();
        assertEquals(1, rows.size());

    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testAsyncSelect() throws Exception {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testAsyncSelect", content);
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        CompletableFuture<AsyncQueryResult> result = cluster.async().query("select * from `" + config().bucketname() + "` where meta().id=\"testAsyncSelect\"", options);
        List<JsonObject> rows = result.get().rows().get();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testAsyncNamedParameterizedSelectQuery() throws Exception {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testAsyncNamedParameterizedSelectQuery", content);
        JsonObject parameters = JsonObject.create().put("id", "testAsyncNamedParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).withParameters(parameters);
        CompletableFuture<AsyncQueryResult> result = cluster.async().query("select * from `" + config().bucketname() + "` where meta().id=$id", options);
        List<JsonObject> rows = result.get().rows().get();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testAsyncPositionalParameterizedSelectQuery() throws Exception {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testAsyncPositionalParameterizedSelectQuery", content);
        JsonArray parameters = JsonArray.create().add("testSimplePositionalParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).withParameters(parameters);
        CompletableFuture<AsyncQueryResult> result = cluster.async().query("select * from `" + config().bucketname() + "` where meta().id=$1", options);
        List<JsonObject> rows = result.get().rows().get();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testReactiveSelect() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testReactiveSelect", content);
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS);
        Mono<ReactiveQueryResult> result = cluster.reactive().query("select * from `" + config().bucketname() + "` where meta().id=\"testReactiveSelect\"", options);
        List<JsonObject> rows = result.flux().flatMap(ReactiveQueryResult::rows).collectList().block();
        assertEquals(1, rows.size());
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testReactiveNamedParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testReactiveNamedParameterizedSelectQuery", content);
        JsonObject parameters = JsonObject.create().put("id", "testReactiveNamedParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).withParameters(parameters);
        Mono<ReactiveQueryResult> result = cluster.reactive().query("select * from `" + config().bucketname() + "` where meta().id=$id", options);
        List<JsonObject> rows = result.flux().flatMap(ReactiveQueryResult::rows).collectList().block();
        assertEquals(1, rows.size());

    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    void testReactivePositionalParameterizedSelectQuery() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        collection.insert("testReactivePositionalParameterizedSelectQuery", content);
        JsonArray parameters = JsonArray.create().add("testReactivePositionalParameterizedSelectQuery");
        QueryOptions options = QueryOptions.queryOptions().withScanConsistency(ScanConsistency.REQUEST_PLUS).withParameters(parameters);
        Mono<ReactiveQueryResult> result =  cluster.reactive().query("select * from `" + config().bucketname() + "` where meta().id=$1", options);
        List<JsonObject> rows = result.flux().flatMap(ReactiveQueryResult::rows).collectList().block();
        assertEquals(1, rows.size());
    }
}
