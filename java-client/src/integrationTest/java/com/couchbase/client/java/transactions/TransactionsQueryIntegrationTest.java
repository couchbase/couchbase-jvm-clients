/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Transactions are heavily tested by FIT, these are some basic sanity tests for transactions involving query.
 */
@IgnoreWhen(clusterTypes = {ClusterType.MOCKED},
        // Using COLLECTIONS as a proxy for 7.0+, which is when query added support for transactions
        missesCapabilities = {Capabilities.CREATE_AS_DELETED, Capabilities.COLLECTIONS, Capabilities.QUERY})
public class TransactionsQueryIntegrationTest extends JavaIntegrationTest {

    static private Cluster cluster;
    static private Collection collection;

    @BeforeAll
    static void beforeAll() {
        cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    }

    @AfterAll
    static void afterAll() {
        cluster.disconnect();
    }

    @Test
    void basic() {
        cluster.transactions().run((ctx) -> {
            ctx.query("SELECT 'Hello World' AS Greeting");
        });
    }

    @Test
    void reactive() {
        cluster.reactive().transactions().run((ctx) ->
                ctx.query("SELECT 'Hello World' AS Greeting"))
                .block();
    }

    @Test
    void kvGet() {
        String docId = UUID.randomUUID().toString();
        collection.upsert(docId, JsonObject.create());

        cluster.transactions().run((ctx) -> {
            ctx.query("SELECT 'Hello World' AS Greeting");
            TransactionGetResult gr = ctx.get(collection, docId);
        });
    }

    @Test
    void insert() {
        String docId = UUID.randomUUID().toString();

        cluster.transactions().run((ctx) -> {
            TransactionQueryResult qr = ctx.query("INSERT INTO `" + config().bucketname() + "` VALUES ($1, $2)",
                    TransactionQueryOptions.queryOptions().parameters(JsonArray.from(docId, JsonObject.create())));
            assertEquals(1, qr.metaData().metrics().get().mutationCount());
        });
    }

    @Test
    void kvInsert() {
        String docId = UUID.randomUUID().toString();

        cluster.transactions().run((ctx) -> {
            ctx.query("SELECT 'Hello World' AS Greeting");
            ctx.insert(collection, docId, JsonObject.create());
        });
    }
}
