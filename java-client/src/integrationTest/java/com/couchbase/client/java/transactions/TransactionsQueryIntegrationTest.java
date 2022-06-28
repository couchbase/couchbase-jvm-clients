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

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionsQueryIntegrationTest.class);

    static private Cluster cluster;
    static private Collection collection;

    @BeforeAll
    static void beforeAll() {
        // On CI seeing transactions expire as a basic "SELECT 'Hello World' AS Greeting" query is taking 20 seconds+.
        // Use a long timeout and try to ensure query is warmed up first.
        cluster = createCluster(env -> env.transactionsConfig(TransactionsConfig.timeout(Duration.ofMinutes(1))));
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
        waitForService(bucket, ServiceType.QUERY);
        cluster.query("SELECT 'Hello World' AS Greeting", QueryOptions.queryOptions().timeout(Duration.ofMinutes(1)));
    }

    @AfterAll
    static void afterAll() {
        cluster.disconnect();
    }

    private static void logFailed(Runnable runnable) {
        try {
            runnable.run();
        }
        catch (TransactionFailedException err) {
            err.logs().forEach(log -> LOGGER.info(log.toString()));
            throw err;
        }
    }

    @Test
    void basic() {
        logFailed(() -> {
            cluster.transactions().run((ctx) -> {
                ctx.query("SELECT 'Hello World' AS Greeting");
            });
        });
    }

    @Test
    void reactive() {
        logFailed(() -> {
            cluster.reactive().transactions().run((ctx) ->
                            ctx.query("SELECT 'Hello World' AS Greeting"))
                    .block();
        });
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
