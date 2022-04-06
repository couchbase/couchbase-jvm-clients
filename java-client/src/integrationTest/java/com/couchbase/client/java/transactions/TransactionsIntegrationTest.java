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

import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.java.transactions.config.TransactionsQueryConfig;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Transactions are heavily tested by FIT, these are some basic sanity tests for KV-only transactions.
 */
@IgnoreWhen(clusterTypes = {ClusterType.MOCKED},
        missesCapabilities = {Capabilities.CREATE_AS_DELETED})
public class TransactionsIntegrationTest extends JavaIntegrationTest {

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

    /**
     * Just demo how transactions config looks.
     */
    @Test
    void testConfig() {
        ClusterEnvironment env = ClusterEnvironment.builder()
                .ioConfig(IoConfig.enableMutationTokens(true)
                        .analyticsCircuitBreakerConfig(CircuitBreakerConfig.enabled(true)))
                .transactionsConfig(TransactionsConfig.durabilityLevel(DurabilityLevel.NONE)
                        .metadataCollection(TransactionKeyspace.create("bkt", "scp", "coll"))
                        .cleanupConfig(TransactionsCleanupConfig.cleanupClientAttempts(false)
                                .cleanupLostAttempts(false)
                                .cleanupWindow(Duration.ofSeconds(10))
                                .addCollection(TransactionKeyspace.create("bkt", "scp", "coll")))
                        .queryConfig(TransactionsQueryConfig.scanConsistency(QueryScanConsistency.REQUEST_PLUS)))
                .build();

        env.shutdown();


    }

    @Test
    void insert() {
        String docId = UUID.randomUUID().toString();
        JsonObject content = JsonObject.create().put("foo", "bar");

        cluster.transactions().run((ctx) -> {
            ctx.insert(collection, docId, content);
        });

        assertEquals(content, collection.get(docId).contentAsObject());
    }

    @Test
    void insertReactive() {
        String docId = UUID.randomUUID().toString();
        JsonObject content = JsonObject.create().put("foo", "bar");

        cluster.reactive().transactions().run((ctx) ->
                        ctx.insert(collection.reactive(), docId, content))
                .block();

        assertEquals(content, collection.get(docId).contentAsObject());
    }

    @Test
    void rollbackInsert() {
        String docId = UUID.randomUUID().toString();
        JsonObject content = JsonObject.create().put("foo", "bar");

        try {
            cluster.transactions().run((ctx) -> {
                ctx.insert(collection, docId, content);
                throw new RuntimeException();
            });
            Assertions.fail();
        } catch (TransactionFailedException err) {
        }

        try {
            collection.get(docId);
            Assertions.fail();
        } catch (DocumentNotFoundException ignored) {
        }
    }

    @Test
    void replace() {
        String docId = UUID.randomUUID().toString();
        JsonObject initial = JsonObject.create().put("foo", "bar");
        JsonObject updated = JsonObject.create().put("foo", "baz");
        collection.insert(docId, initial);

        TransactionResult tr = cluster.transactions().run((ctx) -> {
            TransactionGetResult doc = ctx.get(collection, docId);
            ctx.replace(doc, updated);
        });

        assertEquals(updated, collection.get(docId).contentAsObject());
    }

    @Test
    void replaceReactive() {
        String docId = UUID.randomUUID().toString();
        JsonObject initial = JsonObject.create().put("foo", "bar");
        JsonObject updated = JsonObject.create().put("foo", "baz");
        collection.insert(docId, initial);

        cluster.reactive().transactions().run((ctx) ->
                        ctx.get(collection.reactive(), docId)
                                .flatMap(doc -> ctx.replace(doc, updated)))
                .block();

        assertEquals(updated, collection.get(docId).contentAsObject());
    }

    @Test
    void remove() {
        String docId = UUID.randomUUID().toString();
        JsonObject initial = JsonObject.create().put("foo", "bar");
        collection.insert(docId, initial);

        cluster.transactions().run((ctx) -> {
            TransactionGetResult doc = ctx.get(collection, docId);
            ctx.remove(doc);
        });

        try {
            collection.get(docId);
            Assertions.fail();
        } catch (DocumentNotFoundException ignored) {
        }
    }

    @Test
    void removeReactive() {
        String docId = UUID.randomUUID().toString();
        JsonObject initial = JsonObject.create().put("foo", "bar");
        JsonObject updated = JsonObject.create().put("foo", "baz");
        collection.insert(docId, initial);

        cluster.reactive().transactions().run((ctx) ->
                        ctx.get(collection.reactive(), docId)
                                .flatMap(doc -> ctx.remove(doc)))
                .block();

        try {
            collection.get(docId);
            Assertions.fail();
        } catch (DocumentNotFoundException ignored) {
        }
    }
}