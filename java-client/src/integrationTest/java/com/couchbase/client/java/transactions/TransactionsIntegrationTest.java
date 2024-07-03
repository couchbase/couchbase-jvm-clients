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

import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.transaction.TransactionsStartedEvent;
import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig.TRANSACTIONS_CLEANUP_LOST_PROPERTY;
import static com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig.TRANSACTIONS_CLEANUP_REGULAR_PROPERTY;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Transactions are heavily tested by FIT, these are some basic sanity tests for KV-only transactions.
 */
@IgnoreWhen(clusterTypes = {ClusterType.MOCKED},
  missesCapabilities = {Capabilities.CREATE_AS_DELETED},
  isProtostellarWillWorkLater = true
)
public class TransactionsIntegrationTest extends JavaIntegrationTest {

    static private Cluster cluster;
    static private Collection collection;

    @BeforeAll
    static void beforeAll() {

        cluster = createCluster(env -> env.thresholdLoggingTracerConfig(tracer -> tracer.emitInterval(Duration.ofSeconds(1))));
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
        try (ClusterEnvironment env = ClusterEnvironment.builder()
                .ioConfig(io -> io
                        .enableMutationTokens(true)
                        .analyticsCircuitBreakerConfig(breaker -> breaker.enabled(true))
                )
                .transactionsConfig(txn -> txn
                        .durabilityLevel(DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)
                        .metadataCollection(TransactionKeyspace.create("bkt", "scp", "coll"))
                        .cleanupConfig(cleanup -> cleanup
                                .cleanupClientAttempts(false)
                                .cleanupLostAttempts(false)
                                .cleanupWindow(Duration.ofSeconds(10))
                                .addCollection(TransactionKeyspace.create("bkt", "scp", "coll")))
                        .queryConfig(query -> query.scanConsistency(QueryScanConsistency.REQUEST_PLUS)))
                .build())  {

            assertEquals(
                Optional.of("REQUEST_PLUS"),
                env.transactionsConfig().scanConsistency()
            );

            assertEquals(
                Duration.ofSeconds(10),
                env.transactionsConfig().cleanupConfig().cleanupWindow()
            );

            assertEquals(
                DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                env.transactionsConfig().durabilityLevel()
            );

            assertEquals(
                setOf(new CollectionIdentifier("bkt", Optional.of("scp"), Optional.of("coll"))),
                env.transactionsConfig().cleanupConfig().cleanupSet()
            );
        }
    }

    @Test
    void canSetTransactionConfigOptionsViaConnectionString() {
        try (ClusterEnvironment env = ClusterEnvironment.builder()
              .load(new ConnectionStringPropertyLoader(
                  "couchbases://127.0.0.1?" +
                      String.join("&",
                        "transactions.query.scanConsistency=REQUEST_PLUS",
                        "transactions.cleanup.cleanupWindow=10s",
                        "transactions.durabilityLevel=MAJORITY_AND_PERSIST_TO_ACTIVE"
                      )
              ))
              .build()) {

            assertEquals(
              Optional.of("REQUEST_PLUS"),
              env.transactionsConfig().scanConsistency()
            );

            assertEquals(
              Duration.ofSeconds(10),
              env.transactionsConfig().cleanupConfig().cleanupWindow()
            );

            assertEquals(
              DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
              env.transactionsConfig().durabilityLevel()
            );
        }
    }

    @Test
    void insert() throws InterruptedException {
        String docId = UUID.randomUUID().toString();
        JsonObject content = JsonObject.create().put("foo", "bar");

        cluster.transactions().run((ctx) -> {
            ctx.insert(collection, docId, content);
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Thread.sleep(2000);

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

        assertThrows(TransactionFailedException.class, () ->
            cluster.transactions().run((ctx) -> {
                ctx.insert(collection, docId, content);
                throw new RuntimeException();
            })
        );

        assertThrows(DocumentNotFoundException.class, () -> collection.get(docId));
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

        assertThrows(DocumentNotFoundException.class, () -> collection.get(docId));
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

        assertThrows(DocumentNotFoundException.class, () -> collection.get(docId));
    }

    @Test
    void cleanupDisabled() {
        try {
            System.setProperty(TRANSACTIONS_CLEANUP_LOST_PROPERTY, "false");
            System.setProperty(TRANSACTIONS_CLEANUP_REGULAR_PROPERTY, "false");

            SimpleEventBus eb = new SimpleEventBus(false);

            Cluster cluster1 = createCluster(env -> env.eventBus(eb));
            cluster1.waitUntilReady(Duration.ofSeconds(30));

            assertFalse(cluster1.environment().transactionsConfig().cleanupConfig().runRegularAttemptsCleanupThread());
            assertFalse(cluster1.environment().transactionsConfig().cleanupConfig().runLostAttemptsCleanupThread());

            Util.waitUntilCondition(() -> {
                AtomicBoolean received = new AtomicBoolean(false);

                eb.publishedEvents().forEach(event -> {
                    if (event instanceof TransactionsStartedEvent) {
                        received.set(true);
                        TransactionsStartedEvent ev = (TransactionsStartedEvent) event;
                        assertFalse(ev.runningLostAttemptsCleanupThread());
                        assertFalse(ev.runningRegularAttemptsCleanupThread());
                    }
                });

                return received.get();
            });
        }
        finally {
            System.clearProperty(TRANSACTIONS_CLEANUP_LOST_PROPERTY);
            System.clearProperty(TRANSACTIONS_CLEANUP_REGULAR_PROPERTY);
        }
    }
}
