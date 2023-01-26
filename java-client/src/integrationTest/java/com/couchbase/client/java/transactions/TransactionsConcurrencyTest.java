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
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Testing transactions concurrency, looking for scheduler exhaustion issues and similar.
 */
@IgnoreWhen(clusterTypes = {ClusterType.MOCKED},
  missesCapabilities = {Capabilities.CREATE_AS_DELETED}
)
public class TransactionsConcurrencyTest extends JavaIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionsConcurrencyTest.class);

    static private Cluster cluster;
    static private Collection collection;

    @BeforeAll
    static void beforeAll() {

        cluster = Cluster.connect(seedNodes(), clusterOptions());
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();

        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    }

    @AfterAll
    static void afterAll() {
        cluster.disconnect();
    }

    @DisplayName("Run many concurrent transactions and verify nothing falls over")
    @Test
    void concurrent() {
        AtomicReference<Throwable> err = new AtomicReference<>();

        Runnable r = () -> {
            Thread t = Thread.currentThread();
            try {
                LOGGER.info("Started thread {} {}", t.getId(), t.getName());

                String docId = UUID.randomUUID().toString();
                JsonObject content = JsonObject.create().put("foo", "bar");

                cluster.transactions().run((ctx) -> {
                            Thread ct = Thread.currentThread();
                            LOGGER.info("In lambda on thread {} {}, was on {} {}", ct.getId(), ct.getName(), t.getId(), t.getName());
                            assertTrue(ct.getName().startsWith("cb-txn"));

                            ctx.insert(collection, docId, content);

                            Thread ct2 = Thread.currentThread();
                            LOGGER.info("In lambda after insert on thread {} {}, was on {} {} and {} {}", ct2.getId(), ct2.getName(), ct.getId(), ct.getName(), t.getId(), t.getName());
                            assertTrue(ct2.getName().startsWith("cb-txn"));
                            assertEquals(ct2.getId(), ct.getId());
                        },
                        // Once hotspot is warmed up these transactions take a second or two to complete.  Before then,
                        // it can take close to the default 15 seconds.
                        TransactionOptions.transactionOptions().timeout(Duration.ofSeconds(30)));

                Thread t2 = Thread.currentThread();

                LOGGER.info("After lambda on thread {} {}, was on {} {}", t2.getId(), t2.getName(), t.getId(), t.getName());

                assertEquals(content, collection.get(docId).contentAsObject());
            } catch (Throwable e) {
                LOGGER.warn("Thread {} {} threw {}", t.getId(), t.getName(), e.toString());
                err.set(e);
            }
        };
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            LOGGER.info("Creating thread {}", i);

            Thread t = new Thread(r, "thread-" + i);
            t.start();
            threads.add(t);
        }
        threads.forEach(t -> {
            try {
                LOGGER.info("Waiting for thread {} {}", t.getId(), t.getName());
                t.join();
                LOGGER.info("Finished waiting for thread {} {}", t.getId(), t.getName());
            } catch (InterruptedException e) {
                fail();
            }
        });

        if (err.get() != null) {
            throw new RuntimeException(err.get());
        }
    }
}
