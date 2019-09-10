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
package com.couchbase.client.java.datastructures;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.collections.support.TestObject;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.QueueOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

class CouchbaseQueueTest extends JavaIntegrationTest {
    private static Cluster cluster;
    private static ClusterEnvironment environment;
    private static Collection collection;
    private static QueueOptions options;

    private String uuid;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        collection = cluster.bucket(config().bucketname()).defaultCollection();
        options = QueueOptions.queueOptions();
    }

    @AfterAll
    static void tearDown() {
        cluster.shutdown();
        environment.shutdown();
    }

    @BeforeEach
    void before() {
        uuid = UUID.randomUUID().toString();
    }

    @AfterEach
    void after() {
        collection.remove(uuid);
    }

    @Test
    void canCreateSimpleFifoQueue() {
        CouchbaseQueue<Integer> queue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        queue.offer(5);
        assertEquals(1, queue.size());
        assertEquals(5, queue.poll().intValue());
        assertEquals(0, queue.size());
    }
    @Test
    void canCreateFromExistingQueue()  {
        CouchbaseQueue<Integer> queue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        queue.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(5, queue.size());
        CouchbaseQueue<Integer> sameQueue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        assertEquals(5, sameQueue.size());
    }

    @Test
    void canPeek() {
        CouchbaseQueue<Integer> queue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        queue.addAll(Arrays.asList(1,2,3,4,5));
        // They get pushed in order, so...'
        assertEquals(1, queue.peek().intValue());
        // should not have been consumed, so...
        assertEquals(5, queue.size());
    }
    @Test
    void canPoll() {
        CouchbaseQueue<Integer> queue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        queue.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(5, queue.size());
        // end should be first to come out
        assertEquals(1,queue.poll().intValue());
        // should have been consume
        assertEquals(4, queue.size());
    }
    @Test
    void canOffer() {
        CouchbaseQueue<Integer> queue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        assertEquals(1, queue.peek().intValue());
    }
    @Test
    void canUseJsonObjects() {
        CouchbaseQueue<JsonObject> queue = new CouchbaseQueue<>(uuid, collection, JsonObject.class, options);
        JsonObject obj1 = JsonObject.fromJson("{\"string\":\"foo\", \"integer\":1}");
        JsonObject obj2 = JsonObject.fromJson("{\"string\":\"bar\", \"integer\":2}");
        queue.offer(obj1);
        queue.offer(obj2);
        assertEquals(2, queue.size());
        assertEquals(obj1, queue.poll());
        assertEquals(obj2, queue.poll());
    }
    @Test
    void canUseObjects() {
        CouchbaseQueue<TestObject> queue = new CouchbaseQueue<>(uuid, collection, TestObject.class, options);
        TestObject obj1 = new TestObject(1, "foo");
        TestObject obj2 = new TestObject(2, "bar");
        queue.offer(obj1);
        queue.offer(obj2);
        assertEquals(2, queue.size());
        assertEquals(obj1, queue.poll());
        assertEquals(obj2, queue.poll());
    }
    @Test
    void canConstructUsingCouchbaseCollection() {
        CouchbaseQueue<Integer> queue = collection.queue(uuid, Integer.class, options);
        assertTrue(queue.isEmpty());
        queue.offer(1);
        assertFalse(queue.isEmpty());
        CouchbaseQueue<Integer> queue2 = collection.queue(uuid, Integer.class, options);
        assertFalse(queue.isEmpty());
        assertFalse(queue2.isEmpty());
    }}

