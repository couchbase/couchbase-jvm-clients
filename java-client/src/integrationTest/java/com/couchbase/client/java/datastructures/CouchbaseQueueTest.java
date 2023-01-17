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

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.collections.support.TestObject;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.QueueOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Queue;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class CouchbaseQueueTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection collection;
    private static QueueOptions options;

    private String uuid;

    @BeforeAll
    static void setup() {
        cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();
        options = QueueOptions.queueOptions();
        bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    }

    @AfterAll
    static void tearDown() {
        cluster.disconnect();
    }

    @BeforeEach
    void before() {
        uuid = UUID.randomUUID().toString();
    }

    @AfterEach
    void after() {
        try {
            collection.remove(uuid);
        } catch (DocumentNotFoundException e) {
            // we lazy create, so that's ok
        }
    }

    @Test
    void canCreateEmptyQueue() {
        collection.queue(uuid, Integer.class);
        assertThrows(DocumentNotFoundException.class, () -> collection.get(uuid));
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
    void peekReturnsNullWhenEmpty() {
        CouchbaseQueue<Integer> queue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        assertNull(queue.peek());
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
    void canPollEmptyQueue() {
        Queue<Integer> queue = collection.queue(uuid, Integer.class);
        assertNull(queue.poll());
    }
    @Test
    void canOffer() {
        Queue<Integer> queue = new CouchbaseQueue<>(uuid, collection, Integer.class, options);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        assertEquals(1, queue.peek().intValue());
    }
    @Test
    void canClear() {
        Queue<Integer> queue = collection.queue(uuid, Integer.class);
        queue.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(5, queue.size());
        queue.clear();
        assertEquals(0, queue.size());
        assertThrows(DocumentNotFoundException.class, () -> collection.get(uuid));
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
        Queue<Integer> queue = collection.queue(uuid, Integer.class, options);
        assertTrue(queue.isEmpty());
        queue.offer(1);
        assertFalse(queue.isEmpty());
        Queue<Integer> queue2 = collection.queue(uuid, Integer.class, options);
        assertFalse(queue.isEmpty());
        assertFalse(queue2.isEmpty());
    }
    @Test
    void canRemove() {
        Queue<Integer> queue = collection.queue(uuid, Integer.class);
        queue.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(1, queue.element().intValue());
        queue.remove();
        assertEquals(2, queue.element().intValue());
    }
    @Test
    void mustNextAfterRemoveFromItr() {
        Queue<Integer> queue = collection.queue(uuid, Integer.class);
        queue.addAll(Arrays.asList(1,2,3,4,5));
        Iterator<Integer> it = queue.iterator();
        // throws if you have not started iterating
        assertThrows(IllegalStateException.class, it::remove);
        it.next();
        assertDoesNotThrow(it::remove);
        // throws if you delete 2 times without a next() between 'em
        assertThrows(IllegalStateException.class, it::remove);
        it.next();
        assertDoesNotThrow(it::remove);
    }
    @Test
    void shouldThrowConcurrentModificationExceptionWhenQueueCleared() {
        Queue<Integer> queue1 = collection.queue(uuid, Integer.class);
        queue1.addAll(Arrays.asList(1,2,3,4,5));
        Iterator<Integer> it = queue1.iterator();
        it.next();
        queue1.clear();
        assertThrows(ConcurrentModificationException.class, it::remove);
    }
    @Test
    void shouldThrowConcurrentModificationExceptionWhenIteratorOutOfSync() {
        Queue<Integer> queue1 = collection.queue(uuid, Integer.class);
        Queue<Integer> queue2 = collection.queue(uuid, Integer.class);
        queue1.addAll(Arrays.asList(1,2,3,4,5));
        Iterator<Integer> it1 = queue1.iterator();
        Iterator<Integer> it2 = queue2.iterator();
        it1.next();
        it2.next();
        assertDoesNotThrow(it1::remove);
        assertThrows(ConcurrentModificationException.class, it2::remove);
        assertEquals(4, queue1.size());
        assertFalse(queue1.contains(5));
        assertFalse(queue2.contains(5));
    }
}

