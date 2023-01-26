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
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ArraySetOptions;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class CouchbaseArraySetTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection collection;
    private static ArraySetOptions options;

    private String uuid;

    @BeforeAll
    static void setup() {
        cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = cluster.bucket(config().bucketname()).defaultCollection();
        options = ArraySetOptions.arraySetOptions();
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
            // that's ok, we lazy create.
        }
    }

    @Test
    void shouldNotCreateEmptyDoc() {
        collection.set(uuid, Integer.class);
        assertThrows(DocumentNotFoundException.class, () -> collection.get(uuid));
    }

    @Test
    void shouldConstructEmptySet() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        assertEquals(0, set.size());
        set.add(5);
        set.add(10);
        assertEquals(2, set.size());
    }
    @Test
    void shouldBuildSetFromAddAll() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(5, set.size());
    }
     @Test
    void shouldConstructFromExistingSet() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(5, set.size());
        CouchbaseArraySet<Integer> sameSet = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        assertEquals(5, set.size());
    }
    @Test
    void shouldIsEmptyLikeYouExpect() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
        set.add(3);
        assertEquals(1, set.size());
        assertFalse(set.isEmpty());
    }
    @Test
    void shouldContainsLikeYouExpect() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertTrue(set.contains(3));
        assertFalse(set.contains(6));
    }

    @Test
    void canAddObjectsThatAreClose()  {
        CouchbaseArraySet<Object> set = new CouchbaseArraySet<>(uuid, collection, Object.class, options);
        set.add("1");
        set.add(1);
        set.add("foo");
        assertEquals(3, set.size());
        assertTrue(set.contains("1"));
        assertTrue(set.contains(1));
        assertTrue(set.contains("foo"));
    }
    @Test
    void canRemoveObjectsThatAreClose() {
        CouchbaseArraySet<Object> set = new CouchbaseArraySet<>(uuid, collection, Object.class, options);
        set.add("1");
        set.add(1);
        set.remove(1);
        assertFalse(set.contains(1));
        assertTrue(set.contains("1"));
    }
    @Test
    void shouldClear() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertFalse(set.isEmpty());
        set.clear();
        assertTrue(set.isEmpty());
        assertThrows(DocumentNotFoundException.class, () -> collection.get(uuid));
    }
    @Test
    void shouldIterate() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        HashSet<Integer> javaSet = new HashSet<>(Arrays.asList(1,2,3,4,5));
        for (Integer integer : set) {
            assertTrue(javaSet.contains(integer));
        }
    }
    @Test
    void shouldRemoveViaIterator() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        HashSet<Integer> javaSet = new HashSet<>(Arrays.asList(1,2,3,4,5));
        set.addAll(javaSet);

        Iterator<Integer> it = set.iterator();
        Integer valToRemove = it.next();
        // this _should_ remove the item from the set.
        it.remove();
        assertEquals(4, set.size());

        // gotta do another next() before another remove()
        assertThrows(IllegalStateException.class, it::remove);

        // we last visited 1, so...
        assertFalse(set.contains(valToRemove));
    }
    @Test
    void shouldThrowConcurrentModificationException() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        HashSet<Integer> javaSet = new HashSet<>(Arrays.asList(1,2,3,4,5));
        set.addAll(javaSet);
        Iterator<Integer> it = set.iterator();
        it.next();
        set.clear();
        assertThrows(ConcurrentModificationException.class, it::remove);
    }

    @Test
    void shouldNotRemoveViaIteratorIfListChanged() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class, options);
        HashSet<Integer> javaSet = new HashSet<>(Arrays.asList(1,2,3,4,5));
        set.addAll(javaSet);

        Iterator<Integer> it = set.iterator();
        it.next();
        set.remove(3);
        assertEquals(4, set.size());
        assertThrows(ConcurrentModificationException.class, it::remove);
    }
    @Test
    void shouldNotWorkWithNonJsonValuePrimitives() {
        CouchbaseArraySet<JsonObject> set = new CouchbaseArraySet<>(uuid, collection, JsonObject.class, options);
        JsonObject obj1 = JsonObject.fromJson("{\"string\":\"foo\", \"integer\":1}");
        assertThrows(ClassCastException.class, () -> {
            set.add(obj1);
        });
    }
    @Test
    void shouldCreateOffCouchbaseCollection() {
        Set<Integer> set = collection.set(uuid, Integer.class, options);
        assertTrue(set.isEmpty());
        set.add(1);
        assertFalse(set.isEmpty());

        Set<Integer> set2 = collection.set(uuid, Integer.class, options);
        assertFalse(set2.isEmpty());
    }
}
