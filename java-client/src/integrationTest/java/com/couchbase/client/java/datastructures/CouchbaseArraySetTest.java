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
import com.couchbase.client.java.datastructures.CouchbaseArraySet;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CouchbaseArraySetTest extends JavaIntegrationTest {
    private static Cluster cluster;
    private static ClusterEnvironment environment;
    private static Collection collection ;

    private String uuid;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        collection = cluster.bucket(config().bucketname()).defaultCollection();
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
    void shouldConstructEmptySet() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        assertEquals(0, set.size());
        set.add(5);
        set.add(10);
        assertEquals(2, set.size());
    }
    @Test
    void shouldBuildSetFromAddAll() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(5, set.size());
    }
     @Test
    void shouldConstructFromExistingSet() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertEquals(5, set.size());
        CouchbaseArraySet<Integer> sameSet = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        assertEquals(5, set.size());
    }
    @Test
    void shouldIsEmptyLikeYouExpect() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        assertEquals(0, set.size());
        assertTrue(set.isEmpty());
        set.add(3);
        assertEquals(1, set.size());
        assertFalse(set.isEmpty());
    }
    @Test
    void shouldContainsLikeYouExpect() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertTrue(set.contains(3));
        assertFalse(set.contains(6));
        assertFalse(set.contains("foo"));
    }
    @Test
    void canAddObjectsThatAreClose()  {
        CouchbaseArraySet<Object> set = new CouchbaseArraySet<>(uuid, collection, Object.class);
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
        CouchbaseArraySet<Object> set = new CouchbaseArraySet<>(uuid, collection, Object.class);
        set.add("1");
        set.add(1);
        set.remove(1);
        assertFalse(set.contains(1));
        assertTrue(set.contains("1"));
    }
    @Test
    void shouldClear() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        set.addAll(Arrays.asList(1,2,3,4,5));
        assertFalse(set.isEmpty());
        set.clear();
        assertTrue(set.isEmpty());
    }
    @Test
    void shouldIterate() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        HashSet<Integer> javaSet = new HashSet<>(Arrays.asList(1,2,3,4,5));
        Iterator<Integer> it = set.iterator();
        while(it.hasNext()) {
            assertTrue(javaSet.contains(it.next()));
        }
    }
    @Test
    void shouldRemoveViaIterator() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        HashSet<Integer> javaSet = new HashSet<>(Arrays.asList(1,2,3,4,5));
        set.addAll(javaSet);

        Iterator<Integer> it = set.iterator();
        Integer valToRemove = it.next();
        // this _should_ remove the item from the set.
        it.remove();
        assertEquals(4, set.size());
        // we last visited 1, so...
        assertFalse(set.contains(valToRemove));
    }
    @Test
    void shouldNotRemoveViaIteratorIfListChanged() {
        CouchbaseArraySet<Integer> set = new CouchbaseArraySet<>(uuid, collection, Integer.class);
        HashSet<Integer> javaSet = new HashSet<>(Arrays.asList(1,2,3,4,5));
        set.addAll(javaSet);

        Iterator<Integer> it = set.iterator();
        it.next();
        set.remove(Integer.valueOf(3));
        assertEquals(4, set.size());
        assertThrows(ConcurrentModificationException.class, () -> {it.remove();});
    }
    @Test
    void shouldNotWorkWithNonJsonValuePrimitives() {
        CouchbaseArraySet<JsonObject> set = new CouchbaseArraySet<>(uuid, collection, JsonObject.class);
        JsonObject obj1 = JsonObject.fromJson("{\"string\":\"foo\", \"integer\":1}");
        assertThrows(ClassCastException.class, () -> {
            set.add(obj1);
        });
    }
    @Test
    void shouldCreateOffCouchbaseCollection() {
        CouchbaseArraySet<Integer> set = collection.set(uuid, Integer.class);
        assertTrue(set.isEmpty());
        set.add(1);
        assertFalse(set.isEmpty());

        CouchbaseArraySet<Integer> set2 = collection.set(uuid, Integer.class);
        assertFalse(set.isEmpty());
    }
}
