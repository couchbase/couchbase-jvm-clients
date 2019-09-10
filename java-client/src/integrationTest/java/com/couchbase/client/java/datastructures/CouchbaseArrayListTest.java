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
import com.couchbase.client.java.kv.ArrayListOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CouchbaseArrayListTest extends JavaIntegrationTest {
    private static Cluster cluster;
    private static ClusterEnvironment environment;
    private static Collection collection;
    private static ArrayListOptions options;

    private String uuid;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        collection = cluster.bucket(config().bucketname()).defaultCollection();
        options = ArrayListOptions.arrayListOptions();
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
    void simpleRoundTripWithObject() {
        CouchbaseArrayList<TestObject> list = new CouchbaseArrayList<>(uuid, collection, TestObject.class, ArrayListOptions.arrayListOptions());
        TestObject obj = new TestObject(1, "foo");
        list.add(0, obj);
        assertEquals(obj, list.get(0));
    }
    @Test
    void SimpleRoundTripWithJsonObject() {
        CouchbaseArrayList<JsonObject> list = new CouchbaseArrayList<>(uuid, collection, JsonObject.class, ArrayListOptions.arrayListOptions());

        JsonObject thing1 = JsonObject.fromJson("{\"string\":\"foo\", \"integer\":1}");
        JsonObject thing2 = JsonObject.fromJson("{\"string\":\"bar\", \"integer\":2}");
        list.add(0, thing1);
        list.add(0, thing2);
        assertEquals(2, list.size());
        assertEquals(thing2, list.get(0));
        assertEquals(thing1, list.get(1));
    }
    @Test
    void simpleRoundTrip() {
        // put 'em in
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        // now lets find 'em, using a different list instance
        CouchbaseArrayList<Long> sameList = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        assertEquals(sameList.size(), 5);
        assertTrue(sameList.get(0) == 1L);
    }

    @Test
    void sizeWhenEmpty() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        assertEquals(list.size(), 0);
    }

    @Test
    void sizeWhenNotEmpty() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        assertEquals(list.size(), 5);
    }
    @Test
    void testIsEmptyWhenNotEmpty() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.add(1L);
        assertFalse(list.isEmpty());
    }

    @Test
    void testIsEmptyWhenEmpty() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        assertTrue(list.isEmpty());
    }
    @Test
    void shouldAddByIndex() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        assertFalse(list.contains(1L));
        list.add(0, 1L);
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).longValue(), 1L);
        // TODO: 1L OR Long.valueOf(1L) seem to not work here, but
        // Integer (or int below) does.
        assertTrue(list.contains(1));
    }
    @Test
    void shouldPrepend() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        list.add(0, Long.valueOf(6));
        assertEquals(list.get(0).longValue(), 6L);
    }
    @Test
    void shouldAppend() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        list.add(Long.valueOf(6));
        assertEquals(Long.valueOf(6), list.get(5));
    }
    @Test
    void shouldAddByIndexThrowsWhenIndexBeyondEnd() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        assertThrows(IndexOutOfBoundsException.class, () -> list.add(111, 3L));
    }
    @Test
    void shouldAdd() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.add(Long.valueOf(3));
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).longValue(), 3L);
    }
    @Test
    void shouldRemoveByValue() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        assertEquals(list.size() , 5);
        // Given that jackson converts the longs to ints,
        // remove Integer.valueOf does the right thing, but
        // not Long.valueOf.
        list.remove(Integer.valueOf(3));
        assertEquals(list.size(), 4);

        assertFalse(list.contains(3));
    }
    @Test
    void shouldRemoveByIndex() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        assertEquals(list.size(), 5);
        Long oldVal = list.remove(3);
        assertEquals(4L, oldVal.longValue());
        assertEquals(list.size(), 4);
        // index 3 contained the number 4, so...
        assertFalse(list.contains(oldVal));
    }
    @Test
    void shouldThrowOutOfBoundsExceptionRemove() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        assertThrows(IndexOutOfBoundsException.class, () -> list.remove(111));
    }
    @Test
    void shouldReturnIterator() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        Iterator<Long> it = list.iterator();
        int i = 0;
        while(it.hasNext()) {
            Object obj = it.next();
            // TODO: yea... Jackson turns these into Integers by default, so...
            assertTrue(obj instanceof Integer);
            assertEquals(obj, i+1);
            i++;
        }
    }
    @Test
    void shouldClear() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        assertEquals(list.size(), 5);
        list.clear();
        assertTrue(list.isEmpty());
    }
    @Test
    void shouldClearViaIterator() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        Iterator<Long> it = list.iterator();
        while(it.hasNext()) {
            Object next = it.next();
            assertNotNull(next);
            it.remove();
        }
        assertTrue(list.isEmpty());
    }
    @Test
    void shouldClearViaIteratorInReverse() {
        CouchbaseArrayList<Long> list = new CouchbaseArrayList<>(uuid, collection, Long.class, options);
        list.addAll(Arrays.asList(1L,2L,3L,4L,5L));
        ListIterator<Long> it = list.listIterator();
        Object current = null;
        // move to end
        while(it.hasNext()) current = it.next();
        assertEquals(current, 5);
        int i = 5;
        while(it.hasPrevious()) {
            current = it.previous();
            assertEquals(current, i);
            i--;
            it.remove();
        }
        assertEquals(list.size(), 0);
    }
    @Test
    void shouldCreateDirectlyFromTheCouchbaseCollection() {
        CouchbaseArrayList<Long> list = collection.list(uuid, Long.class, options);
        assertTrue(list.isEmpty());
        list.add(0, 1L);
        assertFalse(list.isEmpty());
        CouchbaseArrayList<Long> list2 = collection.list(uuid, Long.class, options);
        assertFalse(list2.isEmpty());
    }
    @Test
    void shouldAddViaIterator() {
        CouchbaseArrayList<Integer> list = collection.list(uuid, Integer.class, ArrayListOptions.arrayListOptions());
        list.addAll(Arrays.asList(1,2,3,4,5));

        ListIterator<Integer> it = list.listIterator();
        while (it.hasNext()) {
            int val = it.next().intValue();
            if (val % 3 == 0) {
                it.add(100);
            }
        }
        assertEquals(6, list.size());
        assertEquals(100, list.get(3).intValue());
    }
    @Test
    void shouldSetViaIterator() {
        CouchbaseArrayList<Integer> list = collection.list(uuid, Integer.class, ArrayListOptions.arrayListOptions());
        list.addAll(Arrays.asList(1,2,3,4,5));

        ListIterator<Integer> it = list.listIterator();
        while (it.hasNext()) {
            int val = it.next().intValue();
            if (val % 3 == 0) {
                it.set(100);
            }
        }
        assertEquals(5, list.size());
        assertEquals(100, list.get(2).intValue());
    }
    @Test
    void shouldRemoveViaIterator() {
        CouchbaseArrayList<Integer> list = collection.list(uuid, Integer.class, ArrayListOptions.arrayListOptions());
        list.addAll(Arrays.asList(1,2,3,4,5));

        ListIterator<Integer> it = list.listIterator();
        while (it.hasNext()) {
            int val = it.next().intValue();
            if (val % 3 == 0) {
                it.remove();
            }
        }
        assertEquals(4, list.size());
        assertFalse(list.contains(3));
    }
    @Test
    void shouldNotAddViaIteratorIfListChanged() {
        CouchbaseArrayList<Integer> list = collection.list(uuid, Integer.class, ArrayListOptions.arrayListOptions());
        list.addAll(Arrays.asList(1,2,3,4,5));

        ListIterator<Integer> it = list.listIterator();
        // change list _after_ getting iterator
        list.add(100);
        assertThrows(ConcurrentModificationException.class, ()-> {it.add(100);});
    }
    @Test
    void shouldNotSetViaIteratorIfListChanged() {
        CouchbaseArrayList<Integer> list = collection.list(uuid, Integer.class, ArrayListOptions.arrayListOptions());
        list.addAll(Arrays.asList(1,2,3,4,5));

        ListIterator<Integer> it = list.listIterator();
        // change list _after_ getting iterator
        list.add(100);

        // We have to 'visit' something before setting or removing.
        it.next();
        assertThrows(ConcurrentModificationException.class, ()-> {it.set(100);});
    }
    @Test
    void shouldNotRemoveViaIteratorIfListChanged() {
        CouchbaseArrayList<Integer> list = collection.list(uuid, Integer.class, ArrayListOptions.arrayListOptions());
        list.addAll(Arrays.asList(1,2,3,4,5));

        ListIterator<Integer> it = list.listIterator();
        // change list _after_ getting iterator
        list.add(100);

        // We have to 'visit' something before setting or removing.
        it.next();
        assertThrows(ConcurrentModificationException.class, ()-> {it.remove();});
    }

}
