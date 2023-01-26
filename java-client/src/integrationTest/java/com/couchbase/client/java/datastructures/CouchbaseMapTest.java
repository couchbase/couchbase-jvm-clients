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
import com.couchbase.client.java.kv.MapOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class CouchbaseMapTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection collection;
    private static MapOptions options;

    private String uuid;

    @BeforeAll
    static void setup() {
        cluster = createCluster();
        Bucket bucket = cluster.bucket(config().bucketname());
        collection = bucket.defaultCollection();
        options = MapOptions.mapOptions();
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
        } catch(Exception e) {
            // noop
        }
    }

    private HashMap<String, Integer> createJavaMap() {
        HashMap<String, Integer> javaMap = new HashMap<>();
        javaMap.put("a", 1);
        javaMap.put("b", 2);
        javaMap.put("c", 3);
        javaMap.put("d", 4);
        javaMap.put("e", 5);
        return javaMap;
    }

    @Test
    void shouldCreateEmptyMap() {
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        assertEquals(0, map.size());
        assertThrows(DocumentNotFoundException.class, () -> collection.get(uuid));
    }
    @Test
    void shouldPut() {
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        assertEquals(0, map.size());
        Integer oldVal = map.put("a", 1);
        assertNull(oldVal);
        assertEquals(1, map.size());
        assertEquals(1, map.get("a").intValue());
    }
    @Test
    void shouldPutWithUpsertAndReturnOldValue() {
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        map.putAll(createJavaMap());
        Integer oldVal = map.put("a", 100);
        assertEquals(1, oldVal.intValue());
        assertEquals(100, map.get("a").intValue());
    }
    @Test
    void shouldRemove() {
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        map.putAll(createJavaMap());
        Integer oldVal = map.remove("a");
        assertEquals(4, map.size());
        assertEquals(1, oldVal.intValue());
        assertFalse(map.containsKey("a"));
    }
    @Test
    void shouldRemoveWhenEmpty() {
        Map<String, Integer> map = collection.map(uuid, Integer.class);
        assertNull(map.remove("foo"));
    }
    @Test
    void shouldClear() {
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        map.putAll(createJavaMap());
        assertEquals(5, map.size());
        map.clear();
        assertEquals(0, map.size());
        assertThrows(DocumentNotFoundException.class, () -> {collection.get(uuid);});
    }
    @Test
    void shouldEntrySet() {
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        map.putAll(createJavaMap());
        assertEquals(5, map.size());
        assertEquals(5, map.entrySet().size());
    }
    @Test
    void shouldEntrySetWhenEmpty() {
        Map<String, Integer> map = collection.map(uuid, Integer.class);
        assertEquals(0, map.entrySet().size());
    }
    @Test
    void canIterate() {
        HashMap<String, Integer> javaMap = createJavaMap();
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        map.putAll(javaMap);
        map.forEach((key, value) -> {
            assertTrue(javaMap.containsKey(key));
            assertTrue(javaMap.containsValue(value));
        });
    }
    @Test
    void canRemoveFromIterate() {
        Map<String, Integer> map = collection.map(uuid, Integer.class, options);
        map.putAll(createJavaMap());
        assertEquals(5, map.size());
        Iterator<Map.Entry<String, Integer>>  it = map.entrySet().iterator();
        Map.Entry<String, Integer> entry = it.next();
        assertTrue(map.containsKey(entry.getKey()));
        assertTrue(map.containsValue(entry.getValue()));
        it.remove();
        assertEquals(4, map.size());
        assertFalse(map.containsKey(entry.getKey()));
        assertFalse(map.containsKey(entry.getValue()));
    }
    @Test
    void canContainsKey() {
        HashMap<String, Integer> javaMap = createJavaMap();
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        map.putAll(javaMap);
        for(String key: javaMap.keySet()) {
            assertTrue(map.containsKey(key));
        }
    }
    @Test
    void canContainsKeyWhenEmpty() {
        Map<String, Integer> map = collection.map(uuid, Integer.class);
        assertFalse(map.containsKey("foo"));
    }
    @Test
    void canContainsValueWhenEmpty() {
        Map<String, Integer> map = collection.map(uuid, Integer.class);
        assertFalse(map.containsValue(1));
    }
    @Test
    void canContainsValue() {
        HashMap<String, Integer> javaMap = createJavaMap();
        CouchbaseMap<Integer> map = new CouchbaseMap<>(uuid, collection, Integer.class, options);
        map.putAll(javaMap);
        for(Integer value: javaMap.values()) {
            assertTrue(map.containsValue(value));
        }
    }
    @Test
    void canUseJsonObjects() {
        CouchbaseMap<JsonObject> map = new CouchbaseMap<>(uuid, collection, JsonObject.class, options);
        JsonObject obj1 = JsonObject.fromJson("{\"string\":\"foo\", \"integer\":1}");
        JsonObject obj2 = JsonObject.fromJson("{\"string\":\"bar\", \"integer\":2}");
        map.put("a", obj1);
        map.put("b", obj2);
        assertEquals(2, map.size());
        assertEquals(obj1, map.get("a"));
        assertEquals(obj2, map.get("b"));
    }
    @Test
    void canUseObjects() {
        CouchbaseMap<TestObject> map = new CouchbaseMap<>(uuid, collection, TestObject.class, options);
        TestObject obj1 = new TestObject(1, "foo");
        TestObject obj2 = new TestObject(2, "bar");
        map.put("a", obj1);
        map.put("b", obj2);
        assertEquals(obj1, map.get("a"));
        assertEquals(obj2, map.get("b"));
    }
    @Test
    void canConstructUsingCouchbaseCollection() {
        Map<String, Integer> map = collection.map(uuid, Integer.class, options);
        assertTrue(map.isEmpty());
        map.put("a", 1);
        assertFalse(map.isEmpty());
        Map<String, Integer> map2 = collection.map(uuid, Integer.class, options);
        assertFalse(map.isEmpty());
        assertFalse(map2.isEmpty());
    }
}
