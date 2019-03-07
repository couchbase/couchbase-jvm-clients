/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"));
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

package com.couchbase.client.java;

import com.couchbase.client.core.error.subdoc.MultiMutationException;
import com.couchbase.client.core.msg.kv.SubDocumentOpResponseStatus;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

import static com.couchbase.client.java.kv.MutateInOps.mutateInOps;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Ported from the Scala SubdocMutateSpec tests.  Please keep in sync.
 */
class SubdocMutateTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static ClusterEnvironment environment;
    private static Collection coll;

    @BeforeAll
    static void setup() {
        environment = environment().build();
        cluster = Cluster.connect(environment);
        Bucket bucket = cluster.bucket(config().bucketname());
        coll = bucket.defaultCollection();
    }

    @AfterAll
    static void tearDown() {
        environment.shutdown();
        cluster.shutdown();
    }


    private JsonObject getContent(String docId) {
        return coll.get(docId).get().contentAsObject();
    }

    private String prepare(JsonObject content) {
        String docId = docId();
        coll.insert(docId, content);
        return docId;
    }


    private String prepareXattr(JsonObject content) {
        String docId = docId();
        coll.mutateIn(docId,
                mutateInOps().insert(true, "x", content), mutateInOptions().insertDocument(true));
        return docId;
    }

    private String docId() {
        return UUID.randomUUID().toString();
    }

    @Test
    public void noCommands() {
        String docId = docId();
        assertThrows(IllegalArgumentException.class, () ->
                coll.mutateIn(docId,
                        mutateInOps()));
    }


    @Test
    public void insertString() {
        JsonObject content = JsonObject.create();
        String docId = prepare(content);

        coll.mutateIn(docId,
                mutateInOps().insert("foo2", "bar2"));

        assertEquals("bar2", getContent(docId).getString("foo2"));
    }

    @Test
    public void remove() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        String docId = prepare(content);

        coll.mutateIn(docId,
                mutateInOps().remove("foo"));

        assertFalse(getContent(docId).containsKey("foo"));
    }


    private JsonObject checkSingleOpSuccess(JsonObject content, MutateInOps ops) {
        String docId = prepare(content);

        coll.mutateIn(docId, ops);

        return coll.get(docId).get().contentAsObject();
    }

    private JsonObject checkSingleOpSuccessXattr(JsonObject content, MutateInOps ops) {
        String docId = prepareXattr(content);

        coll.mutateIn(docId, ops);

        return coll.lookupIn(docId, Arrays.asList(LookupInOp.get("x").xattr())).get().contentAsObject(0);
    }

    private void checkSingleOpFailure(JsonObject content, MutateInOps ops, SubDocumentOpResponseStatus expected) {
        String docId = prepare(content);

        try {
            coll.mutateIn(docId, ops);
            fail();
        } catch (MultiMutationException err) {
            assertEquals(expected, err.firstFailureStatus());
        }
    }

    private void checkSingleOpFailureXattr(JsonObject content, MutateInOps ops, SubDocumentOpResponseStatus expected) {
        String docId = prepareXattr(content);

        try {
            coll.mutateIn(docId, ops);
            fail();
        } catch (MultiMutationException err) {
            assertEquals(expected, err.firstFailureStatus());
        }
    }

    @Test
    public void insertStringAlreadyThere() {
        checkSingleOpFailure(JsonObject.create().put("foo", "bar"),
                mutateInOps().insert("foo", "bar2"), SubDocumentOpResponseStatus.PATH_EXISTS);
    }


    // TODO get these working

    //  @Test
//public void mutateIn insert bool() {
    //    JsonObject content = JsonObject.create();
    //    String docId = prepare(content);
    //
    //    coll.mutateIn(docId,
//mutateInOps().insert("foo2", false)) match {
    //      case Success(result) => assert(result.cas != cas);
    //      case Failure(err) =>
    //        assert(false, s"unexpected error $err"));
    //    }
    //
    //    assert(!getContent(docId).getString("foo2").bool);
    //  }
    //
    //  @Test
//public void mutateIn insert int() {
    //    JsonObject content = JsonObject.create();
    //    String docId = prepare(content);
    //
    //    coll.mutateIn(docId,
//mutateInOps().insert("foo2", 42)) match {
    //      case Success(result) => assert(result.cas != cas);
    //      case Failure(err) =>
    //        assert(false, s"unexpected error $err"));
    //    }
    //
    //    assertEquals(42, getContent(docId).getString("foo2").num);
    //  }
    //
    //
    //  @Test
//public void mutateIn insert double() {
    //    JsonObject content = JsonObject.create();
    //    String docId = prepare(content);
    //
    //    coll.mutateIn(docId,
//mutateInOps().insert("foo2", 42.3)) match {
    //      case Success(result) => assert(result.cas != cas);
    //      case Failure(err) =>
    //        assert(false, s"unexpected error $err"));
    //    }
    //
    //    assertEquals(42.3, getContent(docId).getString("foo2").num);
    //  }

    @Test
    public void replaceString() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        String docId = prepare(content);

        coll.mutateIn(docId,
                mutateInOps().replace("foo", "bar2"));

        assertEquals("bar2", getContent(docId).getString("foo"));
    }

    @Test
    public void replaceStringDoesNotExist() {
        checkSingleOpFailure(JsonObject.create(),
                mutateInOps().replace("foo", "bar2"), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void upsertString() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", "bar"),
                mutateInOps().upsert("foo", "bar2"));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExist() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                mutateInOps().upsert("foo", "bar2"));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void arrayAppend() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
                mutateInOps().arrayAppend("foo", "world"));
        assertEquals(JsonArray.from("hello", "world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrepend() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
                mutateInOps().arrayPrepend("foo", "world"));
        assertEquals(JsonArray.from("world", "hello"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsert() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                mutateInOps().arrayInsert("foo[1]", "cruel"));
        assertEquals(JsonArray.from("hello", "cruel", "world"), updatedContent.getArray("foo"));
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    public void arrayInsertUniqueDoesNotExist() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                mutateInOps().arrayAddUnique("foo", "cruel"));
        assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsertUniqueDoesExist() {
        checkSingleOpFailure(
                JsonObject.create().put("foo", JsonArray.from("hello", "cruel", "world")),
                mutateInOps().arrayAddUnique("foo", "cruel"),
                SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void counterAdd() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", 10),
                mutateInOps().increment("foo", 5));
        assertEquals(15, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinus() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", 10),
                mutateInOps().decrement("foo", 3));
        assertEquals(7, (int) updatedContent.getInt("foo"));
    }


    @Test
    public void insertXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().insert(true, "x.foo", "bar2"));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void removeXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                mutateInOps().remove(true, "x.foo"));
        assertFalse(updatedContent.containsKey("foo"));
    }

    @Test
    public void removeXattrDoesNotExist() {
        checkSingleOpFailureXattr(JsonObject.create(),
                mutateInOps().remove(true, "x.foo"), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void insertStringAlreadyThereXattr() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", "bar"),
                mutateInOps().insert(true, "x.foo", "bar2"), SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void replaceStringXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                mutateInOps().replace(true, "x.foo", "bar2"));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void replaceStringDoesNotExistXattr() {
        checkSingleOpFailure(JsonObject.create(),
                mutateInOps().replace(true, "x.foo", "bar2"), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void upsertStringXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                mutateInOps().upsert(true, "x.foo", "bar2"));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExistXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().upsert(true, "x.foo", "bar2"));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void arrayAppendXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello")),
                mutateInOps().arrayAppend(true, "x.foo", "world"));
        assertEquals(JsonArray.from("hello", "world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrependXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello")),
                mutateInOps().arrayPrepend(true, "x.foo", "world"));
        assertEquals(JsonArray.from("world", "hello"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsertXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                mutateInOps().arrayInsert(true, "x.foo[1]", "cruel"));
        assertEquals(JsonArray.from("hello", "cruel", "world"), updatedContent.getArray("foo"));
    }

    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    public void arrayInsertUniqueDoesNotExistXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                mutateInOps().arrayAddUnique(true, "x.foo", "cruel"));
        assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsertUniqueDoesExistXattr() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", JsonArray.from("hello", "cruel", "world")),
                mutateInOps().arrayAddUnique(true, "x.foo", "cruel"),
                SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void counterAddXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", 10),
                mutateInOps().increment(true, "x.foo", 5));
        assertEquals(15, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinusXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", 10),
                mutateInOps().decrement(true, "x.foo", 3));
        assertEquals(7, (int) updatedContent.getInt("foo"));
    }


    @Test
    public void insertExpandMacroXattrDoNotFlag() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().insert(true, "x.foo", "${Mutation.CAS}"));
        assertEquals("${Mutation.CAS}", updatedContent.getString("foo"));
    }

    // TODO macro sentinel values
    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    public void insertExpandMacroXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().insert(true, "x.foo", "${Mutation.CAS}", false, true));
        assertNotEquals("${Mutation.CAS}", updatedContent.getString("foo"));
    }


    @Test
    public void insertXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().insert(true, "x.foo.baz", "bar2", true, false));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void insertStringAlreadyThereXattrCreatePath() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                mutateInOps().insert("x.foo.baz", "bar2"), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void upsertStringXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                mutateInOps().upsert(true, "x.foo", "bar2", true, false));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExistXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().upsert(true, "x.foo.baz", "bar2", true, false));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void arrayAppendXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().arrayAppend(true, "x.foo", "world", true, false));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrependXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().arrayPrepend(true, "x.foo", "world", true, false));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    // TODO failing with bad input server error
    //  @Test
//public void arrayInsertXattrCreatePath() {
//    JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
//      mutateInOps().arrayInsert(true, "x.foo[0]", "cruel", true));
//    assertEquals(JsonArray.from("cruel"), updatedContent.getArray("foo"));
//  }
//
//  @Test
//public void arrayInsertUniqueDoesNotExistXattrCreatePath() {
//    JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
//      mutateInOps().arrayAddUnique(true, "x.foo", "cruel", true));
//    assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
//  }


    @Test
    public void counterAddXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().increment(true, "x.foo", 5, true));
        assertEquals(5, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinusXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                mutateInOps().decrement(true, "x.foo", 3, true));
        assertEquals(-3, (int) updatedContent.getInt("foo"));
    }


    @Test
    public void insertCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                mutateInOps().insert("foo.baz", "bar2", true));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void insertStringAlreadyThereCreatePath() {
        checkSingleOpFailure(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                mutateInOps().insert("foo.baz", "bar2"), SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void upsertStringCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                mutateInOps().upsert("foo", "bar2", true));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExistCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                mutateInOps().upsert("foo.baz", "bar2", true));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void arrayAppendCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                mutateInOps().arrayAppend("foo", "world", true));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrependCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                mutateInOps().arrayPrepend("foo", "world", true));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    // TODO failing with bad input server error
    //  @Test
//public void arrayInsertCreatePath() {
    //    JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
    //      mutateInOps().arrayInsert("foo[0]", "cruel", true));
    //    assertEquals(JsonArray.from("cruel"), updatedContent.getArray("foo"));
    //  }
    //
    //  @Test
//public void arrayInsertUniqueDoesNotExistCreatePath() {
    //    JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
    //      mutateInOps().arrayAddUnique("foo", "cruel", true));
    //    assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    //  }


    @Test
    public void counterAddCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                mutateInOps().increment("foo", 5, true));
        assertEquals(5, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinusCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                mutateInOps().decrement("foo", 3, true));
        assertEquals(-3, (int) updatedContent.getInt("foo"));
    }


    @Test
    @IgnoreWhen( clusterTypes = { ClusterType.MOCKED })
    public void expiration() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        coll.mutateIn(docId,
                mutateInOps().insert("foo2", "bar2"), MutateInOptions.mutateInOptions().expiry(Duration.ofSeconds(10)));

        GetResult result = coll.get(docId, GetOptions.getOptions().withExpiration(true)).get();
        assertTrue(result.expiration().isPresent());
        assertTrue(result.expiration().get().getSeconds() != 0);
    }


    @Test
    public void moreThan16() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        assertThrows(IllegalArgumentException.class, () ->
                coll.mutateIn(docId,
                        mutateInOps().insert("foo0", "bar0")
                                .insert("foo1", "bar1")
                                .insert("foo2", "bar2")
                                .insert("foo3", "bar3")
                                .insert("foo4", "bar4")
                                .insert("foo5", "bar5")
                                .insert("foo6", "bar6")
                                .insert("foo7", "bar7")
                                .insert("foo8", "bar8")
                                .insert("foo9", "bar9")
                                .insert("foo10", "bar10")
                                .insert("foo11", "bar11")
                                .insert("foo12", "bar12")
                                .insert("foo13", "bar13")
                                .insert("foo14", "bar14")
                                .insert("foo15", "bar15")
                                .insert("foo16", "bar16")));
    }

    @Test
    public void twoCommandsSucceed() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        coll.mutateIn(docId,
                mutateInOps().insert("foo0", "bar0")
                        .insert("foo1", "bar1")
                        .insert("foo2", "bar2"));

        JsonObject updated = getContent(docId);
        assertEquals("bar1", updated.getString("foo1"));
        assertEquals("bar2", updated.getString("foo2"));
    }


    @Test
    public void twoCommandsOneFails() {
        JsonObject content = JsonObject.create().put("foo1", "bar_orig_1").put("foo2", "bar_orig_2");
        String docId = prepare(content);

        assertThrows(MultiMutationException.class, () ->
                coll.mutateIn(docId,
                        mutateInOps().insert("foo0", "bar0")
                                .insert("foo1", "bar1")
                                .remove("foo3")));

        JsonObject updated = getContent(docId);
        assertEquals("bar_orig_1", updated.getString("foo1"));
    }


}
