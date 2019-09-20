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
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Ported from the Scala SubdocMutateSpec tests.  Please keep in sync.
 */
class SubdocMutateTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection coll;

    @BeforeAll
    static void setup() {
        cluster = Cluster.connect(connectionString(), clusterOptions());
        Bucket bucket = cluster.bucket(config().bucketname());
        coll = bucket.defaultCollection();
    }

    @AfterAll
    static void tearDown() {
        cluster.shutdown();
    }


    private JsonObject getContent(String docId) {
        return coll.get(docId).contentAsObject();
    }

    private String prepare(JsonObject content) {
        String docId = docId();
        coll.insert(docId, content);
        return docId;
    }


    private String prepareXattr(JsonObject content) {
        String docId = docId();
        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.insert("x", content).xattr()),
                mutateInOptions().insertDocument(true));
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
                        Arrays.asList()));
    }

    @Test
    public void insertString() {
        JsonObject content = JsonObject.create();
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.insert("foo2", "bar2")));

        assertEquals("bar2", getContent(docId).getString("foo2"));
    }

    @Test
    public void remove() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.remove("foo")));

        assertFalse(getContent(docId).containsKey("foo"));
    }


    private JsonObject checkSingleOpSuccess(JsonObject content, MutateInSpec ops) {
        return checkSingleOpSuccess(content, Arrays.asList(ops));
    }

    private JsonObject checkSingleOpSuccess(JsonObject content, List<MutateInSpec> ops) {
        String docId = prepare(content);

        coll.mutateIn(docId, ops);

        return coll.get(docId).contentAsObject();
    }

    private JsonObject checkSingleOpSuccessXattr(JsonObject content, MutateInSpec ops) {
        return checkSingleOpSuccessXattr(content, Arrays.asList(ops));
    }

    private JsonObject checkSingleOpSuccessXattr(JsonObject content, List<MutateInSpec> ops) {
        String docId = prepareXattr(content);

        coll.mutateIn(docId, ops);

        return coll.lookupIn(docId, Arrays.asList(LookupInSpec.get("x").xattr())).contentAsObject(0);
    }

    private void checkSingleOpFailure(JsonObject content, MutateInSpec ops, SubDocumentOpResponseStatus expected) {
        String docId = prepare(content);

        try {
            coll.mutateIn(docId, Arrays.asList(ops));
            fail();
        } catch (MultiMutationException err) {
            assertEquals(expected, err.firstFailureStatus());
        }
    }

    private void checkSingleOpFailureXattr(JsonObject content, MutateInSpec ops, SubDocumentOpResponseStatus
            expected) {
        checkSingleOpFailureXattr(content, Arrays.asList(ops), expected);
    }

    private void checkSingleOpFailureXattr(JsonObject
                                                   content, List<MutateInSpec> ops, SubDocumentOpResponseStatus expected) {
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
                MutateInSpec.insert("foo", "bar2"), SubDocumentOpResponseStatus.PATH_EXISTS);
    }


    // TODO get these working

    //  @Test
//public void mutateIn insert bool() {
    //    JsonObject content = JsonObject.create();
    //    String docId = prepare(content);
    //
    //    coll.mutateIn(docId,
//Arrays.asList(MutateInSpec.insert("foo2", false)) match {
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
//Arrays.asList(MutateInSpec.insert("foo2", 42)) match {
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
//Arrays.asList(MutateInSpec.insert("foo2", 42.3)) match {
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
                Arrays.asList(MutateInSpec.replace("foo", "bar2")));

        assertEquals("bar2", getContent(docId).getString("foo"));
    }

    @Test
    public void replaceStringDoesNotExist() {
        checkSingleOpFailure(JsonObject.create(),
                MutateInSpec.replace("foo", "bar2"), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void upsertString() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.upsert("foo", "bar2")));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExist() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.upsert("foo", "bar2")));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void arrayAppend() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayAppend("foo", "world")));
        assertEquals(JsonArray.from("hello", "world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrepend() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayPrepend("foo", "world")));
        assertEquals(JsonArray.from("world", "hello"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsert() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayInsert("foo[1]", "cruel")));
        assertEquals(JsonArray.from("hello", "cruel", "world"), updatedContent.getArray("foo"));
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    public void arrayInsertUniqueDoesNotExist() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayAddUnique("foo", "cruel")));
        assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsertUniqueDoesExist() {
        checkSingleOpFailure(
                JsonObject.create().put("foo", JsonArray.from("hello", "cruel", "world")),
                MutateInSpec.arrayAddUnique("foo", "cruel"),
                SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void counterAdd() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.increment("foo", 5)));
        assertEquals(15, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinus() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.decrement("foo", 3)));
        assertEquals(7, (int) updatedContent.getInt("foo"));
    }


    @Test
    public void insertXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void removeXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.remove("x.foo").xattr()));
        assertFalse(updatedContent.containsKey("foo"));
    }

    @Test
    public void removeXattrDoesNotExist() {
        checkSingleOpFailureXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.remove("x.foo").xattr()), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void insertStringAlreadyThereXattr() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.insert("x.foo", "bar2").xattr()), SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void replaceStringXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.replace("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void replaceStringDoesNotExistXattr() {
        checkSingleOpFailure(JsonObject.create(),
                MutateInSpec.replace("x.foo", "bar2").xattr(), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void upsertStringXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.upsert("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExistXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.upsert("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void arrayAppendXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayAppend("x.foo", "world").xattr()));
        assertEquals(JsonArray.from("hello", "world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrependXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayPrepend("x.foo", "world").xattr()));
        assertEquals(JsonArray.from("world", "hello"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsertXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayInsert("x.foo[1]", "cruel").xattr()));
        assertEquals(JsonArray.from("hello", "cruel", "world"), updatedContent.getArray("foo"));
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    public void arrayInsertUniqueDoesNotExistXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayAddUnique("x.foo", "cruel").xattr()));
        assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsertUniqueDoesExistXattr() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", JsonArray.from("hello", "cruel", "world")),
                Arrays.asList(MutateInSpec.arrayAddUnique("x.foo", "cruel").xattr()),
                SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void counterAddXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.increment("x.foo", 5).xattr()));
        assertEquals(15, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinusXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.decrement("x.foo", 3).xattr()));
        assertEquals(7, (int) updatedContent.getInt("foo"));
    }


    @Test
    public void insertExpandMacroXattrDoNotFlag() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo", "${Mutation.CAS}").xattr()));
        assertEquals("${Mutation.CAS}", updatedContent.getString("foo"));
    }

    // TODO macro sentinel values
    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    public void insertExpandMacroXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo", "${Mutation.CAS}").xattr().expandMacro()));
        assertNotEquals("${Mutation.CAS}", updatedContent.getString("foo"));
    }


    @Test
    public void insertXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo.baz", "bar2").xattr().createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void insertStringAlreadyThereXattrCreatePath() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                Arrays.asList(MutateInSpec.insert("x.foo.baz", "bar2")), SubDocumentOpResponseStatus.PATH_NOT_FOUND);
    }

    @Test
    public void upsertStringXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                Arrays.asList(MutateInSpec.upsert("x.foo", "bar2").xattr().createPath()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExistXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.upsert("x.foo.baz", "bar2").xattr().createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void arrayAppendXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayAppend("x.foo", "world").xattr().createPath()));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrependXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayPrepend("x.foo", "world").xattr().createPath()));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    // TODO failing with bad input server error
    //  @Test
//public void arrayInsertXattrCreatePath() {
//    JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
//      Arrays.asList(MutateInSpec.arrayInsert(true, "x.foo[0]", "cruel", true));
//    assertEquals(JsonArray.from("cruel"), updatedContent.getArray("foo"));
//  }
//
//  @Test
//public void arrayInsertUniqueDoesNotExistXattrCreatePath() {
//    JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
//      Arrays.asList(MutateInSpec.arrayAddUnique(true, "x.foo", "cruel", true));
//    assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
//  }


    @Test
    public void counterAddXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.increment("x.foo", 5).xattr().createPath()));
        assertEquals(5, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinusXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.decrement("x.foo", 3).xattr().createPath()));
        assertEquals(-3, (int) updatedContent.getInt("foo"));
    }


    @Test
    public void insertCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("foo.baz", "bar2").createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void insertStringAlreadyThereCreatePath() {
        checkSingleOpFailure(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                MutateInSpec.insert("foo.baz", "bar2"), SubDocumentOpResponseStatus.PATH_EXISTS);
    }

    @Test
    public void upsertStringCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                Arrays.asList(MutateInSpec.upsert("foo", "bar2").createPath()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    public void upsertStringDoesNotExistCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.upsert("foo.baz", "bar2").createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    public void arrayAppendCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayAppend("foo", "world").createPath()));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrependCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayPrepend("foo", "world").createPath()));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    // TODO failing with bad input server error
    //  @Test
//public void arrayInsertCreatePath() {
    //    JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
    //      Arrays.asList(MutateInSpec.arrayInsert("foo[0]", "cruel", true));
    //    assertEquals(JsonArray.from("cruel"), updatedContent.getArray("foo"));
    //  }
    //
    //  @Test
//public void arrayInsertUniqueDoesNotExistCreatePath() {
    //    JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
    //      Arrays.asList(MutateInSpec.arrayAddUnique("foo", "cruel", true));
    //    assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    //  }


    @Test
    public void counterAddCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.increment("foo", 5).createPath()));
        assertEquals(5, (int) updatedContent.getInt("foo"));
    }

    @Test
    public void counterMinusCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.decrement("foo", 3).createPath()));
        assertEquals(-3, (int) updatedContent.getInt("foo"));
    }


    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    public void expiration() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.insert("foo2", "bar2")), MutateInOptions.mutateInOptions().expiry(Duration.ofSeconds(10)));

        GetResult result = coll.get(docId, GetOptions.getOptions().withExpiry(true));
        assertTrue(result.expiry().isPresent());
        assertTrue(result.expiry().get().getSeconds() != 0);
    }


    @Test
    public void moreThan16() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        assertThrows(IllegalArgumentException.class, () ->
                coll.mutateIn(docId,
                        Arrays.asList(MutateInSpec.insert("foo0", "bar0"),
                                MutateInSpec.insert("foo1", "bar1"),
                                MutateInSpec.insert("foo2", "bar2"),
                                MutateInSpec.insert("foo3", "bar3"),
                                MutateInSpec.insert("foo4", "bar4"),
                                MutateInSpec.insert("foo5", "bar5"),
                                MutateInSpec.insert("foo6", "bar6"),
                                MutateInSpec.insert("foo7", "bar7"),
                                MutateInSpec.insert("foo8", "bar8"),
                                MutateInSpec.insert("foo9", "bar9"),
                                MutateInSpec.insert("foo10", "bar10"),
                                MutateInSpec.insert("foo11", "bar11"),
                                MutateInSpec.insert("foo12", "bar12"),
                                MutateInSpec.insert("foo13", "bar13"),
                                MutateInSpec.insert("foo14", "bar14"),
                                MutateInSpec.insert("foo15", "bar15"),
                                MutateInSpec.insert("foo16", "bar16"))));
    }

    @Test
    public void twoCommandsSucceed() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.insert("foo0", "bar0"),
                        MutateInSpec.insert("foo1", "bar1"),
                        MutateInSpec.insert("foo2", "bar2")));

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
                        Arrays.asList(MutateInSpec.insert("foo0", "bar0"),
                                MutateInSpec.insert("foo1", "bar1"),
                                MutateInSpec.remove("foo3"))));

        JsonObject updated = getContent(docId);
        assertEquals("bar_orig_1", updated.getString("foo1"));
    }


}
