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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.subdoc.PathExistsException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInMacro;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.StoreSemantics;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.MutateInSpec.upsert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Ported from the Scala SubdocMutateSpec tests.  Please keep in sync.
 */
class SubdocMutateIntegrationTest extends JavaIntegrationTest {

    private static Cluster cluster;
    private static Collection coll;

    @BeforeAll
    static void setup() {
      cluster = Cluster.connect(seedNodes(), clusterOptions());
      Bucket bucket = cluster.bucket(config().bucketname());
      coll = bucket.defaultCollection();
      bucket.waitUntilReady(Duration.ofSeconds(5));
    }

    @AfterAll
    static void tearDown() {
        cluster.disconnect();
    }


    private JsonObject getContent(String docId) {
        return coll.get(docId).contentAsObject();
    }

    private String prepare(JsonObject content) {
        String docId = docId();
        try {
          coll.insert(docId, content);
        } catch (RequestCanceledException ex) {
          // suspect this happens because another test is running and the server closed because of some
          // invalid arg tests... redo the insert one more time
          coll.insert(docId, content);
        }
        return docId;
    }


    private String prepareXattr(JsonObject content) {
        String docId = docId();
        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.insert("x", content).xattr()),
                mutateInOptions().storeSemantics(StoreSemantics.INSERT));
        return docId;
    }

    private String docId() {
        return UUID.randomUUID().toString();
    }

    @Test
    void noCommands() {
        assertThrows(InvalidArgumentException.class, () -> coll.mutateIn(docId(), Collections.emptyList()));
    }

    @Test
    void insertString() {
        JsonObject content = JsonObject.create();
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.insert("foo2", "bar2")));

        assertEquals("bar2", getContent(docId).getString("foo2"));
    }

    @Test
    void remove() {
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

    private void checkSingleOpFailure(JsonObject content, MutateInSpec ops, Class<?> expected) {
        String docId = prepare(content);

        try {
            coll.mutateIn(docId, Arrays.asList(ops));
            fail();
        } catch (CouchbaseException ex) {
           assertTrue(ex.getClass().isAssignableFrom(expected));
        }
    }

    private void checkSingleOpFailureXattr(JsonObject content, MutateInSpec ops, Class<?> expected) {
        checkSingleOpFailureXattr(content, Arrays.asList(ops), expected);
    }

    private void checkSingleOpFailureXattr(JsonObject content, List<MutateInSpec> ops, Class<?> expected) {
        String docId = prepareXattr(content);

        try {
            coll.mutateIn(docId, ops);
            fail();
        } catch (CouchbaseException ex) {
            assertTrue(ex.getClass().isAssignableFrom(expected));
        }
    }

    @Test
    void insertStringAlreadyThere() {
        checkSingleOpFailure(JsonObject.create().put("foo", "bar"),
                MutateInSpec.insert("foo", "bar2"), PathExistsException.class);
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
    void replaceString() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.replace("foo", "bar2")));

        assertEquals("bar2", getContent(docId).getString("foo"));
    }

    @Test
    void replaceFullDocument() {
        JsonObject content = JsonObject.create().put("foo", "bar");
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.replace("", JsonObject.create().put("foo2", "bar2"))));

        assertEquals("bar2", getContent(docId).getString("foo2"));
    }

    @Test
    void replaceStringDoesNotExist() {
        checkSingleOpFailure(JsonObject.create(),
                MutateInSpec.replace("foo", "bar2"), PathNotFoundException.class);
    }

    @Test
    void upsertString() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", "bar"),
                Arrays.asList(upsert("foo", "bar2")));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void upsertStringDoesNotExist() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(upsert("foo", "bar2")));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void arrayAppend() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayAppend("foo", Arrays.asList("world"))));
        assertEquals(JsonArray.from("hello", "world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayAppendMulti() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayAppend("foo", Arrays.asList("world", "mars"))));
        assertEquals(JsonArray.from("hello", "world", "mars"), updatedContent.getArray("foo"));
    }

  @Test
  public void arrayAppendListString() {
    JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
            Arrays.asList(MutateInSpec.arrayAppend("foo", Arrays.asList("world", Arrays.asList("mars", "jupiter")))));
    assertEquals(JsonArray.from("hello", "world", JsonArray.from("mars", "jupiter")), updatedContent.getArray("foo"));
  }

  @Test
    public void arrayPrepend() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayPrepend("foo", Arrays.asList("world"))));
        assertEquals(JsonArray.from("world", "hello"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayPrependMulti() {
      JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello")),
              Arrays.asList(MutateInSpec.arrayPrepend("foo", Arrays.asList("world", "mars"))));
      assertEquals(JsonArray.from("world", "mars", "hello"), updatedContent.getArray("foo"));
    }

    @Test
    void arrayInsert() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayInsert("foo[1]", Arrays.asList("cruel"))));
        assertEquals(JsonArray.from("hello", "cruel", "world"), updatedContent.getArray("foo"));
    }

    @Test
    public void arrayInsertMulti() {
      JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
              Arrays.asList(MutateInSpec.arrayInsert("foo[1]", Arrays.asList("cruel", "mars"))));
      assertEquals(JsonArray.from("hello", "cruel", "mars", "world"), updatedContent.getArray("foo"));
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void arrayInsertUniqueDoesNotExist() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayAddUnique("foo", "cruel")));
        assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    }

    @Test
    void arrayInsertUniqueDoesExist() {
        checkSingleOpFailure(
                JsonObject.create().put("foo", JsonArray.from("hello", "cruel", "world")),
                MutateInSpec.arrayAddUnique("foo", "cruel"),
                PathExistsException.class);
    }

    @Test
    void counterAdd() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.increment("foo", 5)));
        assertEquals(15, (int) updatedContent.getInt("foo"));
    }

    @Test
    void counterMinus() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.decrement("foo", 3)));
        assertEquals(7, (int) updatedContent.getInt("foo"));
    }


    @Test
    void insertXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void removeXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.remove("x.foo").xattr()));
        assertFalse(updatedContent.containsKey("foo"));
    }

    @Test
    void removeXattrDoesNotExist() {
        checkSingleOpFailureXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.remove("x.foo").xattr()), PathNotFoundException.class);
    }

    @Test
    void insertStringAlreadyThereXattr() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.insert("x.foo", "bar2").xattr()), PathExistsException.class);
    }

    @Test
    void replaceStringXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(MutateInSpec.replace("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void replaceStringDoesNotExistXattr() {
        checkSingleOpFailure(JsonObject.create(),
                MutateInSpec.replace("x.foo", "bar2").xattr(), PathNotFoundException.class);
    }

    @Test
    void upsertStringXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", "bar"),
                Arrays.asList(upsert("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void upsertStringDoesNotExistXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(upsert("x.foo", "bar2").xattr()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void arrayAppendXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayAppend("x.foo", Arrays.asList("world")).xattr()));
        assertEquals(JsonArray.from("hello", "world"), updatedContent.getArray("foo"));
    }

    @Test
    void arrayPrependXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello")),
                Arrays.asList(MutateInSpec.arrayPrepend("x.foo", Arrays.asList("world")).xattr()));
        assertEquals(JsonArray.from("world", "hello"), updatedContent.getArray("foo"));
    }

    @Test
    void arrayInsertXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayInsert("x.foo[1]", Arrays.asList("cruel")).xattr()));
        assertEquals(JsonArray.from("hello", "cruel", "world"), updatedContent.getArray("foo"));
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void arrayInsertUniqueDoesNotExistXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonArray.from("hello", "world")),
                Arrays.asList(MutateInSpec.arrayAddUnique("x.foo", "cruel").xattr()));
        assertEquals(JsonArray.from("hello", "world", "cruel"), updatedContent.getArray("foo"));
    }

    @Test
    void arrayInsertUniqueDoesExistXattr() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", JsonArray.from("hello", "cruel", "world")),
                Arrays.asList(MutateInSpec.arrayAddUnique("x.foo", "cruel").xattr()),
                PathExistsException.class);
    }

    @Test
    void counterAddXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.increment("x.foo", 5).xattr()));
        assertEquals(15, (int) updatedContent.getInt("foo"));
    }

    @Test
    void xattrOpsAreReordered() {
      JsonObject content = JsonObject.create();
      String docId = prepareXattr(content);

      MutateInResult result = coll.mutateIn(docId,
              Arrays.asList(MutateInSpec.insert("foo2", "bar2"),
                      MutateInSpec.increment("x.foo", 5).xattr()));

      assertThrows(NoSuchElementException.class, () -> result.contentAs(0, String.class));
      assertEquals(5, result.contentAs(1, Integer.class));
    }

    @Test
    void counterMinusXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", 10),
                Arrays.asList(MutateInSpec.decrement("x.foo", 3).xattr()));
        assertEquals(7, (int) updatedContent.getInt("foo"));
    }


    @Test
    void insertExpandMacroXattrDoNotFlag() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo", "${Mutation.CAS}").xattr()));
        assertEquals("${Mutation.CAS}", updatedContent.getString("foo"));
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void insertExpandMacroXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo", MutateInMacro.CAS).xattr()));
        assertNotEquals("${Mutation.CAS}", updatedContent.getString("foo"));
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void upsertExpandMacroXattr() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo", MutateInMacro.CAS).xattr()));
        assertNotEquals("${Mutation.CAS}", updatedContent.getString("foo"));
    }

    @Test
    void insertXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("x.foo.baz", "bar2").xattr().createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    void insertStringAlreadyThereXattrCreatePath() {
        checkSingleOpFailureXattr(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                Arrays.asList(MutateInSpec.insert("x.foo.baz", "bar2")), PathNotFoundException.class);
    }

    @Test
    void upsertStringXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                Arrays.asList(upsert("x.foo", "bar2").xattr().createPath()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void upsertStringDoesNotExistXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(upsert("x.foo.baz", "bar2").xattr().createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    void arrayAppendXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayAppend("x.foo", Arrays.asList("world")).xattr().createPath()));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    @Test
    void arrayPrependXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayPrepend("x.foo", Arrays.asList("world")).xattr().createPath()));
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
    void counterAddXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.increment("x.foo", 5).xattr().createPath()));
        assertEquals(5, (int) updatedContent.getInt("foo"));
    }

    @Test
    void counterMinusXattrCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccessXattr(JsonObject.create(),
                Arrays.asList(MutateInSpec.decrement("x.foo", 3).xattr().createPath()));
        assertEquals(-3, (int) updatedContent.getInt("foo"));
    }


    @Test
    void insertCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.insert("foo.baz", "bar2").createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    void insertStringAlreadyThereCreatePath() {
        checkSingleOpFailure(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                MutateInSpec.insert("foo.baz", "bar2"), PathExistsException.class);
    }

    @Test
    void upsertStringCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create().put("foo", JsonObject.create().put("baz", "bar")),
                Arrays.asList(upsert("foo", "bar2").createPath()));
        assertEquals("bar2", updatedContent.getString("foo"));
    }

    @Test
    void upsertStringDoesNotExistCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(upsert("foo.baz", "bar2").createPath()));
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"));
    }

    @Test
    void arrayAppendCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayAppend("foo", Arrays.asList("world")).createPath()));
        assertEquals(JsonArray.from("world"), updatedContent.getArray("foo"));
    }

    @Test
    void arrayPrependCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.arrayPrepend("foo", Arrays.asList("world")).createPath()));
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
    void counterAddCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.increment("foo", 5).createPath()));
        assertEquals(5, (int) updatedContent.getInt("foo"));
    }

    @Test
    void counterMinusCreatePath() {
        JsonObject updatedContent = checkSingleOpSuccess(JsonObject.create(),
                Arrays.asList(MutateInSpec.decrement("foo", 3).createPath()));
        assertEquals(-3, (int) updatedContent.getInt("foo"));
    }


    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void expiration() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        coll.mutateIn(docId,
                Arrays.asList(MutateInSpec.insert("foo2", "bar2")),
                MutateInOptions.mutateInOptions().expiry(Duration.ofSeconds(10)));

        GetResult result = coll.get(docId, GetOptions.getOptions().withExpiry(true));
        assertTrue(result.expiry().isPresent());
        assertTrue(result.expiry().get().getSeconds() != 0);
    }


    @Test
    void moreThan16() {
        JsonObject content = JsonObject.create().put("hello", "world");
        String docId = prepare(content);

        assertThrows(InvalidArgumentException.class, () ->
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
    void twoCommandsSucceed() {
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
    void twoCommandsOneFails() {
        JsonObject content = JsonObject.create()
          .put("foo1", "bar_orig_1")
          .put("foo2", "bar_orig_2");
        String docId = prepare(content);

        assertThrows(PathExistsException.class, () ->
                coll.mutateIn(docId,
                        Arrays.asList(MutateInSpec.insert("foo0", "bar0"),
                                MutateInSpec.insert("foo1", "bar1"),
                                MutateInSpec.remove("foo3"))));

        JsonObject updated = getContent(docId);
        assertEquals("bar_orig_1", updated.getString("foo1"));
    }

    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    @Test
    public void multipleXattrKeysShouldFail() {
        String docId = docId();
        assertThrows(
          XattrInvalidKeyComboException.class,
          () -> coll.mutateIn(
            docId,
            Arrays.asList(
              MutateInSpec.increment("count", 1).xattr().createPath(),
              MutateInSpec.arrayAppend("logs", Collections.singletonList("someValue")),
              upsert("logs[-1].c", MutateInMacro.CAS).xattr()
            ),
            MutateInOptions.mutateInOptions().storeSemantics(StoreSemantics.UPSERT))
        );
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void createAsDeletedCanAccess() {
      String docId = docId();

      try {
        coll.mutateIn(docId,
                Collections.singletonList(MutateInSpec.insert("foo", "bar").xattr()),
                MutateInOptions.mutateInOptions().createAsDeleted(true).storeSemantics(StoreSemantics.INSERT));

        assertThrows(DocumentNotFoundException.class, () -> coll.get(docId));
        assertThrows(DocumentNotFoundException.class, () ->
                coll.lookupIn(docId, Collections.singletonList(LookupInSpec.get("foo").xattr())));

        LookupInResult result = coll.lookupIn(docId,
                Collections.singletonList(LookupInSpec.get("foo").xattr()),
                LookupInOptions.lookupInOptions().accessDeleted(true));

        assertEquals("bar", result.contentAs(0, String.class));
      } catch (CouchbaseException err) {
        // createAsDeleted flag only supported in 6.5.1+
        assertEquals(ResponseStatus.INVALID_REQUEST, err.context().responseStatus());
      }
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void createAsDeletedCanInsertOnTop() {
      String docId = docId();

      try {
        coll.mutateIn(docId,
                Collections.singletonList(MutateInSpec.insert("foo", "bar").xattr()),
                MutateInOptions.mutateInOptions().createAsDeleted(true).storeSemantics(StoreSemantics.INSERT));

        coll.mutateIn(docId,
                Collections.singletonList(MutateInSpec.insert("foo", "bar").xattr()),
                MutateInOptions.mutateInOptions().storeSemantics(StoreSemantics.INSERT));

        LookupInResult result = coll.lookupIn(docId,
                Collections.singletonList(LookupInSpec.get("foo").xattr()));

        assertEquals("bar", result.contentAs(0, String.class));
      } catch (CouchbaseException err) {
        // createAsDeleted flag only supported in 6.5.1+
        assertEquals(ResponseStatus.INVALID_REQUEST, err.context().responseStatus());
      }
    }

    @Test
    @IgnoreWhen(clusterTypes = {ClusterType.MOCKED})
    void createAsDeletedMustCombineWithStoreSemantics() {
      String docId = docId();

      try {
        coll.mutateIn(docId,
                Collections.singletonList(MutateInSpec.insert("foo", "bar").xattr()),
                MutateInOptions.mutateInOptions().createAsDeleted(true));
      } catch (DocumentNotFoundException err) {
        // Expected response on 6.5.1+
      } catch (CouchbaseException err) {
        // createAsDeleted flag only supported in 6.5.1+
        assertEquals(ResponseStatus.INVALID_REQUEST, err.context().responseStatus());
      }
    }

    // JCBC-1600
    @Test
    void expiryWithDocumentFlagsShouldNotFail() {
      String docId = docId();

      coll.mutateIn(docId, Arrays.asList(
              upsert("a", "b"),
              upsert("c", "d")
              ),
              mutateInOptions().storeSemantics(StoreSemantics.UPSERT).expiry(Duration.ofSeconds(60 * 60 * 24)));
    }
}
