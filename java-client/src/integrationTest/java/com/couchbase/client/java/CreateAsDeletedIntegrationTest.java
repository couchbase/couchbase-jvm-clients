/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.StoreSemantics;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test the CreateAsDeleted flag added with MB-37374 in 6.6.
 */
@IgnoreWhen(missesCapabilities = Capabilities.CREATE_AS_DELETED,
  isProtostellarWillWorkLater = true
)
class CreateAsDeletedIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Collection coll;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    coll = bucket.defaultCollection();
    // no waitUntilReady check, to (sometimes) trigger the bucketConfig null check logic in `mutateInRequest`
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }


  private String docId() {
    return UUID.randomUUID().toString();
  }

  private static LookupInResult assertIsTombstone(Collection coll, String id) {
    assertThrows(DocumentNotFoundException.class, () -> coll.get(id));

    LookupInResult result = coll.lookupIn(id, Arrays.asList(
            LookupInSpec.get("txn").xattr(),
            LookupInSpec.get("")
    ), LookupInOptions.lookupInOptions().accessDeleted(true));

    assertTrue(result.isDeleted());

    return result;
  }

  private static LookupInResult assertIsRegularDoc(Collection coll, String id) {
    coll.get(id);

    LookupInResult result = coll.lookupIn(id, Arrays.asList(
            LookupInSpec.get("txn").xattr(),
            LookupInSpec.get("")
    ), LookupInOptions.lookupInOptions());

    assertFalse(result.isDeleted());

    return result;
  }

  private static void insertTombstoneWithTxnXattr(Collection coll, String id, JsonObject txnXattr) {
    coll.mutateIn(id, Arrays.asList(
            MutateInSpec.insert("txn.id.txn", UUID.randomUUID().toString()).xattr().createPath(),
            MutateInSpec.insert("txn.type", "insert").xattr()
            ),
            MutateInOptions.mutateInOptions()
                    .storeSemantics(StoreSemantics.INSERT)
                    .accessDeleted(true)
                    .createAsDeleted(true)
                    .cas(0)
                    .durability(DurabilityLevel.MAJORITY));
    assertIsTombstone(coll, id);
  }

  private static void upsertTombstoneWithTxnXattr(Collection coll, String id, JsonObject txnXattr) {
    coll.mutateIn(id, Arrays.asList(
            MutateInSpec.insert("txn", txnXattr).xattr()
            ),
            MutateInOptions.mutateInOptions()
                    .storeSemantics(StoreSemantics.UPSERT)
                    .accessDeleted(true)
                    .createAsDeleted(true));
    assertIsTombstone(coll, id);
  }

  private static void cleanupTombstone(Collection coll, String id) {
    coll.mutateIn(id, Collections.singletonList(
            MutateInSpec.remove("txn").xattr()
            ),
            MutateInOptions.mutateInOptions().accessDeleted(true));
  }

  private static void upsertEmptyTombstone(Collection coll, String id) {
    coll.mutateIn(id, Arrays.asList(
            MutateInSpec.upsert("dummy", JsonObject.create()).xattr()
            ),
            MutateInOptions.mutateInOptions()
                    .storeSemantics(StoreSemantics.INSERT)
                    .createAsDeleted(true));
  }

  private static void createRegularDoc(Collection coll, String id, JsonObject body) {
    coll.upsert(id, body);
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void insertTombstone() {
    String id = "test";
    insertTombstoneWithTxnXattr(coll, id, JsonObject.create());
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void getRegularTombstone() {
    String id = docId();
    coll.upsert(id, JsonObject.create());
    coll.remove(id);

    LookupInResult result = coll
            .lookupIn(id, Arrays.asList(
                    LookupInSpec.get("txn.atr.id").xattr(),
                    LookupInSpec.get("")
            ), LookupInOptions.lookupInOptions()
                    .accessDeleted(true));

    assertFalse(result.exists(0));
    assertFalse(result.exists(1));
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void insertTombstoneOverTombstoneWithTxnXattr() {
    String id = docId();
    insertTombstoneWithTxnXattr(coll, id, JsonObject.create());
    assertThrows(DocumentExistsException.class, () ->
            insertTombstoneWithTxnXattr(coll, id, JsonObject.create()));
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void insertTombstoneOverTombstoneWithoutTxnXattr() {
    String id = docId();
    upsertEmptyTombstone(coll, id);
    assertThrows(DocumentExistsException.class, () ->
      insertTombstoneWithTxnXattr(coll, id, JsonObject.create()));
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void insertTombstoneOverExistingDoc() {
    String id = docId();
    createRegularDoc(coll, id, JsonObject.create());
    assertThrows(DocumentExistsException.class, () ->
            insertTombstoneWithTxnXattr(coll, id, JsonObject.create()));
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void replaceTombstoneWithTxnXattrWithCAS() {
    String id = docId();
    insertTombstoneWithTxnXattr(coll, id, JsonObject.create());

    LookupInResult result = assertIsTombstone(coll, id);

    coll.mutateIn(id, Collections.singletonList(
            MutateInSpec.upsert("txn", JsonObject.create()).xattr()
            ),
            MutateInOptions.mutateInOptions()
                    .storeSemantics(StoreSemantics.REPLACE)
                    .cas(result.cas())
                    .accessDeleted(true));
  }

  // See comment in replaceTombstoneWithTxnXattrWithCASWhichHasChanged_71Plus for why we split this test.
  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES, hasCapabilities = {Capabilities.SUBDOC_REVIVE_DOCUMENT})
  void replaceTombstoneWithTxnXattrWithCASWhichHasChanged_pre71() {
    String id = docId();
    insertTombstoneWithTxnXattr(coll, id, JsonObject.create());

    LookupInResult result = assertIsTombstone(coll, id);

    upsertEmptyTombstone(coll, id);

    LookupInResult result2 = assertIsTombstone(coll, id);

    assertNotEquals(result.cas(), result2.cas());

    assertThrows(CasMismatchException.class, () -> {
      coll.mutateIn(id, Collections.singletonList(
              MutateInSpec.insert("txn", JsonObject.create()).xattr()
              ),
              MutateInOptions.mutateInOptions()
                      .cas(result.cas()) // note using the original CAS not the changed one
                      .accessDeleted(true));
    });
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES, missesCapabilities = {Capabilities.SUBDOC_REVIVE_DOCUMENT})
  void replaceTombstoneWithTxnXattrWithCASWhichHasChanged_71Plus() {
    String id = docId();
    insertTombstoneWithTxnXattr(coll, id, JsonObject.create());

    assertIsTombstone(coll, id);

    // Behaviour changed on 7.1: previously this would succeed.
    // Checking with KV team if intentional.
    assertThrows(DocumentExistsException.class, () -> {
      upsertEmptyTombstone(coll, id);
    });
  }

  @Test
  void removeTombstone() {
    String id = docId();
    upsertTombstoneWithTxnXattr(coll, id, JsonObject.create());
    assertThrows(DocumentNotFoundException.class, () -> coll.remove(id));
  }

  @Test
  void cleanupTombstone() {
    String id = docId();
    upsertTombstoneWithTxnXattr(coll, id, JsonObject.create());
    cleanupTombstone(coll, id);

    LookupInResult r = assertIsTombstone(coll, id);

    assertFalse(r.exists(0)); // txn xattr
  }

  @Test
  void replaceTombstone() {
    String id = docId();
    upsertTombstoneWithTxnXattr(coll, id, JsonObject.create());
    assertThrows(DocumentNotFoundException.class, () -> coll.replace(id, JsonObject.create()));
  }

  @Test
  @IgnoreWhen(clusterTypes = ClusterType.CAVES)
  void upsertTombstone() {
    String id = docId();
    upsertTombstoneWithTxnXattr(coll, id, JsonObject.create());

    coll.upsert(id, JsonObject.create());

    LookupInResult r = assertIsRegularDoc(coll, id);

    // Make sure the transaction xattr's in the tombstone have been wiped
    assertFalse(r.exists(0)); // txn xattr
    assertTrue(r.exists(1)); // body
  }

  @Test
  void requestOnNonExistentBucket() {
    String id = docId();
    Bucket doesNotExist = cluster.bucket("hokey_kokey");

    assertThrows(UnambiguousTimeoutException.class, () -> {
      doesNotExist.defaultCollection().mutateIn(id, Arrays.asList(
              MutateInSpec.insert("txn", JsonObject.create()).xattr()
              ),
              MutateInOptions.mutateInOptions()
                      .timeout(Duration.ofSeconds(1))
                      .storeSemantics(StoreSemantics.UPSERT)
                      .accessDeleted(true)
                      .createAsDeleted(true));
    });
  }

  @Test
  void requestOnNonExistentBucketReactive() {
    String id = docId();
    Bucket doesNotExist = cluster.bucket("hokey_kokey");

    assertThrows(UnambiguousTimeoutException.class, () -> {
      doesNotExist.defaultCollection().reactive().mutateIn(id, Arrays.asList(
              MutateInSpec.insert("txn", JsonObject.create()).xattr()
              ),
              MutateInOptions.mutateInOptions()
                      .timeout(Duration.ofSeconds(1))
                      .storeSemantics(StoreSemantics.UPSERT)
                      .accessDeleted(true)
                      .createAsDeleted(true))
              .block();
    });
  }
}
