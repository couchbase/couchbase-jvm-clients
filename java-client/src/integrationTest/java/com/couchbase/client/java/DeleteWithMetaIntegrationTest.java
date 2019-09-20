/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.msg.kv.DeleteWithMetaRequest;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.DeleteWithMetaAccessor;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This integration test makes sure the DeleteWithMeta commands work as expected
 *
 * @since 3.0.0
 */
@IgnoreWhen(clusterTypes = { ClusterType.MOCKED })
class DeleteWithMetaIntegrationTest extends JavaIntegrationTest {

    static private Cluster cluster;
    static private ClusterEnvironment environment;
    static private Collection collection;
    static private boolean isLastWriteWinsBucket;

    @BeforeAll
    static void beforeAll() {
        cluster = Cluster.connect(connectionString(), clusterOptions());
        Bucket bucket = cluster.bucket(config().bucketname());
        isLastWriteWinsBucket = false;
        collection = bucket.defaultCollection();
    }

    @AfterAll
    static void afterAll() {
        cluster.disconnect();
    }

    /**
     * This functionality is only used by transactions currently, so a true API to write an xattrs key is not exposed.
     */
    ByteBuf createXattrsBody(String key, String content) {
        ByteBuf keyPair = Unpooled.buffer();
        keyPair.writeBytes(key.getBytes(StandardCharsets.UTF_8));
        keyPair.writeByte(0);
        keyPair.writeBytes(content.getBytes(StandardCharsets.UTF_8));
        keyPair.writeByte(0);

        ByteBuf bytes = Unpooled.buffer();
        bytes.writeInt(keyPair.readableBytes() + 4); // total length
        bytes.writeInt(keyPair.readableBytes()); // length of next xattr pair
        bytes.writeBytes(keyPair);

        return bytes;
    }

    @Test
    void withXattrsSucceeds() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();

        String doc = "{\"hello\":\"world\"}";

        ByteBuf bytes = createXattrsBody("txn", doc);

        byte[] raw = new byte[bytes.readableBytes()];
        bytes.readBytes(raw);

        DeleteWithMetaRequest request = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                false, isLastWriteWinsBucket);

        DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request, id).get();


        LookupInResult result = collection.lookupIn(id,
                Arrays.asList(LookupInSpec.get("txn").xattr()),
                LookupInOptions.lookupInOptions().accessDeleted(true));

//        assertEquals(2, result.cas());
        assertEquals(result.contentAsObject(0).toString(), doc);

        LookupInResult specificField = collection.lookupIn(id,
                Arrays.asList(LookupInSpec.get("txn.hello").xattr()),
                LookupInOptions.lookupInOptions().accessDeleted(true));

        assertEquals("world", specificField.contentAs(0, String.class));
    }

    @Test
    void canOverrideDoc() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();

        String doc = "{\"hello\":\"world\"}";

        ByteBuf bytes = createXattrsBody("txn", doc);

        byte[] raw = new byte[bytes.readableBytes()];
        bytes.readBytes(raw);

        DeleteWithMetaRequest request = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                true, isLastWriteWinsBucket);

        DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request, id).get();

        assertThrows(DocumentNotFoundException.class, () -> collection.get(id));

        // Turn it into a regular doc
        collection.upsert(id, JsonObject.create());

        collection.get(id);

        assertThrows(PathNotFoundException.class, () ->
                collection.lookupIn(id,
                        Arrays.asList(LookupInSpec.get("txn").xattr()),
                        LookupInOptions.lookupInOptions()));

    }

    // Conflict detection is performed, but because we set the seqnum and CAS to a hardcoded value, the second write
    // is rejected with KEY_EXISTS
    @Test
    void detectKeyAlreadyExists() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();

        String doc = "{\"hello\":\"world\"}";

        ByteBuf bytes = createXattrsBody("txn", doc);

        byte[] raw = new byte[bytes.readableBytes()];
        bytes.readBytes(raw);

        DeleteWithMetaRequest request = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                true, isLastWriteWinsBucket);

        DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request, id).get();

        DeleteWithMetaRequest request2 = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                // Don't regenerate CAS as it also disables conflict resolution, then we don't get KEY_EXISTS
                false, isLastWriteWinsBucket);

        try {
            DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request2, id).get();
            fail();
        } catch (ExecutionException err) {
            assertTrue(err.getCause() instanceof DocumentExistsException);
        }
    }

    @Test
    void detectKeyAlreadyExistsNoData() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();

        byte[] raw = new byte[]{};

        DeleteWithMetaRequest request = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                true, isLastWriteWinsBucket);

        DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request, id).get();

        DeleteWithMetaRequest request2 = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                false, isLastWriteWinsBucket);

        try {
            DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request2, id).get();
            fail();
        } catch (ExecutionException err) {
            assertTrue(err.getCause() instanceof DocumentExistsException);
        }
    }


    @Test
    void withoutXattrsSucceeds() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();

        byte[] raw = new byte[]{};

        DeleteWithMetaRequest request = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                false, isLastWriteWinsBucket);

        DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request, id).get();
    }

    @Test
    void mutateInAccessDeleted() throws ExecutionException, InterruptedException {
        String id = UUID.randomUUID().toString();

        String doc = "{\"hello\":\"world\"}";

        ByteBuf bytes = createXattrsBody("txn", doc);

        byte[] raw = new byte[bytes.readableBytes()];
        bytes.readBytes(raw);

        DeleteWithMetaRequest request = DeleteWithMetaAccessor.deleteWithMetaRequest(collection,
                id,
                raw,
                cluster.environment().timeoutConfig().kvTimeout(),
                cluster.environment().retryStrategy(),
                0, 2, 0,
                false, isLastWriteWinsBucket);

        DeleteWithMetaAccessor.deleteWithMeta(cluster.core(), request, id).get();

        collection.mutateIn(id,
                Arrays.asList(MutateInSpec.upsert("txn.hello", "mars").xattr()),
                MutateInOptions.mutateInOptions().accessDeleted(true));

        LookupInResult specificField = collection.lookupIn(id,
                Arrays.asList(LookupInSpec.get("txn.hello").xattr()),
                LookupInOptions.lookupInOptions().accessDeleted(true));

        assertEquals("mars", specificField.contentAs(0, String.class));
    }
}
