/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin

import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.client.core.error.CasMismatchException
import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.DocumentUnretrievableException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.TimeoutException
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException
import com.couchbase.client.core.error.subdoc.PathMismatchException
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.couchbase.client.kotlin.kv.Durability.Companion.clientVerified
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.Expiry.Companion.of
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.MutateInMacro
import com.couchbase.client.kotlin.kv.MutateInSpec
import com.couchbase.client.kotlin.kv.PersistTo
import com.couchbase.client.kotlin.kv.ReplicateTo
import com.couchbase.client.kotlin.kv.StoreSemantics.Companion.insert
import com.couchbase.client.kotlin.kv.StoreSemantics.Companion.replace
import com.couchbase.client.kotlin.kv.StoreSemantics.Companion.upsert
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.ClusterType.CAVES
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.math.min
import kotlin.system.measureNanoTime
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import java.time.temporal.ChronoUnit.DAYS as ChronoDays
import java.time.temporal.ChronoUnit.SECONDS as ChronoSeconds

// Prevent timeouts against the Mock from interfering
// with Gerrit verification.  Remove when mock is fixed:
//    http://review.couchbase.org/c/CouchbaseMock/+/148081
@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class KeyValueIntegrationTest : KotlinIntegrationTest() {

    val nearFutureExpiry = Expiry.of(Instant.now().plus(3, ChronoDays).truncatedTo(ChronoSeconds))

    val availableReplicas by lazy {
        runBlocking {
            val bucketConfig = cluster
                .bucket(config().bucketname())
                .config(30.seconds) as CouchbaseBucketConfig

            min(bucketConfig.nodes().size - 1, bucketConfig.numberOfReplicas())
        }
    }

    @Nested
    inner class GetAllReplicas {
        @Test
        fun `flow is empty when not found`(): Unit = runBlocking {
            val flow = collection.getAllReplicas(ABSENT_ID)
            assertThat(flow.toList()).isEmpty()
        }

        @Test
        fun `flow is cold`(): Unit = runBlocking {
            val id = nextId()
            val flow = collection.getAllReplicas(id)

            assertThat(flow.toList()).isEmpty()

            collection.upsert(id, "foo")
            assertThat(flow.toList()).isNotEmpty()
        }

        @Test
        fun `returns primary and replicas`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(
                id, "foo",
                durability = clientVerified(PersistTo.NONE, ReplicateTo.replicas(availableReplicas))
            )

            val result = collection.getAllReplicas(id).toList()

            result.forEach {
                assertEquals(id, it.id)
                assertEquals("foo", it.contentAs<String>())
            }

            assertThat(result).hasSize(availableReplicas + 1)
            assertThat(result.filter { !it.replica }).hasSize(1)
        }
    }

    @Nested
    inner class GetAnyReplica {
        @Test
        fun `throws DocumentUnretrievableException`(): Unit = runBlocking {
            assertThrows<DocumentUnretrievableException> { collection.getAnyReplica(ABSENT_ID) }
        }

        @Test
        fun `returns result`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            assertEquals(id, collection.getAnyReplica(id).id)
            assertEquals("foo", collection.getAnyReplica(id).contentAs<String>())
        }
    }

    @Nested
    inner class GetAnyReplicaOrNull {
        @Test
        fun `returns null`(): Unit = runBlocking {
            assertNull(collection.getAnyReplicaOrNull(ABSENT_ID))
        }

        @Test
        fun `returns result`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            assertEquals(id, collection.getAnyReplicaOrNull(id)?.id)
            assertEquals("foo", collection.getAnyReplicaOrNull(id)?.contentAs<String>())
        }
    }

    @Nested
    inner class Get {
        @Test
        fun `throws DocumentNotFoundException`(): Unit = runBlocking {
            assertThrows<DocumentNotFoundException> { collection.get(ABSENT_ID) }
        }

        @Test
        fun `json string content`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            assertEquals("foo", collection.get(id).contentAs<String>())
        }

        @Test
        fun `plain string content`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, Content.string("foo"))
            assertEquals("foo", collection.get(id).content.bytes.toStringUtf8())
        }

        @Test
        fun `binary content`(): Unit = runBlocking {
            val id = nextId()
            val contentBytes = "xyzzy".toByteArray()

            collection.upsert(id, Content.binary(contentBytes))
            assertArrayEquals(contentBytes, collection.get(id).content.bytes)
        }

        @Test
        fun `binary content with and without expiry`(): Unit = runBlocking {
            val id = nextId()
            val contentBytes = "xyzzy".toByteArray()

            collection.upsert(id, Content.binary(contentBytes), expiry = nearFutureExpiry)
            val result = collection.get(id, withExpiry = true)
            assertArrayEquals(contentBytes, result.content.bytes)
            assertEquals(nearFutureExpiry, result.expiry)

            collection.upsert(id, Content.binary(contentBytes))
            assertEquals(Expiry.none(), collection.get(id, withExpiry = true).expiry)
            assertEquals(Expiry.Unknown, collection.get(id).expiry)
        }

        @Test
        fun `with and without expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            assertEquals(Expiry.Unknown, collection.get(id).expiry)
            assertEquals(Expiry.none(), collection.get(id, withExpiry = true).expiry)

            collection.upsert(id, "foo", expiry = nearFutureExpiry)
            assertEquals(nearFutureExpiry, collection.get(id, withExpiry = true).expiry)
        }

        @Test
        fun `with projections`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(
                id, mapOf(
                    "numbers" to mapOf(
                        "one" to 1,
                        "two" to 2,
                    ),
                    "fruit" to "apple",
                )
            )

            val expected = mapOf(
                "numbers" to mapOf(
                    "one" to 1,
                )
            )

            val path = "numbers.one"
            // repeated, absent, and mismatched paths should all have no effect
            val harmlessGarbage = listOf(path, "no-such-path", "numbers[0]")
            val result = collection.get(id, project = listOf(path) + harmlessGarbage)
            assertEquals(expected, result.contentAs<Any>())
        }

        @Test
        @IgnoreWhen(clusterTypes = [CAVES])
        fun `projection fails when document is too deep`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, deeplyNested(128, "foo"))
            assertThrows<DocumentTooDeepException> { collection.get(id, project = listOf("foo")) }
        }

        @Test
        @IgnoreWhen(clusterTypes = [CAVES])
        fun `projection fails when document is binary`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, Content.binary("xyzzy".toByteArray()))
            assertThrows<DocumentNotJsonException> { collection.get(id, project = listOf("foo")) }
        }

        @Test
        fun `with projections and expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar", "x" to "y"), expiry = nearFutureExpiry)

            collection.get(id, withExpiry = true, project = listOf("foo")).let {
                assertEquals(mapOf("foo" to "bar"), it.contentAs<Any>())
                assertEquals(nearFutureExpiry, it.expiry)
            }
        }

        @Test
        fun `with projections mismatch`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar", "x" to "y"), expiry = nearFutureExpiry)

            assertEquals(
                mapOf("foo" to "bar"),
                collection.get(id, project = listOf("foo", "foo[0]")).contentAs<Any>(),
            )

            assertEquals(
                mapOf("foo" to "bar"),
                collection.get(id, project = listOf("foo", "absent")).contentAs<Any>(),
            )

            assertEquals(
                mapOf("foo" to "bar"),
                collection.get(id, project = listOf("foo", "foo[0]", "absent")).contentAs<Any>(),
            )
        }
    }

    @Nested
    inner class GetOrNull {
        @Test
        fun `returns null for absent document`(): Unit = runBlocking {
            assertNull(collection.getOrNull(ABSENT_ID)?.contentAs<String>())
        }

        @Test
        fun `returns present document`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            assertEquals("foo", collection.getOrNull(id)?.contentAs<String>())
        }
    }

    @Nested
    inner class GetAndLock {
        @Test
        fun `throws DocumentNotFoundException`(): Unit = runBlocking {
            assertThrows<DocumentNotFoundException> {
                collection.getAndLock("this document does not exist", 5.seconds)
            }
        }

        @Test
        fun `throws TimeoutException when can't acquire lock`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            val cas = collection.getAndLock(id, 15.seconds).cas
            assertNotEquals(0, cas)

            assertThrows<TimeoutException> {
                collection.getAndLock(
                    id, 1.seconds,
                    common = CommonOptions(timeout = 3.seconds),
                )
            }
        }

        @Test
        fun `competing write times out`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            collection.getAndLock(id, 15.seconds).cas
            assertThrows<TimeoutException> { collection.upsert(id, "bar") }
        }

        @Test
        fun `can replace if cas is known`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            val cas = collection.getAndLock(id, 15.seconds).cas
            collection.replace(id, "bar", cas = cas)
            assertEquals("bar", collection.get(id).contentAs<String>())
        }

        @Test
        fun `locking changes cas`(): Unit = runBlocking {
            val id = nextId()
            val upsertCas = collection.upsert(id, "foo").cas
            val cas = collection.getAndLock(id, 15.seconds).cas
            assertNotEquals(upsertCas, cas)
        }

        @Test
        fun `lock expires`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            val lockTime = 3.seconds
            val lockWaitNanos = measureNanoTime {
                collection.getAndLock(id, lockTime).cas
                collection.getAndLock(
                    id, 1.seconds,
                    common = CommonOptions(timeout = 30.seconds)
                )
            }

            assertThat(lockWaitNanos).isGreaterThanOrEqualTo(lockTime.inWholeNanoseconds)
        }
    }

    @Nested
    inner class Unlock {
        @Test
        fun `releases lock`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            val lockTime = 25.seconds
            val cas = collection.getAndLock(id, lockTime).cas
            collection.unlock(id, cas)
            collection.getAndLock(id, 1.seconds)
        }

        @Test
        fun `throws DocumentNotFoundException`(): Unit = runBlocking {
            assertThrows<DocumentNotFoundException> { collection.unlock(ABSENT_ID, 123) }
        }

        @Test
        fun `throws InvalidArgumentException on zero cas`(): Unit = runBlocking {
            assertThrows<InvalidArgumentException> { collection.unlock("foo", 0) }
        }
    }

    @Nested
    inner class GetAndTouch {
        @Test
        fun `throws DocumentNotFoundException`(): Unit = runBlocking {
            assertThrows<DocumentNotFoundException> {
                collection.getAndTouch("does not exist", Expiry.none())
            }
        }

        @Test
        fun `sets expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")

            collection.getAndTouch(id, nearFutureExpiry).let {
                assertEquals("foo", it.contentAs<String>())
                assertEquals(nearFutureExpiry, it.expiry)
            }
            assertExpiry(nearFutureExpiry, id)
        }

        @Test
        fun `overrides existing expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo", expiry = of(15.minutes))

            collection.getAndTouch(id, nearFutureExpiry).let {
                assertEquals("foo", it.contentAs<String>())
                assertEquals(nearFutureExpiry, it.expiry)
            }
            assertExpiry(nearFutureExpiry, id)
        }

        @Test
        fun `can remove expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo", expiry = nearFutureExpiry)

            collection.getAndTouch(id, Expiry.none()).let {
                assertEquals("foo", it.contentAs<String>())
                assertEquals(Expiry.none(), it.expiry)
            }
            assertExpiry(Expiry.none(), id)
        }

        @Test
        fun `changes cas`(): Unit = runBlocking {
            val id = nextId()
            val upsertCas = collection.upsert(id, "foo").cas
            val touchCas = collection.getAndTouch(id, nearFutureExpiry)
            assertNotEquals(0, touchCas)
            assertNotEquals(upsertCas, touchCas)
        }
    }

    @Nested
    inner class Touch {
        @Test
        fun `throws DocumentNotFoundException`(): Unit = runBlocking {
            assertThrows<DocumentNotFoundException> { collection.touch(ABSENT_ID, Expiry.none()) }
        }

        @Test
        fun `sets expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            collection.touch(id, nearFutureExpiry)
            assertExpiry(nearFutureExpiry, id)
        }

        @Test
        fun `overrides existing expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo", expiry = of(15.minutes))
            collection.touch(id, nearFutureExpiry)
            assertExpiry(nearFutureExpiry, id)
        }

        @Test
        fun `can remove expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo", expiry = nearFutureExpiry)
            collection.touch(id, Expiry.none())
            assertExpiry(Expiry.none(), id)
        }

        @Test
        fun `changes cas`(): Unit = runBlocking {
            val id = nextId()
            val upsertCas = collection.upsert(id, "foo").cas
            val touchCas = collection.touch(id, nearFutureExpiry)
            assertNotEquals(0, touchCas)
            assertNotEquals(upsertCas, touchCas)
        }
    }


    @Nested
    inner class Insert {
        @Test
        fun `throws DocumentExistsException`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            assertThrows<DocumentExistsException> { collection.insert(id, "bar") }
        }

        @Test
        fun `basic insertion`(): Unit = runBlocking {
            val id = nextId()
            collection.insert(id, "foo")
            assertEquals("foo", collection.get(id).contentAs<String>())
        }

        @Test
        fun `with expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.insert(id, "foo", expiry = nearFutureExpiry)
            assertExpiry(nearFutureExpiry, id)
            assertEquals("foo", collection.get(id).contentAs<String>())
        }

        @Test
        fun `with client verified durability`(): Unit = runBlocking {
            val id = nextId()
            val content = "some value"
            collection.insert(id, content, durability = clientVerified(PersistTo.ACTIVE))
            assertEquals(content, collection.get(id).contentAs<String>())
        }
    }

    @Nested
    inner class Upsert {
        @Test
        fun `with client verified durability`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo", durability = clientVerified(PersistTo.ACTIVE))
            assertEquals("foo", collection.get(id).contentAs<String>())
        }

        @Test
        fun `upserting CommonOptions gives nice error message`(): Unit = runBlocking {
            val t = assertThrows<IllegalArgumentException> { collection.upsert("foo", CommonOptions()) }
            assertThat(t.message).startsWith("Expected document content")
        }

        @Test
        fun `can insert and update`(): Unit = runBlocking {
            val id = nextId()
            val insertResult = collection.upsert(id, mapOf("foo" to true))
            assertNotEquals(0, insertResult.cas)
            assertNotNull(insertResult.mutationToken)
            assertEquals(mapOf("foo" to true), collection.get(id).contentAs<Any>())

            val updateResult = collection.upsert(id, mapOf("foo" to false))

            assertNotEquals(0, updateResult.cas)
            assertNotNull(updateResult.mutationToken)

            assertNotEquals(insertResult.cas, updateResult.cas)
            assertNotEquals(insertResult.mutationToken, updateResult.mutationToken)
            assertEquals(mapOf("foo" to false), collection.get(id).contentAs<Any>())
        }

        @Test
        fun `unknown expiry is illegal argument`(): Unit = runBlocking {
            val e = assertThrows<IllegalArgumentException> {
                collection.upsert(nextId(), "foo", expiry = Expiry.Unknown)
            }
            assertTrue((e.message?:"").contains("Expiry.Unknown"))
        }
    }

    @Nested
    inner class Replace {
        @Test
        fun `throws DocumentNotFoundException`(): Unit = runBlocking {
            assertThrows<DocumentNotFoundException> { collection.replace(ABSENT_ID, "foo") }
        }

        @Test
        fun `without cas`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            collection.replace(id, "bar")
            assertEquals("bar", collection.get(id).contentAs<String>())
        }

        @Test
        fun `with cas`(): Unit = runBlocking {
            val id = nextId()
            val cas = collection.upsert(id, "foo").cas
            collection.replace(id, "bar", cas = cas)
            assertEquals("bar", collection.get(id).contentAs<String>())
        }

        @Test
        fun `with expiry`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            collection.replace(id, "bar", expiry = nearFutureExpiry)
            assertExpiry(nearFutureExpiry, id)
        }

        @Test
        fun `throws CasMismatchException`(): Unit = runBlocking {
            val id = nextId()
            val cas = collection.upsert(id, "foo").cas

            assertThrows<CasMismatchException> {
                collection.replace(id, "bar", cas = cas + 1)
            }
        }
    }

    @Nested
    inner class Remove {
        @Test
        fun `throws DocumentNotFoundException`(): Unit = runBlocking {
            assertThrows<DocumentNotFoundException> { collection.remove(ABSENT_ID) }
        }

        @Test
        fun `without cas`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            collection.remove(id)
            assertFalse(collection.exists(id).exists)
        }

        @Test
        fun `with cas`(): Unit = runBlocking {
            val id = nextId()
            val cas = collection.upsert(id, "foo").cas
            collection.remove(id, cas = cas)
            assertFalse(collection.exists(id).exists)
        }

        @Test
        fun `throws CasMismatchException`(): Unit = runBlocking {
            val id = nextId()
            val cas = collection.upsert(id, "foo").cas

            assertThrows<CasMismatchException> {
                collection.remove(id, cas = cas + 1)
            }
        }
    }

    @Nested
    inner class Exists {
        @Test
        fun `absent does not exist`(): Unit = runBlocking {
            collection.exists(ABSENT_ID).let {
                assertFalse(it.exists)
                assertEquals(0, it.cas)
            }
        }

        @Test
        fun `present exists`(): Unit = runBlocking {
            val id = nextId()
            val expectedCas = collection.upsert(id, "foo").cas
            collection.exists(id).let {
                assertTrue(it.exists)
                assertEquals(expectedCas, it.cas)
            }
        }

        @Test
        fun `recently deleted does not exist`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, "foo")
            collection.remove(id)
            collection.exists(id).let {
                assertFalse(it.exists)
                assertEquals(0, it.cas)
            }
        }
    }

    @Nested
    inner class MutateIn {
        @Test
        fun `insert succeeds if document does not already exist`(): Unit = runBlocking {
            val id = nextId()

            val spec = MutateInSpec()
            spec.upsert("foo", "bar")
            collection.mutateIn(id, spec, storeSemantics = insert())

            assertEquals(mapOf("foo" to "bar"), collection.get(id).contentAs<Any>())
        }

        @Test
        fun `insert throws DocumentExistsException`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf<String, Any?>())

            val spec = MutateInSpec().apply { upsert("foo", "bar") }
            assertThrows<DocumentExistsException> {
                collection.mutateIn(id, spec, storeSemantics = insert())
            }
        }

        @Test
        fun `replace throws DocumentNotFoundException`(): Unit = runBlocking {
            val id = nextId()
            val spec = MutateInSpec().apply { upsert("foo", "bar") }
            assertThrows<DocumentNotFoundException> {
                collection.mutateIn(id, spec, storeSemantics = replace())
            }
        }

        @Test
        fun `replace fails on path mismatch`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, listOf<Any>())
            val spec = MutateInSpec().apply { upsert("foo", "bar") }
            assertThrows<PathMismatchException> { collection.mutateIn(id, spec) }
        }

        @Test
        fun `replace throws CasMismatchException`(): Unit = runBlocking {
            val id = nextId()
            val cas = collection.upsert(id, mapOf<String, Any?>()).cas
            val spec = MutateInSpec().apply { upsert("foo", "bar") }
            assertThrows<CasMismatchException> {
                collection.mutateIn(id, spec, storeSemantics = replace(cas + 1))
            }
        }

        @Test
        fun `upsert does not care if document already exists`(): Unit = runBlocking {
            val id = nextId()

            run {
                val spec = MutateInSpec().apply { upsert("foo", "bar") }
                collection.mutateIn(id, spec, storeSemantics = upsert())
                assertEquals(mapOf("foo" to "bar"), collection.get(id).contentAs<Any>())
            }

            run {
                val spec = MutateInSpec().apply { upsert("one", 1) }
                collection.mutateIn(id, spec, storeSemantics = upsert())
                assertEquals(mapOf("foo" to "bar", "one" to 1), collection.get(id).contentAs<Any>())
            }
        }

        @Test
        @IgnoreWhen(clusterTypes = [CAVES])
        fun `can upsert macro`(): Unit = runBlocking {
            val id = nextId()

            val spec = MutateInSpec().apply { upsert("foo", MutateInMacro.SeqNo) }
            val seqno = collection.mutateIn(id, spec, storeSemantics = upsert())
                .mutationToken!!.sequenceNumber()

            val lookupSpec = object : LookupInSpec() {
                val foo = get("foo", xattr = true)
            }
            collection.lookupIn(id, lookupSpec) {
                assertEquals("0x%016x".format(seqno), lookupSpec.foo.contentAs<String>())
            }
        }
    }

    private suspend fun assertExpiry(expiry: Expiry, id: String) {
        assertEquals(expiry, collection.get(id, withExpiry = true).expiry)
    }
}

internal fun deeplyNested(depth: Int, fieldName: String = "x"): Map<String, Any?> {
    return if (depth == 0) mapOf() else mapOf(fieldName to deeplyNested(depth - 1))
}

internal const val ABSENT_ID = "this document does not exist"
