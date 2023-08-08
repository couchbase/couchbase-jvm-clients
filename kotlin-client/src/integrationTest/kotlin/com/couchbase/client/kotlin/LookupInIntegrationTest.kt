package com.couchbase.client.kotlin

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.DocumentUnretrievableException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.subdoc.DocumentNotJsonException
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException
import com.couchbase.client.core.error.subdoc.PathInvalidException
import com.couchbase.client.core.error.subdoc.PathMismatchException
import com.couchbase.client.core.error.subdoc.PathTooDeepException
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.RawBinaryTranscoder
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.couchbase.client.kotlin.kv.LookupInReplicaResult
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.internal.LookupInMacro
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class LookupInIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `fails when spec is empty`(): Unit = runBlocking {
        val id = nextId()
        val emptySpec = object : LookupInSpec() {}
        assertThrows<InvalidArgumentException> { collection.lookupIn(id, emptySpec) }
    }

    @Test
    fun `throws when document is not json`(): Unit = runBlocking {
        val id = nextId()

        collection.upsert(id, "not json".toByteArray(), transcoder = RawBinaryTranscoder)

        val spec = object : LookupInSpec() {
            val foo = get("foo")
            val fooExists = exists("foo")
            val count = count("foo")
        }
        // Not a top-level failure, because the spec might include xattrs which *can* be accessed.
        collection.lookupIn(id, spec) {
            assertThrows<DocumentNotJsonException> { spec.fooExists.value }
            assertThrows<DocumentNotJsonException> { spec.foo.exists }
            assertThrows<DocumentNotJsonException> { spec.count.value }
            assertThrows<DocumentNotJsonException> { spec.foo.contentAsBytes }
        }
    }

    @Test
    fun `can reuse spec`(): Unit = runBlocking {
        val id = nextId()
        collection.upsert(id, mapOf("foo" to "bar"))

        val spec = object : LookupInSpec() {
            val foo = get("foo")
        }
        repeat(2) {
            collection.lookupIn(id, spec) {
                assertEquals("bar", spec.foo.contentAs<String>())
            }
        }
    }

    @Test
    fun `fails on spec mismatch`(): Unit = runBlocking {
        val id = nextId()
        collection.upsert(id, mapOf("foo" to "bar"))

        val spec = object : LookupInSpec() {
            val foo = get("foo")
        }

        val result = collection.lookupIn(id, spec)
        assertEquals("bar", spec.foo.contentAs<String>(result))

        val t = assertThrows<IllegalArgumentException> {
            val otherSpec = object : LookupInSpec() {
                val foo = get("foo")
            }
            otherSpec.foo.contentAs<String>(result)
        }
        assertThat(t).hasMessageContaining("not created from the same LookupInSpec")

    }

    @Test
    fun `throws DocumentNotFoundException when document is absent`(): Unit = runBlocking {
        val spec = object : LookupInSpec() {
            @Suppress("unused")
            val foo = get("foo")
        }
        assertThrows<DocumentNotFoundException> { collection.lookupIn(ABSENT_ID, spec) }
    }

    @Test
    fun `returns cas`(): Unit = runBlocking {
        val id = nextId()
        val cas = collection.upsert(id, mapOf("foo" to "bar")).cas

        val spec = object : LookupInSpec() {
            @Suppress("unused")
            val foo = get("foo")
        }
        assertEquals(cas, collection.lookupIn(id, spec).cas)
    }

    @Nested
    inner class Exists {
        @Test
        fun `returns true for existing field`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = object : LookupInSpec() {
                val fooExists = exists("foo")
                val foo = get("foo")
            }
            collection.lookupIn(id, spec) {
                assertTrue(spec.fooExists.value)
                assertTrue(spec.fooExists.get(this))
                assertTrue(spec.foo.exists)
                assertTrue(spec.foo.exists(this))
            }
        }

        @Test
        fun `returns false for absent field`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = object : LookupInSpec() {
                val nopeExists = exists("nope")
                val nope = get("nope")
            }
            collection.lookupIn(id, spec) {
                assertFalse(spec.nopeExists.value)
                assertFalse(spec.nopeExists.get(this))
                assertFalse(spec.nope.exists)
                assertFalse(spec.nope.exists(this))
            }
        }

        @Test
        @IgnoreWhen(clusterTypes = [ClusterType.CAVES])
        fun `throws when doc is too deep`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, deeplyNested(128, "foo"))

            val spec = object : LookupInSpec() {
                val fooExists = exists("foo")
                val foo = get("foo")
            }
            collection.lookupIn(id, spec) {
                assertThrows<DocumentTooDeepException> { spec.fooExists.value }
                assertThrows<DocumentTooDeepException> { spec.fooExists.get(this) }
                assertThrows<DocumentTooDeepException> { spec.foo.exists }
                assertThrows<DocumentTooDeepException> { spec.foo.exists(this) }
            }
        }

        @Test
        @IgnoreWhen(clusterTypes = [ClusterType.CAVES])
        fun `throws when path is too deep`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("magicWord" to "xyzzy"))

            val path = List(128) { _ -> "x" }.joinToString(".")

            val spec = object : LookupInSpec() {
                val fooExists = exists(path)
                val foo = get(path)
            }
            collection.lookupIn(id, spec) {
                assertThrows<PathTooDeepException> { spec.fooExists.value }
                assertThrows<PathTooDeepException> { spec.fooExists.get(this) }
                assertThrows<PathTooDeepException> { spec.foo.exists }
                assertThrows<PathTooDeepException> { spec.foo.exists(this) }
            }
        }

        @Test
        fun `returns false for path mismatch`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("magicWord" to "xyzzy"))

            val spec = object : LookupInSpec() {
                val nopeExists = exists("magicWord[0].foo")
                val nope = get("magicWord[0].foo")
            }
            collection.lookupIn(id, spec) {
                // Kotlin SDK diverges from spec; see https://issues.couchbase.com/browse/KCBC-119
                assertFalse(spec.nopeExists.value)
                assertFalse(spec.nopeExists.get(this))
                assertFalse(spec.nope.exists)
                assertFalse(spec.nope.exists(this))

                // sanity check, path is actually a mismatch
                assertThrows<PathMismatchException> { spec.nope.contentAsBytes }
            }
        }

        @Test
        @IgnoreWhen(clusterTypes = [ClusterType.CAVES])
        fun `throws when path is invalid`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("magicWord" to "xyzzy"))

            val path = "syntaxError["

            val spec = object : LookupInSpec() {
                val fooExists = exists(path)
                val foo = get(path)
            }
            collection.lookupIn(id, spec) {
                assertThrows<PathInvalidException> { spec.fooExists.value }
                assertThrows<PathInvalidException> { spec.fooExists.get(this) }
                assertThrows<PathInvalidException> { spec.foo.exists }
                assertThrows<PathInvalidException> { spec.foo.exists(this) }
            }
        }
    }

    @Nested
    inner class Get {
        @Test
        fun `can get xattr of deleted document`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))
            collection.remove(id)

            val spec = object : LookupInSpec() {
                val flags = get(LookupInMacro.Flags)
            }
            collection.lookupIn(id, spec, accessDeleted = true) {
                assertTrue(this.deleted)
                assertEquals(CodecFlags.JSON_COMMON_FLAGS, spec.flags.contentAs())
            }
        }

        @Test
        fun `can request xattr after document field`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = object : LookupInSpec() {
                val foo = get("foo")
                val flags = get(LookupInMacro.Flags)
            }
            collection.lookupIn(id, spec) {
                assertFalse(this.deleted)
                assertEquals(CodecFlags.JSON_COMPAT_FLAGS, spec.flags.contentAs<Int>())
                assertEquals("bar", spec.foo.contentAs<String>())
            }
        }

        @Test
        @IgnoreWhen(clusterTypes = [ClusterType.CAVES])
        fun `fails when doc is too deep`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, deeplyNested(128))

            val spec = object : LookupInSpec() {
                val foo = get("foo")
            }
            collection.lookupIn(id, spec) {
                assertThrows<DocumentTooDeepException> { spec.foo.contentAsBytes }
            }
        }

        @Test
        fun `can get binary document`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, Content.binary("xyzzy".toByteArray()))

            val spec = object : LookupInSpec() {
                val doc = get("")
            }
            collection.lookupIn(id, spec) { assertEquals("xyzzy", spec.doc.contentAsBytes.toStringUtf8()) }
        }
    }

    @Nested
    inner class Count {
        @Test
        fun `can count array elements`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, listOf(1, 2, 3))

            val spec = object : LookupInSpec() {
                val count = count("")
            }
            collection.lookupIn(id, spec) { assertEquals(3, spec.count.value) }
        }
    }

    @Nested
    @IgnoreWhen(clusterVersionIsBelow = "7.5")
    inner class AnyReplica {
        @Test
        fun `fails when spec is empty`(): Unit = runBlocking {
            val id = nextId()
            val emptySpec = object : LookupInSpec() {}
            assertThrows<InvalidArgumentException> { collection.lookupIn(id, emptySpec) }
        }

        @Test
        fun `throws DocumentUnretrievable when document is absent`(): Unit = runBlocking {
            val spec = object : LookupInSpec() {
                @Suppress("unused")
                val foo = get("foo")
            }
            assertThrows<DocumentUnretrievableException> { collection.lookupInAnyReplica(ABSENT_ID, spec) }
        }

        @Test
        fun `can get when present`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = object : LookupInSpec() {
                val foo = get("foo")
            }
            collection.lookupInAnyReplica(id, spec) {
                assertEquals("bar", spec.foo.contentAs<String>())
            }
        }
    }

    @Nested
    @IgnoreWhen(clusterVersionIsBelow = "7.5")
    inner class AllReplicas {
        @Test
        fun `fails when spec is empty`(): Unit = runBlocking {
            val id = nextId()
            val emptySpec = object : LookupInSpec() {}
            assertThrows<InvalidArgumentException> { collection.lookupIn(id, emptySpec) }
        }

        @Test
        fun `returns empty Flow when document is absent`(): Unit = runBlocking {
            val spec = object : LookupInSpec() {
                @Suppress("unused")
                val foo = get("foo")
            }
            assertEquals(
                emptyList<LookupInReplicaResult>(),
                collection.lookupInAllReplicas(ABSENT_ID, spec).toList(),
            )
        }

        @Test
        fun `always includes exactly one result from active`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = object : LookupInSpec() {
                val foo = get("foo")
            }

            val all = collection.lookupInAllReplicas(id, spec).toList()
            assertEquals(1, all.count { !it.replica })
            all.forEach { result -> assertEquals("bar", spec.foo.contentAs<String>(result)) }
        }
    }
}
