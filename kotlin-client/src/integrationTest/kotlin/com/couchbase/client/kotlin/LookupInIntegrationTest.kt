package com.couchbase.client.kotlin

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.internal.LookupInMacro
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
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
        assertThrows<IllegalArgumentException> { collection.lookupIn(id, emptySpec) }
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
}
