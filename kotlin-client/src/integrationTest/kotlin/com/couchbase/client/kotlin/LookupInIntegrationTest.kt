package com.couchbase.client.kotlin

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.subdoc.DocumentTooDeepException
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.internal.LookupInMacro
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@OptIn(VolatileCouchbaseApi::class)
@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class LookupInIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `fails when spec is empty`(): Unit = runBlocking {
        val id = nextId()
        assertThrows<IllegalArgumentException> { collection.lookupIn(id, LookupInSpec()) }
    }

    @Test
    fun `fails when spec is reused`(): Unit = runBlocking {
        val id = nextId()
        collection.upsert(id, mapOf("foo" to "bar"))

        val spec = LookupInSpec()
        spec.get("foo")
        collection.lookupIn(id, spec)
        assertThrows<IllegalStateException> { collection.lookupIn(id, spec) }
    }

    @Test
    fun `fails on spec mismatch`(): Unit = runBlocking {
        val id = nextId()
        collection.upsert(id, mapOf("foo" to "bar"))

        val spec = LookupInSpec()
        val foo = spec.get("foo")
        val result = collection.lookupIn(id, spec)
        Assertions.assertEquals("bar", foo.contentAs<String>(result))
        val t = assertThrows<IllegalArgumentException> {
            LookupInSpec().get("foo").contentAs<String>(result)
        }
        assertThat(t).hasMessageContaining("not created from the same LookupInSpec")

    }

    @Test
    fun `throws DocumentNotFoundException when document is absent`(): Unit = runBlocking {
        val spec = LookupInSpec()
        spec.get("foo")
        assertThrows<DocumentNotFoundException> { collection.lookupIn(ABSENT_ID, spec) }
    }

    @Test
    fun `returns cas`(): Unit = runBlocking {
        val id = nextId()
        val cas = collection.upsert(id, mapOf("foo" to "bar")).cas

        val spec = LookupInSpec()
        spec.get("foo")
        Assertions.assertEquals(cas, collection.lookupIn(id, spec).cas)
    }

    @Nested
    inner class Exists {
        @Test
        fun `returns true for existing field`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = LookupInSpec()
            val fooExists = spec.exists("foo")
            collection.lookupIn(id, spec) {
                Assertions.assertTrue(fooExists.value)
                Assertions.assertTrue(fooExists.get(this))
            }
        }

        @Test
        fun `returns false for absent field`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = LookupInSpec()
            val nope = spec.exists("nope")
            collection.lookupIn(id, spec) {
                Assertions.assertFalse(nope.value)
                Assertions.assertFalse(nope.get(this))
            }
        }

        @Test
        fun `throws when doc is too deep`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, deeplyNested(128, "foo"))

            val spec = LookupInSpec()
            val fooExists = spec.exists("foo")
            collection.lookupIn(id, spec) {
                assertThrows<DocumentTooDeepException> { fooExists.value }
                assertThrows<DocumentTooDeepException> { fooExists.get(this) }
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

            val spec = LookupInSpec()
            val flags = spec.get(LookupInMacro.Flags)
            collection.lookupIn(id, spec, accessDeleted = true) {
                Assertions.assertTrue(this.deleted)
                Assertions.assertEquals(CodecFlags.JSON_COMMON_FLAGS, flags.contentAs())
            }
        }

        @Test
        fun `can request xattr after document field`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, mapOf("foo" to "bar"))

            val spec = LookupInSpec()
            val foo = spec.get("foo")
            val flags = spec.get(LookupInMacro.Flags)
            collection.lookupIn(id, spec) {
                Assertions.assertFalse(this.deleted)
                Assertions.assertEquals(CodecFlags.JSON_COMPAT_FLAGS, flags.contentAs())
                Assertions.assertEquals("bar", foo.contentAs())
            }
        }

        @Test
        fun `fails when doc is too deep`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, deeplyNested(128))

            val spec = LookupInSpec()
            val foo = spec.get("foo")
            collection.lookupIn(id, spec) {
                assertThrows<DocumentTooDeepException> { foo.content }
            }
        }

        @Test
        fun `can get binary document`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, Content.binary("xyzzy".toByteArray()))

            val spec = LookupInSpec()
            val doc = spec.get("")
            collection.lookupIn(id, spec) { Assertions.assertEquals("xyzzy", doc.content.toStringUtf8()) }
        }
    }

    @Nested
    inner class Count {
        @Test
        fun `can count array elements`(): Unit = runBlocking {
            val id = nextId()
            collection.upsert(id, listOf(1, 2, 3))

            val spec = LookupInSpec()
            val count = spec.count("")
            collection.lookupIn(id, spec) { Assertions.assertEquals(3, count.value) }
        }
    }
}
