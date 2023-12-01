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

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.subdoc.PathExistsException
import com.couchbase.client.core.error.subdoc.PathNotFoundException
import com.couchbase.client.core.error.subdoc.XattrInvalidKeyComboException
import com.couchbase.client.core.msg.kv.SubdocMutateRequest.SUBDOC_MAX_FIELDS
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.MutateInMacro
import com.couchbase.client.kotlin.kv.MutateInSpec
import com.couchbase.client.kotlin.kv.StoreSemantics.Companion.insert
import com.couchbase.client.kotlin.kv.StoreSemantics.Companion.upsert
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.ClusterType.MOCKED
import com.couchbase.client.test.IgnoreWhen
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.temporal.ChronoUnit.DAYS
import java.time.temporal.ChronoUnit.SECONDS

/**
 * Ported from the Scala SubdocMutateSpec tests. Please keep in sync.
 */
@IgnoreWhen(clusterTypes = [MOCKED])
internal class SubdocMutateIntegrationTest : KotlinIntegrationTest() {

    private suspend fun getContent(docId: String): ObjectNode = collection.get(docId).contentAs()

    private suspend fun prepare(content: Map<String, Any?>): String {
        val docId: String = nextId()
        collection.insert(docId, content)
        return docId
    }

    private suspend fun prepareXattr(content: Map<String, Any?>): String {
        val docId = nextId()
        collection.mutateIn(docId, storeSemantics = insert()) {
            insert("x", content, xattr = true)
        }
        return docId
    }

    @Test
    fun `no commands`(): Unit = runBlocking {
        assertThrows<IllegalArgumentException> { collection.mutateIn(nextId(), MutateInSpec()) }
    }

    @Test
    fun `too many commands`(): Unit = runBlocking {
        assertThrows<IllegalArgumentException> {
            collection.mutateIn(nextId()) {
                repeat(SUBDOC_MAX_FIELDS + 1) {
                    upsert("foo", "bar")
                }
            }
        }
    }

    @Test
    fun `insert string`(): Unit = runBlocking {
        val docId = prepare(mapOf())
        collection.mutateIn(docId) { insert("foo2", "bar2") }
        assertEquals("bar2", getContent(docId).getString("foo2"))
    }

    @Test
    fun remove(): Unit = runBlocking {
        val docId = prepare(mapOf("foo" to "bar"))
        collection.mutateIn(docId) { remove("foo") }
        assertFalse(getContent(docId).containsKey("foo"))
    }

    private suspend fun checkSingleOpSuccess(content: Map<String, Any?>, block: MutateInSpec.() -> Unit): ObjectNode {
        val docId = prepare(content)
        collection.mutateIn(docId, block = block)
        return getContent(docId)
    }

    private suspend fun checkSingleOpSuccessXattr(
        content: Map<String, Any?>,
        block: MutateInSpec.() -> Unit,
    ): ObjectNode {
        val docId = prepareXattr(content)
        collection.mutateIn(docId, block = block)
        val spec = object : LookupInSpec() {
            val x = get("x", xattr = true)
        }
        val result = collection.lookupIn(docId, spec)
        return spec.x.contentAs(result)
    }

    private suspend inline fun <reified T : Throwable> checkSingleOpFailure(
        content: Map<String, Any?>,
        noinline block: MutateInSpec.() -> Unit,
    ) {
        val docId = prepare(content)
        assertThrows<T> { collection.mutateIn(docId, block = block) }
    }

    private suspend inline fun <reified T : Throwable> checkSingleOpFailureXattr(
        content: Map<String, Any?>,
        noinline block: MutateInSpec.() -> Unit,
    ) {
        val docId = prepareXattr(content)
        assertThrows<T> { collection.mutateIn(docId, block = block) }
    }


    @Test
    fun `insert string already there`(): Unit = runBlocking {
        checkSingleOpFailure<PathExistsException>(mapOf("foo" to "bar")) { insert("foo", "bar2") }
    }

    @Test
    fun `insert bool`(): Unit = runBlocking {
        val docId = prepare(mapOf())
        collection.mutateIn(docId) { insert("foo", false) }
        assertEquals(objectNodeOf("foo" to false), getContent(docId))
    }

    @Test
    fun `insert int`(): Unit = runBlocking {
        val docId = prepare(mapOf())
        collection.mutateIn(docId) { insert("foo", 42) }
        assertEquals(objectNodeOf("foo" to 42), getContent(docId))
    }

    @Test
    fun `insert double`(): Unit = runBlocking {
        val docId = prepare(mapOf())
        collection.mutateIn(docId) { insert("foo", 42.3) }
        assertEquals(objectNodeOf("foo" to 42.3), getContent(docId))
    }

    @Test
    fun `replace string`(): Unit = runBlocking {
        val docId = prepare(mapOf("foo" to "bar"))
        collection.mutateIn(docId) { replace("foo", "bar2") }
        assertEquals(objectNodeOf("foo" to "bar2"), getContent(docId))
    }

    @Test
    fun `replace full document`(): Unit = runBlocking {
        val docId = prepare(mapOf("foo" to "bar"))
        collection.mutateIn(docId) { replace("", mapOf("foo2" to "bar2")) }
        assertEquals(objectNodeOf("foo2" to "bar2"), getContent(docId))
    }

    @Test
    fun `replace string does not exist`(): Unit = runBlocking {
        checkSingleOpFailure<PathNotFoundException>(mapOf()) { replace("foo", "bar2") }
    }

    @Test
    fun `upsert string`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to "bar")) { upsert("foo", "bar2") }
        assertEquals("bar2", updatedContent.getString("foo"))
    }

    @Test
    fun `upsert string does not exist`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf()) { upsert("foo", "bar2") }
        assertEquals("bar2", updatedContent.getString("foo"))
    }

    @Test
    fun `array append`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello"))) {
            arrayAppend("foo", listOf("world"))
        }
        assertEquals(listOf("hello", "world"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array append multi`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello"))) {
            arrayAppend("foo", listOf("world", "mars"))
        }
        assertEquals(listOf("hello", "world", "mars"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array append list string`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello"))) {
            arrayAppend("foo", listOf("world", listOf("mars", "jupiter")))
        }
        assertEquals(listOf("hello", "world", listOf("mars", "jupiter")), updatedContent.getArray("foo"))
    }

    @Test
    fun `array prepend`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello"))) {
            arrayPrepend("foo", listOf("world"))
        }
        assertEquals(listOf("world", "hello"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array prepend multi`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello"))) {
            arrayPrepend("foo", listOf("world", "mars"))
        }
        assertEquals(listOf("world", "mars", "hello"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array prepend list string`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello"))) {
            arrayPrepend("foo", listOf("world", listOf("mars", "jupiter")))
        }
        assertEquals(listOf("world", listOf("mars", "jupiter"), "hello"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array insert`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello", "world"))) {
            arrayInsert("foo[1]", listOf("cruel"))
        }
        assertEquals(listOf("hello", "cruel", "world"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array insert multi`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello", "world"))) {
            arrayInsert("foo[1]", listOf("cruel", "mars"))
        }
        assertEquals(listOf("hello", "cruel", "mars", "world"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array insert unique does not exist`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to listOf("hello", "world"))) {
            arrayAddUnique("foo", "cruel")
            arrayAddUnique("foo", Int.MAX_VALUE)
            arrayAddUnique("foo", Long.MAX_VALUE)
            arrayAddUnique("foo", true)
            arrayAddUnique("foo", null as String?)
        }
        assertEquals(listOf("hello", "world", "cruel", Int.MAX_VALUE, Long.MAX_VALUE, true, null), updatedContent.getArray("foo"))
    }

    @Test
    fun `array insert unique does exist`(): Unit = runBlocking {
        checkSingleOpFailure<PathExistsException>(mapOf("foo" to listOf("hello", "world"))) {
            arrayAddUnique("foo", "hello")
        }
    }

    @Test
    fun `counter add`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to 10)) { addAndGet("foo", 5 ) }
        assertEquals(15, updatedContent.getInt("foo"))
    }

    @Test
    fun `counter minus`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccess(mapOf("foo" to 10)) { addAndGet("foo", -3 ) }
        assertEquals(7, updatedContent.getInt("foo"))
    }

    @Test
    fun `insert xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) { insert("x.foo", "bar2", xattr = true) }
        assertEquals("bar2", updatedContent.getString("foo"))
    }

    @Test
    fun `remove xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to "bar")) { remove("x.foo", xattr = true) }
        assertFalse(updatedContent.containsKey("foo"))
    }

    @Test
    fun `remove xattr does not exist`(): Unit = runBlocking {
        checkSingleOpFailure<PathNotFoundException>(mapOf()) { remove("x.foo", xattr = true) }
    }

    @Test
    fun `insert string already there xattr`(): Unit = runBlocking {
        checkSingleOpFailureXattr<PathExistsException>(mapOf("foo" to "bar")) { insert("x.foo", "bar2", xattr = true) }
    }

    @Test
    fun `replace string xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to "bar")) { replace("x.foo", "bar2", xattr = true) }
        assertEquals("bar2", updatedContent.getString("foo"))
    }

    @Test
    fun `replace string does not exist xattr`(): Unit = runBlocking {
        checkSingleOpFailureXattr<PathNotFoundException>(mapOf()) { replace("x.foo", "bar2", xattr = true) }
    }

    @Test
    fun `upsert string xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to "bar")) { upsert("x.foo", "bar2", xattr = true) }
        assertEquals("bar2", updatedContent.getString("foo"))
    }

    @Test
    fun `upsert string does not exist xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) { upsert("x.foo", "bar2", xattr = true) }
        assertEquals("bar2", updatedContent.getString("foo"))
    }

    @Test
    fun `array append xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to listOf("hello"))) {
            arrayAppend("x.foo", listOf("world"), xattr = true)
        }
        assertEquals(listOf("hello", "world"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array prepend xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to listOf("hello"))) {
            arrayPrepend("x.foo", listOf("world"), xattr = true)
        }
        assertEquals(listOf("world", "hello"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array insert xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to listOf("hello", "world"))) {
            arrayInsert("x.foo[1]", listOf("cruel"), xattr = true)
        }
        assertEquals(listOf("hello", "cruel", "world"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array insert unique does not exist xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to listOf("hello", "world"))) {
            arrayAddUnique("x.foo", "cruel", xattr = true)
        }
        assertEquals(listOf("hello", "world", "cruel"), updatedContent.getArray("foo"))
    }

    @Test
    fun `array insert unique does exist xattr`(): Unit = runBlocking {
        checkSingleOpFailureXattr<PathExistsException>(mapOf("foo" to listOf("hello", "world"))) {
            arrayAddUnique("x.foo", "hello", xattr = true)
        }
    }

    @Test
    fun `counter add xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to 10)) { addAndGet("x.foo", 5, xattr = true) }
        assertEquals(15, updatedContent.getInt("foo"))
    }

    @Test
    fun `xattr ops are reordered`(): Unit = runBlocking {
        val docId = prepareXattr(mapOf())

        val spec = MutateInSpec()
        spec.insert("foo2", "bar2")
        val count = spec.addAndGet("x.foo", 5, xattr = true)

        val result = collection.mutateIn(docId, spec)
        assertEquals(5, count.get(result))
    }

    @Test
    fun `counter minus xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf("foo" to 10)) { addAndGet("x.foo", -3, xattr = true) }
        assertEquals(7, updatedContent.getInt("foo"))
    }

    @IgnoreWhen(clusterTypes = [MOCKED])
    @Test
    fun `insert expand macro xattr do not flag`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) {
            insert("x.foo", MutateInMacro.Cas.value, xattr = true)
        }
        assertEquals("\${Mutation.CAS}", updatedContent.getString("foo"))
    }

    @IgnoreWhen(clusterTypes = [MOCKED])
    @Test
    fun `insert expand macro xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) {
            insert("x.foo", MutateInMacro.Cas, xattr = true)
        }
        assertThat(updatedContent.getString("foo")).matches(HEX_ENCODED_INT64)
    }

    @IgnoreWhen(clusterTypes = [MOCKED])
    @Test
    fun `insert expand macro crc32 xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) {
            insert("x.foo", MutateInMacro.ValueCrc32c, xattr = true)
        }
        assertThat(updatedContent.getString("foo")).matches(HEX_ENCODED_INT32)
    }

    @IgnoreWhen(clusterTypes = [MOCKED])
    @Test
    fun `insert expand macro seqno xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) {
            insert("x.foo", MutateInMacro.SeqNo, xattr = true)
        }
        assertThat(updatedContent.getString("foo")).matches(HEX_ENCODED_INT64)
    }

    @IgnoreWhen(clusterTypes = [MOCKED])
    @Test
    fun `upsert expand macro xattr`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) {
            upsert("x.foo", MutateInMacro.Cas, xattr = true)
        }
        assertThat(updatedContent.getString("foo")).matches(HEX_ENCODED_INT64)
    }

    @Test
    fun `insert xattr create path`(): Unit = runBlocking {
        val updatedContent = checkSingleOpSuccessXattr(mapOf()) {
            insert("x.foo.baz", "bar2", xattr = true)
        }
        assertEquals("bar2", updatedContent.getObject("foo").getString("baz"))
    }

    @IgnoreWhen(clusterTypes = [MOCKED])
    @Test
    fun expiration(): Unit = runBlocking {
        val docId = prepare(mapOf("hello" to "world"))
        val expiry = Expiry.of(Instant.now().plus(3, DAYS).truncatedTo(SECONDS))
        collection.mutateIn(docId, expiry = expiry) { upsert("foo", "bar") }
        assertEquals(expiry, collection.get(docId, withExpiry = true).expiry)
    }

    @Test
    fun `more than 16`(): Unit = runBlocking {
        val docId = prepare(mapOf())
        assertThrows<IllegalArgumentException> {
            collection.mutateIn(docId) {
                for (i in 0..16) insert("foo-$i", "bar")
            }
        }
    }

    @Test
    fun `two commands succeed`(): Unit = runBlocking {
        val docId = prepare(mapOf())
        collection.mutateIn(docId) { for (i in 0..1) insert("foo-$i", "bar") }
        assertEquals(objectNodeOf("foo-0" to "bar", "foo-1" to "bar"), getContent(docId))
    }

    @Test
    fun `two commands one fails`(): Unit = runBlocking {
        val docId = prepare(mapOf("foo1" to "bar1"))
        assertThrows<PathExistsException> {
            collection.mutateIn(docId) {
                insert("foo0", "bar0")
                insert("foo1", "zot")
                remove("foo3")
            }
        }
        assertEquals(objectNodeOf("foo1" to "bar1"), getContent(docId))
    }

    @Test
    @IgnoreWhen(clusterTypes = [MOCKED],
        clusterVersionIsEqualToOrAbove = "7.6.0") // Support added in MB-57864
    fun `multiple xattr keys should fail`(): Unit = runBlocking {
        assertThrows<XattrInvalidKeyComboException> {
            collection.mutateIn(nextId(), storeSemantics = upsert()) {
                insert("color", "red", xattr = true)
                insert("fruit", "apple", xattr = true)
            }
        }
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.CREATE_AS_DELETED])
    fun `create as deleted can access`(): Unit = runBlocking {
        val docId = nextId()
        collection.mutateIn(docId, storeSemantics = insert(), createAsDeleted = true) {
            insert("foo", "bar", xattr = true)
        }
        assertThrows<DocumentNotFoundException> { collection.get(docId) }
        assertThrows<DocumentNotFoundException> {
            val spec = object : LookupInSpec() {
                @Suppress("unused")
                val foo = get("foo", xattr = true)
            }
            collection.lookupIn(docId, spec)
        }

        val spec = object : LookupInSpec() {
            val foo = get("foo", xattr = true)
        }
        val result = collection.lookupIn(docId, spec, accessDeleted = true)
        assertEquals("bar", spec.foo.contentAs<String>(result))
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.CREATE_AS_DELETED])
    fun `create as deleted can insert on top`(): Unit = runBlocking {
        val docId = nextId()

        collection.mutateIn(docId, storeSemantics = insert(), createAsDeleted = true) {
            insert("foo", "bar", xattr = true)
        }
        collection.mutateIn(docId, storeSemantics = insert()) {
            insert("foo", "bar", xattr = true)
        }

        val spec = object : LookupInSpec() {
            val foo = get("foo", xattr = true)
        }
        val result = collection.lookupIn(docId, spec)
        assertEquals("bar", spec.foo.contentAs<String>(result))
    }
}

private const val HEX_ENCODED_INT32 = "0x[0-9a-f]{8}"
private const val HEX_ENCODED_INT64 = "0x[0-9a-f]{16}"

private val jsonMapper = ObjectMapper()

private fun objectNodeOf(vararg pair: Pair<String, Any?>) =
    jsonMapper.convertValue(pair.toMap(), ObjectNode::class.java)

private fun ObjectNode.getString(field: String) = path(field).textValue()
private fun ObjectNode.getInt(field: String) = path(field).intValue()
private fun ObjectNode.getArray(field: String) = jsonMapper.convertValue(path(field), List::class.java)
private fun ObjectNode.getObject(field: String) = get(field) as ObjectNode
private fun ObjectNode.containsKey(field: String) = get(field) != null
