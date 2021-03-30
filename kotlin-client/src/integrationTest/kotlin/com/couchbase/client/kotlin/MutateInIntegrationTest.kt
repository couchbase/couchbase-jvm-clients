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

import com.couchbase.client.core.error.subdoc.ValueInvalidException
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.MutateInSpec
import com.couchbase.client.kotlin.kv.StoreSemantics.Companion.insert
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class MutateInIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `can decrement counter below zero`(): Unit = runBlocking {
        val id = nextId()
        collection.upsert(id, Content.json("{}"));

        val spec = MutateInSpec()
        val counter = spec.decrementAndGet("foo.bar")
        val result = collection.mutateIn(id, spec)

        assertEquals(counter.get(result), -1)
        assertEquals(
            mapOf("foo" to mapOf("bar" to -1)),
            collection.get(id).contentAs<Any>()
        )
    }

    @Test
    fun `fails on counter overflow`(): Unit = runBlocking {
        val id = nextId()
        collection.upsert(id, mapOf("foo" to 1))
        val spec = MutateInSpec()
        spec.incrementAndGet("foo", delta = Long.MAX_VALUE)
        assertThrows<ValueInvalidException> { collection.mutateIn(id, spec) }
    }

    @Test
    fun `remove full document`(): Unit = runBlocking {
        val id = nextId()
        val systemXattrName = "_x" // underscore lets it survive document deletion
        collection.upsert(id, mapOf("foo" to "bar"))
        collection.mutateIn(id) {
            remove("")
            upsert(systemXattrName, "y", xattr = true)
        }

        assertFalse(collection.exists(id).exists)

        val spec = object : LookupInSpec() {
            val systemXattr = get(systemXattrName, xattr = true)
        }
        val result = collection.lookupIn(id, spec, accessDeleted = true)
        assertEquals("y", spec.systemXattr.contentAs<String>(result))
    }

    @Test
    fun `counter works`(): Unit = runBlocking {
        val id = nextId()
        run {
            val spec = MutateInSpec()
            val counter = spec.incrementAndGet("foo")
            val result = collection.mutateIn(id, spec, storeSemantics = insert())
            assertEquals(1, counter.get(result))
        }
        run {
            val spec = MutateInSpec()
            val counter = spec.incrementAndGet("foo")
            val result = collection.mutateIn(id, spec)
            assertEquals(2, counter.get(result))
        }
    }
}
