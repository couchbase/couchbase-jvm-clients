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

import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.StoreSemantics.Companion.insert
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType.MOCKED
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

public data class Point(public val x: Int, public val y: Int)

@IgnoreWhen(clusterTypes = [MOCKED])
internal class SubdocSerializationIntegrationTest : KotlinIntegrationTest() {

    // shared lookup spec
    private val lookup = object : LookupInSpec() {
        val foo = get("foo")
    }

    @Test
    fun `single bean`(): Unit = runBlocking {
        val docId = nextId()
        collection.mutateIn(docId, storeSemantics = insert()) {
            insert("foo", Point(1, 2))
        }

        val result = collection.lookupIn(docId, lookup)
        assertEquals(Point(1, 2), lookup.foo.contentAs<Point>(result))
    }

    @Test
    fun `list of beans`(): Unit = runBlocking {
        val docId = nextId()
        collection.mutateIn(docId, storeSemantics = insert()) {
            insert("foo", listOf(Point(1, 2), Point(3, 4)))
        }

        val result = collection.lookupIn(docId, lookup)
        assertEquals(listOf(Point(1, 2), Point(3, 4)), lookup.foo.contentAs<List<Point>>(result))
    }

    @Test
    fun `append beans to array of beans`(): Unit = runBlocking {
        val docId = nextId()
        collection.mutateIn(docId, storeSemantics = insert()) {
            arrayAppend("foo", listOf(Point(1, 2), Point(3, 4)))
        }

        val result = collection.lookupIn(docId, lookup)
        assertEquals(listOf(Point(1, 2), Point(3, 4)), lookup.foo.contentAs<List<Point>>(result))
    }

    @Test
    fun `insert beans to array of beans`(): Unit = runBlocking {
        val docId = nextId()
        collection.mutateIn(docId, storeSemantics = insert()) {
            arrayAppend("foo", listOf(listOf(Point(1, 2), Point(3, 4))))
        }

        val result = collection.lookupIn(docId, lookup)
        assertEquals(
            listOf(listOf(Point(1, 2), Point(3, 4))),
            lookup.foo.contentAs<List<List<Point>>>(result)
        )
    }
}
