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

import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.manager.view.DesignDocument
import com.couchbase.client.kotlin.manager.view.View
import com.couchbase.client.kotlin.manager.view.ViewIndexManager
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.view.DesignDocumentNamespace
import com.couchbase.client.kotlin.view.DesignDocumentNamespace.PRODUCTION
import com.couchbase.client.kotlin.view.ViewScanConsistency.Companion.requestPlus
import com.couchbase.client.kotlin.view.ViewSelection.Companion.keys
import com.couchbase.client.kotlin.view.execute
import com.couchbase.client.test.ClusterType.CAVES
import com.couchbase.client.test.ClusterType.MOCKED
import com.couchbase.client.test.IgnoreWhen
import com.couchbase.client.test.Util.waitUntilCondition
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.*

@OptIn(VolatileCouchbaseApi::class)
@IgnoreWhen(clusterTypes = [MOCKED, CAVES])
internal class ViewIntegrationTest : KotlinIntegrationTest() {
    private val DDOC_NAME = "everything"
    private val VIEW_NAME = "all"
    private val VIEW_WITH_REDUCE_NAME = "all_red"

    private val viewIndexes by lazy { bucket.viewIndexes }

    private suspend fun ViewIndexManager.getDesignDocumentOrNull(name: String, namespace: DesignDocumentNamespace) =
        getAllDesignDocuments(namespace).firstOrNull { ddoc -> ddoc.name == name }

    private fun createDesignDocument() {
        val views = HashMap<String, View>()
        views[VIEW_NAME] = View("function(doc,meta) { emit(meta.id, doc) }")
        views[VIEW_WITH_REDUCE_NAME] = View("function(doc,meta) { emit(meta.id, doc) }", "_count")
        val designDocument = DesignDocument(DDOC_NAME, views)

        runBlocking {
            viewIndexes.upsertDesignDocument(designDocument, PRODUCTION)
            waitUntilCondition {
                runBlocking { viewIndexes.getDesignDocumentOrNull(DDOC_NAME, PRODUCTION) != null }
            }
        }
    }

    @BeforeAll
    fun setup() {
        createDesignDocument();
    }

    @AfterAll
    fun cleanup(): Unit = runBlocking {
        runCatching { viewIndexes.dropDesignDocument(DDOC_NAME, PRODUCTION) }
    }

    @Test
    fun succeedsWithNoRowsReturned(): Unit = runBlocking {
        val viewResult = bucket.viewQuery(DDOC_NAME, VIEW_NAME, limit = 0).execute()
        assertThat(viewResult.rows).isEmpty()
        assertNull(viewResult.metadata.debug)
    }

    @Test
    fun canReadDebugInfo(): Unit = runBlocking {
        val viewResult = bucket.viewQuery(DDOC_NAME, VIEW_NAME, debug = true).execute()
        assertNotNull(viewResult.metadata.debug)
    }

    @Test
    fun returnsDataJustWritten(): Unit = runBlocking {
        val docsToWrite = 10
        val ids = HashSet<String>()

        for (i in 0 until docsToWrite) {
            val id = "viewdoc-$i"
            ids += id
            collection.upsert(id, "foo")
        }

        val viewResult = bucket.viewQuery(DDOC_NAME, VIEW_NAME, scanConsistency = requestPlus())
            .execute()

        viewResult.rows
            .filter { ids.remove(it.id) }
            .forEach { assertEquals("foo", it.valueAs<String>()) }

        assertThat(ids).isEmpty()
    }

    @Test
    fun canQueryWithKeysPresent(): Unit = runBlocking {
        val docsToWrite = 2
        val ids = ArrayList<String>()

        for (i in 0 until docsToWrite) {
            val id = "keydoc-$i"
            ids += id
            collection.upsert(id, "foo")
        }

        val viewResult = bucket.viewQuery(
            DDOC_NAME, VIEW_NAME,
            selection = keys(ids),
            scanConsistency = requestPlus()
        ).execute()

        assertEquals(ids, viewResult.rows.map { it.id })
        assertEquals(listOf("foo", "foo"), viewResult.rows.map { it.valueAs<String>() })
    }

    @Test
    fun canQueryWithReduceEnabled(): Unit = runBlocking {
        val docsToWrite = 2
        val ids = ArrayList<String>()

        for (i in 0 until docsToWrite) {
            val id = "reddoc-$i"
            ids += id
            collection.upsert(id, "foo")
        }

        bucket.viewQuery(
            DDOC_NAME, VIEW_WITH_REDUCE_NAME,
            scanConsistency = requestPlus()
        ).execute().let { viewResult ->
            // total rows is always 0 on a reduce response
            assertEquals(0, viewResult.metadata.totalRows)

            val count = viewResult.rows[0].valueAs<Int>()
            assertThat(count).isGreaterThanOrEqualTo(docsToWrite)
        }

        bucket.viewQuery(
            DDOC_NAME, VIEW_WITH_REDUCE_NAME,
            limit = 0,
            scanConsistency = requestPlus()
        ).execute().let { viewResult ->
            assertEquals(0, viewResult.metadata.totalRows)
            assertThat(viewResult.rows).isEmpty()
        }
    }
}
