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

import com.couchbase.client.core.error.ParsingFailureException
import com.couchbase.client.kotlin.analytics.AnalyticsParameters.Companion.named
import com.couchbase.client.kotlin.analytics.AnalyticsParameters.Companion.positional
import com.couchbase.client.kotlin.analytics.AnalyticsStatus
import com.couchbase.client.kotlin.analytics.execute
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.Capabilities.ANALYTICS
import com.couchbase.client.test.ClusterType.MOCKED
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

@IgnoreWhen(clusterTypes = [MOCKED], missesCapabilities = [ANALYTICS])
internal class AnalyticsIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `positional args dataverse query`(): Unit = runBlocking {
        val result = cluster.analyticsQuery(
            "SELECT RAW DataverseName FROM Metadata.`Dataverse` where DataverseName = $1",
            parameters = positional(listOf("Metadata"))
        ).execute()

        assertEquals(listOf("Metadata"), result.rows.map { it.contentAs<String>() })
    }

    @Test
    fun `named args dataverse query`(): Unit = runBlocking {
        val result = cluster.analyticsQuery(
            "SELECT RAW DataverseName FROM Metadata.`Dataverse` where DataverseName = \$dataverse",
            parameters = named("dataverse" to "Metadata")
        ).execute()

        assertEquals(listOf("Metadata"), result.rows.map { it.contentAs<String>() })
    }

    @Test
    fun `performs dataverse query`(): Unit = runBlocking {
        val result = cluster.analyticsQuery("SELECT DataverseName FROM Metadata.`Dataverse`")
            .execute()
        val rows = result.rows

        assertThat(rows).isNotEmpty()

        for (row in rows) {
            assertNotNull(row.contentAs<Map<String, Any?>>()["DataverseName"])
        }

        val meta = result.metadata
        assertThat(meta.clientContextId).isNotEmpty()
        assertNotNull(meta.signature)
        assertThat(meta.requestId).isNotEmpty()
        assertEquals(AnalyticsStatus.SUCCESS, meta.status)

        meta.metrics.let {
            assertThat(it.elapsedTime).isGreaterThan(Duration.ZERO);
            assertThat(it.executionTime).isGreaterThan(Duration.ZERO);

            assertEquals(rows.size.toLong(), it.resultCount)
            assertEquals(rows.size.toLong(), it.processedObjects)
            assertThat(it.resultSize).isGreaterThan(0)
            assertEquals(0, it.errorCount)
        }

        assertThat(meta.warnings).isEmpty()
    }

    @Test
    fun `fails on error`(): Unit = runBlocking {
        assertThrows<ParsingFailureException> { cluster.analyticsQuery("SELECT 1=").execute() }
    }

    @Test
    fun canSetCustomContextId(): Unit = runBlocking {
        val contextId = "mycontextid"
        val result = cluster.analyticsQuery(
            "SELECT DataverseName FROM Metadata.`Dataverse`",
            clientContextId = contextId
        ).execute()

        assertEquals(contextId, result.metadata.clientContextId)
    }
}
