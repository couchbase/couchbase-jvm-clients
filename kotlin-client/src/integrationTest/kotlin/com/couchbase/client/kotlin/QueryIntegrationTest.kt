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

import com.couchbase.client.kotlin.query.QueryDiagnostics
import com.couchbase.client.kotlin.query.execute
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

@IgnoreWhen(missesCapabilities = [Capabilities.QUERY])
internal class QueryIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `query sandbox`() = runBlocking {
        val flow = cluster.query("select * from default")

        println(flow.execute())
        println(flow.execute())

        cluster.query("select * from default").execute()


        println(
            cluster.query(
                "select * from default",
                diagnostics = QueryDiagnostics(
                    metrics = true
                )
            ).execute().metadata
        )
        //.map{ it -> it.contentAs<Any>})
        //.collect { value -> println(value) }
    }
}
