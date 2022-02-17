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

@file:Suppress("DEPRECATION")

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.view.DesignDocumentNamespace
import com.couchbase.client.kotlin.view.ViewMetadata
import com.couchbase.client.kotlin.view.ViewResult
import com.couchbase.client.kotlin.view.ViewSelection
import com.couchbase.client.kotlin.view.execute

internal suspend fun bufferedViewQuery(bucket: Bucket) {
    // Buffered view query, for when results are known to fit in memory
    val result: ViewResult = bucket
        .viewQuery(
            designDocument = "myDesignDoc",
            viewName = "myView",
            namespace = DesignDocumentNamespace.DEVELOPMENT,
            selection = ViewSelection.key("foo")
        ).execute()
    result.rows.forEach { println(it) }
    println(result.metadata)
}

internal suspend fun streamingViewQuery(bucket: Bucket) {
    // Streaming view query, for when results are large or unbounded
    val metadata: ViewMetadata = bucket
        .viewQuery(
            designDocument = "myDesignDoc",
            viewName = "myView",
            namespace = DesignDocumentNamespace.PRODUCTION,
            selection = ViewSelection.range(
                startKey = "somePrefix",
                endKey = "somePrefix\uefff"
            )
        ).execute { row -> println(row) }
    println(metadata)
}
