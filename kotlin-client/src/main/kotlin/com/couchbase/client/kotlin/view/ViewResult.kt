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

package com.couchbase.client.kotlin.view

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect

public class ViewResult(
    public val rows: List<ViewRow>,
    public val metadata: ViewMetadata,
) {
    override fun toString(): String {
        return "ViewResult(rows=$rows, metadata=$metadata)"
    }
}

/**
 * Collects a View Flow into a ViewResult. Should only be called
 * if the View results are expected to fit in memory.
 */
public suspend fun Flow<ViewFlowItem>.execute(): ViewResult {
    val rows = ArrayList<ViewRow>()
    val meta = execute { rows.add(it) }
    return ViewResult(rows, meta);
}

/**
 * Collects a View Flow, passing each result row to the given lambda.
 * Returns metadata about the View.
 */
public suspend inline fun Flow<ViewFlowItem>.execute(
    crossinline rowAction: suspend (ViewRow) -> Unit
): ViewMetadata {

    var meta: ViewMetadata? = null

    collect { item ->
        when (item) {
            is ViewRow -> rowAction.invoke(item)
            is ViewMetadata -> meta = item
        }
    }

    check(meta != null) { "Expected View flow to have metadata, but none found." }
    return meta!!
}
