/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.manager.collection.CollectionManager
import com.couchbase.client.kotlin.manager.collection.ScopeSpec

internal suspend fun createScopeAndCollections(bucket: Bucket) {
    // Create a new scope containing some collections
    val manager: CollectionManager = bucket.collections

    with(manager) {
        val scopeName = "tenant-a"
        createScope(scopeName)
        createCollection(scopeName = scopeName, collectionName = "widgets")
        createCollection(scopeName = scopeName, collectionName = "invoices")

        println(getScope(scopeName))
    }
}

internal suspend fun copyScope(bucket: Bucket) {
    // Create a new scope using an existing scope as a template.
    // (The new scope will have collections with the same names
    // as the template's collections.)
    val manager: CollectionManager = bucket.collections

    val templateScopeSpec: ScopeSpec = manager.getScope("existing-scope")
    val nameOfNewScope = "new-scope"
    manager.createScope(nameOfNewScope)
    templateScopeSpec.collections
        .filter { it.name != "_default" } // can't create default collection
        .forEach { manager.createCollection(it.copy(scopeName = nameOfNewScope)) }
}
