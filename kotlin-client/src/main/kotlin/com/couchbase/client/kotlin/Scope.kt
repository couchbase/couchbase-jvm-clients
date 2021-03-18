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

import com.couchbase.client.core.Core
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.kotlin.internal.toOptional
import java.util.concurrent.ConcurrentHashMap

public class Scope(
    public val name: String,
    public val bucket: Bucket,
) {
    internal val core: Core = bucket.core

    private val collectionCache = ConcurrentHashMap<String, Collection>()

    /**
     * Opens a collection for this scope.
     *
     * @param name the collection name.
     */
    public fun collection(name: String): Collection {
        val collectionName = name
        val scopeName = this.name
        return collectionCache.computeIfAbsent(name) {
            val defaultScopeAndCollection = collectionName == DEFAULT_COLLECTION && scopeName == DEFAULT_SCOPE
            if (!defaultScopeAndCollection) {
                core.configurationProvider().refreshCollectionId(
                    CollectionIdentifier(bucket.name, scopeName.toOptional(), collectionName.toOptional())
                )
            }
            Collection(collectionName, this)
        }
    }

    public fun defaultCollection(): Collection = collection(DEFAULT_COLLECTION)
}
