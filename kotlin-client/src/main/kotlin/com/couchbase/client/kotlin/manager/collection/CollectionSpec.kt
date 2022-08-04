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

package com.couchbase.client.kotlin.manager.collection

import kotlin.time.Duration

/**
 * Information about a collection.
 *
 * @property scopeName Name of the parent scope
 * @property name Name of the collection
 * @property maxExpiry Maximum expiry for documents in the collection, or null if unlimited.
 */
public class CollectionSpec(
    public val scopeName: String,
    public val name: String,
    public val maxExpiry: Duration? = null,
) {
    public fun copy(
        scopeName: String = this.scopeName,
        name: String = this.name,
        maxExpiry: Duration? = this.maxExpiry,
    ): CollectionSpec = CollectionSpec(scopeName, name, maxExpiry)

    override fun toString(): String {
        return "CollectionSpec(scopeName='$scopeName', name='$name', maxExpiry=$maxExpiry)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CollectionSpec

        if (scopeName != other.scopeName) return false
        if (name != other.name) return false
        if (maxExpiry != other.maxExpiry) return false

        return true
    }

    override fun hashCode(): Int {
        var result = scopeName.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + (maxExpiry?.hashCode() ?: 0)
        return result
    }
}

