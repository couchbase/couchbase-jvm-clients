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

/**
 * Information about a scope and its child collections.
 */
public class ScopeSpec internal constructor(
    public val name: String,
    public val collections: List<CollectionSpec>,
) {
    public fun copy(
        name: String = this.name,
        collections: List<CollectionSpec> = this.collections,
    ): ScopeSpec = ScopeSpec(name, collections)

    override fun toString(): String {
        return "ScopeSpec(name='$name', collections=$collections)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ScopeSpec

        if (name != other.name) return false
        if (collections != other.collections) return false

        return true
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}
