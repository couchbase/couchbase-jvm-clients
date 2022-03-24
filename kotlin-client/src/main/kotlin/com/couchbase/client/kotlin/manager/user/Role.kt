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

package com.couchbase.client.kotlin.manager.user

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode

public class Role(
    public val name: String,
    public val bucket: String? = null,
    public val scope: String? = null,
    public val collection: String? = null,
) {
    private fun String?.isSpecified() = this != null && this != "*"
    private fun String?.wildcardToNull() = if (this == "*") null else this

    init {
        if (scope.isSpecified()) require(bucket.isSpecified()) { "Bucket must be non-null and not '*' when specifying a scope." }
        if (collection.isSpecified()) require(scope.isSpecified()) { "Scope must be non-null and not '*' when specifying a collection." }
    }

    internal constructor(json: ObjectNode) : this(
        name = json.path("role").textValue() ?: "",
        bucket = json.path("bucket_name").textValue(),
        scope = json.path("scope_name").textValue(),
        collection = json.path("collection_name").textValue(),
    )

    internal fun format(omitScopeAndCollectionWildcards: Boolean = true): String {
        // When upserting a user, the server only accepts wildcards for the bucket name.
        // Instead of wildcards for scope and collection, the component must be omitted.
        val s = if (omitScopeAndCollectionWildcards) scope.wildcardToNull() else scope
        val c = if (omitScopeAndCollectionWildcards) collection.wildcardToNull() else collection

        val list = listOfNotNull(bucket, s, c)
        return if (list.isEmpty()) name else "$name[" + list.joinToString(":") + "]"
    }

    override fun toString(): String = format(omitScopeAndCollectionWildcards = false)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Role

        if (name != other.name) return false
        if (bucket != other.bucket) return false
        if (scope.wildcardToNull() != other.scope.wildcardToNull()) return false
        if (collection.wildcardToNull() != other.collection.wildcardToNull()) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + (bucket?.hashCode() ?: 0)
        result = 31 * result + (scope?.hashCode() ?: 0)
        result = 31 * result + (collection?.hashCode() ?: 0)
        return result
    }
}
