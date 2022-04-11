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

package com.couchbase.client.kotlin

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE

/**
 * Identifies a collection by its fully-qualified path:
 * bucket name, scope name, and collection name.
 *
 * Called a "keyspace" because Couchbase document IDs (keys)
 * are unique within a collection.
 */
public class Keyspace(
    public val bucket: String,
    @SinceCouchbase("7.0") public val scope: String = DEFAULT_SCOPE,
    @SinceCouchbase("7.0") public val collection: String = DEFAULT_COLLECTION,
) {
    init {
        require(collection != DEFAULT_COLLECTION || scope == DEFAULT_SCOPE) {
            "Must specify a non-default collection; only the default scope may have a default collection."
        }
    }

    public fun copy(
        bucket: String = this.bucket,
        scope: String = this.scope,
        collection: String = this.collection,
    ): Keyspace = Keyspace(bucket, scope, collection)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Keyspace

        if (bucket != other.bucket) return false
        if (scope != other.scope) return false
        if (collection != other.collection) return false

        return true
    }

    override fun hashCode(): Int {
        var result = bucket.hashCode()
        result = 31 * result + scope.hashCode()
        result = 31 * result + collection.hashCode()
        return result
    }

    override fun toString(): String {
        return "Keyspace(bucket='$bucket', scope='$scope', collection='$collection')"
    }

    internal fun format(): String {
        return "$bucket:$scope.$collection"
    }
}
