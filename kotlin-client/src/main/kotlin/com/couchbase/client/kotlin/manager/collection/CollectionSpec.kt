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

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.manager.collection.CollectionSpec.Companion.NEVER_EXPIRE
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Information about a collection.
 *
 * @property scopeName Name of the parent scope
 * @property name Name of the collection
 * @property maxExpiry Maximum expiry for documents in the collection.
 * Null means the collection's max expiry is always the same as the bucket's max expiry.
 * A duration equal to [NEVER_EXPIRE] (-1 seconds) means documents in the collection never expire,
 * regardless of the bucket's max expiry.
 */
public class CollectionSpec(
    public val scopeName: String,
    public val name: String,
    public val maxExpiry: Duration? = null,
    public val history: Boolean? = null,
) {
    public companion object {
        /**
         * A special collection "max expiry" value that means documents in the collection
         * never expire, regardless of the bucket's max expiry setting.
         *
         * Requires Couchbase Server 7.6 or later.
         */
        @SinceCouchbase("7.6")
        @VolatileCouchbaseApi
        public val NEVER_EXPIRE: Duration = (-1).seconds
    }

    @Deprecated("Retained for binary compatibility", level = DeprecationLevel.HIDDEN)
    public constructor(
        scopeName: String,
        name: String,
        maxExpiry: Duration? = null,
    ) : this(
        scopeName = scopeName,
        name = name,
        maxExpiry = maxExpiry,
    )

    public fun copy(
        scopeName: String = this.scopeName,
        name: String = this.name,
        maxExpiry: Duration? = this.maxExpiry,
        history: Boolean? = this.history,
    ): CollectionSpec = CollectionSpec(scopeName, name, maxExpiry, history)

    @Deprecated("Retained for binary compatibility", level = DeprecationLevel.HIDDEN)
    public fun copy(
        scopeName: String = this.scopeName,
        name: String = this.name,
        maxExpiry: Duration? = this.maxExpiry,
    ): CollectionSpec = CollectionSpec(scopeName, name, maxExpiry)

    override fun toString(): String {
        return "CollectionSpec(scopeName='$scopeName', name='$name', maxExpiry=$maxExpiry, history=$history)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CollectionSpec

        if (scopeName != other.scopeName) return false
        if (name != other.name) return false
        if (maxExpiry != other.maxExpiry) return false
        if (history != other.history) return false

        return true
    }

    override fun hashCode(): Int {
        var result = scopeName.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + (maxExpiry?.hashCode() ?: 0)
        return result
    }
}

