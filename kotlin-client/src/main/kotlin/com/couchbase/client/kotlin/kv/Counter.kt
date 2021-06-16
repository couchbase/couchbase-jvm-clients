/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.kv

import com.couchbase.client.kotlin.BinaryCollection
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.Content
import kotlin.math.absoluteValue

/**
 * A counter backed by a document on Couchbase Server.
 *
 * Counter operations are atomic with respect to a single Couchbase Server cluster,
 * but not between clusters when Cross-Datacenter Replication (XDCR) is used.
 *
 * The content of a counter document is a single JSON integer with
 * a minimum value of zero and a maximum value of 2^64 - 1.
 *
 * A counter decremented below zero is reset to zero.
 *
 * A counter incremented above 2^64 - 1 overflows (wraps around).
 *
 * Counter values above 2^53 - 1 may have interoperability issues with
 * other languages that store all numbers as floating point values.
 *
 * @see Collection.binary
 * @see BinaryCollection.increment
 * @see BinaryCollection.decrement
 */
@VolatileCouchbaseApi
public class Counter internal constructor(
    public val collection: Collection,
    public val documentId: String,
    public val common: CommonOptions,
    public val durability: Durability,
    public val expiry: Expiry,
) {
    /**
     * Atomically increments by one the current value.
     *
     * @return the updated value
     */
    public suspend fun incrementAndGet(): ULong = addAndGet(1)

    /**
     * Atomically decrements by one the current value.
     *
     * @return the updated value
     */
    public suspend fun decrementAndGet(): ULong = addAndGet(-1)

    /**
     * Atomically adds the given delta to the current value.
     *
     * @return the updated value
     */
    public suspend fun addAndGet(delta: Long): ULong {
        return if (delta >= 0)
            collection.binary.increment(documentId, common, durability, expiry, delta.toULong()).content
        else
            collection.binary.decrement(documentId, common, durability, expiry, absUnsigned(delta)).content
    }

    /**
     * Returns the current value of the counter.
     */
    public suspend fun get(): ULong = addAndGet(0)

    /**
     * Sets the value of the counter.
     */
    public suspend fun set(value: ULong): MutationResult =
        collection.upsert(
            id = documentId,
            content = Content.string(value.toString()),
            common = common,
            expiry = expiry,
            durability = durability,
        )

    override fun toString(): String {
        return "Counter(collection=$collection, documentId='$documentId')"
    }
}

// calculates absolute value of a Long, returning the result as an unsigned long
// to handle that pesky edge condition at Long.MIN_VALUE
private fun absUnsigned(value: Long): ULong {
    return if (value == Long.MIN_VALUE) Long.MAX_VALUE.toULong() + 1u
    else value.absoluteValue.toULong()
}
