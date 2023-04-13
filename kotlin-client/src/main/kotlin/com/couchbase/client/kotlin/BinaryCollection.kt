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

package com.couchbase.client.kotlin

import com.couchbase.client.core.CoreKeyspace
import com.couchbase.client.core.error.CasMismatchException
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.kotlin.kv.CounterResult
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.MutationResult
import java.util.Optional

public class BinaryCollection internal constructor(
    collection: Collection
) {
    private val ops = collection.couchbaseOps.kvBinaryOps(CoreKeyspace.from(collection.collectionId))

    /**
     * Appends binary content to a document.
     *
     * @param id the ID of the document to modify.
     * @param content the binary content to append to the document.
     * @throws DocumentNotFoundException if the document is not found in the collection.
     * @throws CasMismatchException if a non-zero CAS is specified and the document has been concurrently modified on the server.
     */
    public suspend fun append(
        id: String,
        content: ByteArray,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.none(),
        cas: Long = 0,
    ): MutationResult {
        ops.appendAsync(
            id,
            content,
            common.toCore(),
            cas,
            durability.toCore()
        ).await().let {
            return MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    /**
     * Prepends binary content to a document.
     *
     * @param id the ID of the document to modify.
     * @param content the binary content to prepend to the document.
     * @throws DocumentNotFoundException if the document is not found in the collection.
     * @throws CasMismatchException if a non-zero CAS is specified and the document has been concurrently modified on the server.
     */
    public suspend fun prepend(
        id: String,
        content: ByteArray,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.none(),
        cas: Long = 0,
    ): MutationResult {
        ops.prependAsync(
            id,
            content,
            common.toCore(),
            cas,
            durability.toCore()
        ).await().let {
            return MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    /**
     * Increments a counter document and returns the new value.
     *
     * This operation is atomic with respect to a single Couchbase Server cluster,
     * but not between clusters when Cross-Datacenter Replication (XDCR) is used.
     *
     * If the counter document does not exist, the behavior depends on the
     * `initialValue` argument. If null, [DocumentNotFoundException] is thrown.
     * Otherwise, the document is created with the initial value, and this initial
     * value is returned regardless of the `delta` argument.
     *
     * The content of a counter document is a single JSON integer with
     * a minimum value of zero and a maximum value of 2^64 - 1.
     *
     * A counter incremented above 2^64 - 1 will overflow (wrap around).
     *
     * Counter values above 2^53 - 1 may have interoperability issues with
     * languages that store all numbers as floating point values.
     *
     * @param initialValue if the counter document does not exist, it will be created
     * with this value and this value will be returned, ignoring `delta`.
     * If null and the document does not exist, [DocumentNotFoundException]
     * is thrown.
     *
     * @param delta the number to add to the counter. A value of zero
     * returns the current value of the counter. Ignored if the counter document
     * does not already exist, in which case the returned value is determined
     * by `initialValue`.
     *
     * @param expiry The expiry to assign to the counter if the document does
     * not already exist. If the document already exists, this argument is
     * ignored and the initial expiry is preserved.
     *
     * @param durability The durability requirement for the document update.
     */
    public suspend fun increment(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.none(),
        expiry: Expiry = Expiry.none(),
        delta: ULong = 1u,
        initialValue: ULong? = delta,
    ): CounterResult {
        ops.incrementAsync(
            id,
            common.toCore(),
            expiry.encode(),
            delta.toLong(),
            Optional.ofNullable(initialValue?.toLong()),
            durability.toCore()
        ).await().let {
            return CounterResult(it.cas(), it.mutationToken().orElse(null), it.content().toULong())
        }
    }

    /**
     * Decrements a counter document and returns the new value.
     *
     * This operation is atomic with respect to a single Couchbase Server cluster,
     * but not between clusters when Cross-Datacenter Replication (XDCR) is used.
     *
     * If the counter document does not exist, the behavior depends on the
     * `initialValue` argument. If null, [DocumentNotFoundException] is thrown.
     * Otherwise the document is created with the initial value and this initial
     * value is returned and the `delta` argument is ignored.
     *
     * The content of a counter document is a single JSON integer with
     * a minimum value of zero and a maximum value of 2^64 - 1.
     *
     * A counter decremented below zero will reset to zero.
     *
     * Counter values above 2^53 - 1 may have interoperability issues with
     * languages that store all numbers as floating point values.
     *
     * @param initialValue if the counter document does not exist, it will be created
     * with this value and this value will be returned, ignoring `delta`.
     * If null and the document does not exist, [DocumentNotFoundException]
     * is thrown.
     *
     * @param delta the number to subtract from to the counter. A value of zero
     * returns the current value of the counter. Ignored if the counter document
     * does not already exist, in which case the returned value is determined
     * by `initialValue`.
     *
     * @param expiry The expiry to assign to the counter if the document does
     * not already exist. If the document already exists, this argument is
     * ignored and the initial expiry is preserved.
     *
     * @param durability The durability requirement for the document update.
     */
    public suspend fun decrement(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.none(),
        expiry: Expiry = Expiry.none(),
        delta: ULong = 1u,
        initialValue: ULong? = 0u,
    ): CounterResult {
        ops.decrementAsync(
            id,
            common.toCore(),
            expiry.encode(),
            delta.toLong(),
            Optional.ofNullable(initialValue?.toLong()),
            durability.toCore()
        ).await().let {
            return CounterResult(it.cas(), it.mutationToken().orElse(null), it.content().toULong())
        }
    }
}


