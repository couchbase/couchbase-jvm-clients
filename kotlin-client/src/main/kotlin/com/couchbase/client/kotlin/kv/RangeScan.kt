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

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.kv.RangeScanOrchestrator
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.internal.isEmpty
import com.couchbase.client.kotlin.kv.KvScanConsistency.Companion.consistentWith
import com.couchbase.client.kotlin.kv.KvScanConsistency.Companion.notBounded
import com.couchbase.client.kotlin.util.StorageSize
import com.couchbase.client.kotlin.util.StorageSize.Companion.bytes

@VolatileCouchbaseApi
public val DEFAULT_SCAN_BATCH_SIZE_LIMIT: StorageSize = RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_BYTE_LIMIT.bytes

@VolatileCouchbaseApi
public const val DEFAULT_SCAN_BATCH_ITEM_LIMIT: Int = RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_ITEM_LIMIT

/**
 * Specifies the type of scan to do (range scan or sample scan)
 * and associated parameters.
 *
 * Create an instance using the [ScanType.prefix], [ScanType.range],
 * or [ScanType.sample] companion factory methods.
 */
@VolatileCouchbaseApi
public sealed class ScanType {
    public class Range internal constructor(
        public val from: ScanTerm?,
        public val to: ScanTerm?,
    ) : ScanType()

    public class Sample internal constructor(
        public val limit: Long,
        public val seed: Long?,
    ) : ScanType() {
        init {
            require(limit > 0) { "Sample size limit must be > 0 but got $limit" }
        }
    }

    public class Prefix internal constructor(
        public val prefix: String
    ) : ScanType()

    public companion object {
        /**
         * Selects all document IDs in a range.
         *
         * @param from Start of the range (lower bound) or null for unbounded.
         * @param to End of the range (upper bound) or null for unbounded.
         */
        public fun range(
            from: ScanTerm? = null,
            to: ScanTerm? = null,
        ): Range = Range(
            from = from,
            to = to,
        )

        /**
         * Selects all document IDs with the given [prefix].
         */
        public fun prefix(prefix: String): Prefix = Prefix(prefix)

        /**
         * Selects random documents.
         *
         * @param limit Upper bound (inclusive) on the number of items to select.
         * @param seed Seed for the random number generator that selects the items, or null to use a random seed.
         * **CAVEAT:** Specifying the same seed does not guarantee the same documents are selected.
         */
        public fun sample(
            limit: Long,
            seed: Long? = null,
        ): Sample = Sample(
            limit = limit,
            seed = seed,
        )
    }
}

/**
 * A lower or upper bound of a KV range scan.
 *
 * For a convenient way to create a ScanTerm, see [String.inclusive] and [String.exclusive].
 */
@VolatileCouchbaseApi
public class ScanTerm(
    public val term: String,
    public val exclusive: Boolean = false,
) {
    override fun toString(): String {
        return "ScanTerm(term='$term', exclusive=$exclusive)"
    }

    public companion object {
        public inline val String.inclusive: ScanTerm get() = ScanTerm(this, false)
        public inline val String.exclusive: ScanTerm get() = ScanTerm(this, true)
    }
}

/**
 * Specifies whether to wait for certain KV mutations to be indexed
 * before starting the scan.
 *
 * Create instances using the [consistentWith] or [notBounded]
 * factory methods.
 */
@VolatileCouchbaseApi
public sealed class KvScanConsistency(
    internal val mutationState: MutationState?,
) {
    public companion object {
        /**
         * For when speed matters more than consistency. Executes the scan
         * immediately, without waiting for prior K/V mutations to be indexed.
         */
        public fun notBounded(): KvScanConsistency =
            NotBounded

        /**
         * Targeted consistency. Waits for specific K/V mutations to be indexed
         * before executing the scan.
         *
         * Sometimes referred to as "At Plus".
         *
         * @param tokens the mutations to await before executing the scan
         */
        public fun consistentWith(tokens: MutationState): KvScanConsistency =
            if (tokens.isEmpty()) NotBounded else ConsistentWith(tokens)
    }

    private object NotBounded : KvScanConsistency(null)

    private class ConsistentWith internal constructor(
        tokens: MutationState,
    ) : KvScanConsistency(tokens)
}
