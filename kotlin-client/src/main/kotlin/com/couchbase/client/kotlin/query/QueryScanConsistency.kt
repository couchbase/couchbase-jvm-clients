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

package com.couchbase.client.kotlin.query

import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.kotlin.internal.isEmpty
import com.couchbase.client.kotlin.kv.MutationState
import com.couchbase.client.kotlin.query.QueryScanConsistency.Companion.consistentWith
import com.couchbase.client.kotlin.query.QueryScanConsistency.Companion.notBounded
import com.couchbase.client.kotlin.query.QueryScanConsistency.Companion.requestPlus
import kotlin.time.Duration

/**
 * Create instances using the [requestPlus], [consistentWith], or [notBounded]
 * factory methods.
 */
public sealed class QueryScanConsistency(
    internal val scanWait: Duration?,
) {

    public companion object {
        /**
         * For when speed matters more than consistency. Executes the query
         * immediately, without waiting for prior K/V mutations to be indexed.
         */
        public fun notBounded(): QueryScanConsistency =
            NotBounded

        /**
         * Strong consistency. Waits for all prior K/V mutations to be indexed
         * before executing the query.
         *
         * @param scanWait max time to wait for the indexer to catch up
         */
        public fun requestPlus(scanWait: Duration? = null): QueryScanConsistency =
            RequestPlus(scanWait)

        /**
         * Targeted consistency. Waits for specific K/V mutations to be indexed
         * before executing the query.
         *
         * Sometimes referred to as "At Plus".
         *
         * @param tokens the mutations to await before executing the query
         * @param scanWait max time to wait for the indexer to catch up
         */
        public fun consistentWith(tokens: MutationState, scanWait: Duration? = null): QueryScanConsistency =
            if (tokens.isEmpty()) NotBounded else ConsistentWith(tokens, scanWait)
    }

    internal object NotBounded : QueryScanConsistency(null)

    internal class RequestPlus internal constructor(scanWait: Duration? = null) :
        QueryScanConsistency(scanWait)

    internal class ConsistentWith internal constructor(
        private val tokens: MutationState,
        scanWait: Duration? = null,
    ) : QueryScanConsistency(scanWait) {
        fun toCore(): CoreMutationState = CoreMutationState(tokens)
    }
}
