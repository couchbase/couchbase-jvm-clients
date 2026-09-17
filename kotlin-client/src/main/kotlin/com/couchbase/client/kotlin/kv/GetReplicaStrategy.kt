/*
 * Copyright 2026 Couchbase, Inc.
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

import com.couchbase.client.core.api.kv.CoreGetReplicaStrategy
import com.couchbase.client.core.api.kv.CoreReplicaIndex
import com.couchbase.client.core.service.kv.CoreGetReplicaStrategyFromIndex
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi

public sealed class GetReplicaStrategy {
    internal abstract fun toCore(): CoreGetReplicaStrategy

    public companion object {
        /**
         * Returns a strategy that reads from the specified replica.
         *
         * Set [wrap] to true if you want to efficiently read from a single replica,
         * but don't necessarily care which one.
         *
         * When wrap is true:
         *
         * - The specified replica index, modulo the actual number of replicas configured on the bucket,
         *   is the starting point for a scan of all available replicas; the result may come from a different replica index than the one specified.
         * - [com.couchbase.client.core.error.ReplicaIndexOutOfBoundsException] is not thrown unless there are no replicas.
         * - [com.couchbase.client.core.error.ReplicaIndexCurrentlyUnavailableException] is not thrown unless all replicas are currently unavailable.
         */
        public fun fromIndex(
            index: ReplicaIndex,
            wrap: Boolean = false,
        ) : FromIndex = FromIndex(index, wrap)

        /**
         * Returns a strategy that reads from a single available replica.
         *
         * The selection algorithm is unspecified.
         */
        @VolatileCouchbaseApi
        public fun any() : GetReplicaStrategy = fromIndex(ReplicaIndex.FIRST, wrap = true)
    }

    public class FromIndex internal constructor(
        private val index: ReplicaIndex,
        private val wrap: Boolean,
    ) : GetReplicaStrategy() {
        override fun toCore(): CoreGetReplicaStrategy = CoreGetReplicaStrategyFromIndex(
            CoreReplicaIndex.entries[index.ordinal],
            wrap,
        )
    }
}
