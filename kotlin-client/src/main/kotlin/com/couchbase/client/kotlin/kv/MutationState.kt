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

package com.couchbase.client.kotlin.kv;

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.msg.kv.MutationTokenAggregator

/**
 * Aggregation of one or more [MutationToken]s for specifying
 * advanced scan consistency requirements for N1QL or FTS queries.
 *
 * Thread-safe.
 */
public class MutationState private constructor(
    private val tokens: MutationTokenAggregator,
) : Iterable<MutationToken> {

    /**
     * Creates an empty mutation state.
     */
    public constructor() : this(MutationTokenAggregator())

    /**
     * Creates a mutation state representing the given tokens.
     */
    public constructor(tokens: Iterable<MutationToken>) : this() {
        tokens.forEach { add(it) }
    }

    /**
     * Adds the given token to this state.
     */
    public fun add(token: MutationToken): Unit = tokens.add(token)

    /**
     * Exports the this mutation state into a universal format,
     * which can be used either to serialize it into a N1QL query
     * or to send it over the network to a different application/SDK.
     */
    public fun export(): Map<String, Any?> = tokens.export()

    internal fun exportForSearch(): Map<String, Any?> = tokens.exportForSearch()

    override fun toString(): String = tokens.toString()

    public companion object {
        /**
         * Parses the serialized form returned by [export].
         */
        public fun from(exported: Map<String, Any?>): MutationState =
            MutationState(MutationTokenAggregator.from(exported))
    }

    override fun iterator(): Iterator<MutationToken> = tokens.iterator()
}
