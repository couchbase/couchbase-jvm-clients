/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.kotlin.env.dsl

import com.couchbase.client.kotlin.Keyspace
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

@ClusterEnvironmentDslMarker
public class TransactionsCleanupConfigDslBuilder {
    /**
     * Controls whether a background thread is created to clean up any transaction attempts made by this client.
     *
     * Defaults to true.
     *
     * This should typically be left at the default value.
     *
     * If false, this client's transactions will only be cleaned up
     * by the lost attempts cleanup process, which is by necessity slower.
     */
    public var cleanupClientAttempts: Boolean = true

    /**
     * Controls whether a background process is created to clean up any 'lost' transaction attempts.
     *
     * Defaults to true.
     *
     * This should typically be left at the default value, because cleanup is an essential part of Couchbase
     * transactions.
     */
    public var cleanupLostAttempts: Boolean = true


    /**
     * Determines how frequently the SDK scans for 'lost' transaction attempts
     * when [cleanupLostAttempts] is true.
     *
     * Defaults to 1 minute.
     *
     * The default value balances promptness of lost transaction discovery
     * against the cost of doing the scan, and is suitable for most environments.
     *
     * An application that prefers to discover lost transactions sooner may reduce this value
     * at the cost of increased resource usage on client and server.
     */
    public var cleanupWindow: Duration = 1.minutes

    /**
     * The initial set of transaction metadata collections to clean up.
     * Has no effect if transaction cleanup is explicitly disabled.
     *
     * Defaults to an empty set.
     *
     * Specifying at least one collection has the side effect of starting cleanup immediately,
     * instead of deferring cleanup until the first transaction starts.
     *
     * Note that [TransactionsConfigDslBuilder.metadataCollection] does not need to be specified here,
     * since it's always cleaned up (unless transaction cleanup is explicitly disabled).
     */
    public var collections: Set<Keyspace> = emptySet()
}
