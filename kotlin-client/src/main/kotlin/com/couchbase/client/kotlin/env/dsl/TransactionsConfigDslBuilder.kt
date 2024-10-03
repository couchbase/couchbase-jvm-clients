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

import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds
import com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions
import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.transactions.toCollectionIdentifier
import java.util.Optional
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration


internal fun checkTransactionDurability(value: Durability?) {
    require(value !is Durability.ClientVerified) { "Transaction durability must not be ClientVerified." }
    require(value !is Durability.None && value !is Durability.Disabled) { "Transaction durability must not be None." }
}

@ClusterEnvironmentDslMarker
public class TransactionsConfigDslBuilder {
    /**
     * Default durability level for all writes in transactions.
     *
     * The initial value is [Durability.majority], meaning a transaction will pause on each write
     * until it is available in-memory on a majority of configured replicas.
     *
     * Must not be [Durability.none] or [Durability.clientVerified], which are not compatible with transactions.
     */
    public var durabilityLevel: Durability = Durability.majority()
        set(value) {
            checkTransactionDurability(value)
            field = value
        }

    /**
     * Default maximum time a transaction can run for.
     *
     * The initial value is 15 seconds.
     *
     * After this time, the transaction will abort. Note that this could be mid-commit, in which case the cleanup process
     * will complete the transaction asynchronously at a later point.
     *
     * Applications can increase or decrease this as desired. The trade-off to understand is that documents
     * being mutated in a transaction `A` are effectively locked from being updated by other transactions until
     * transaction `A` has completed (committed or rolled back). If transaction `A` is unable to complete for whatever
     * reason, the document can be locked for this [timeout] time.
     */
    public var timeout: Duration = 15.seconds
        set(value) {
            require(value.inWholeMilliseconds > 0) { "Transaction timeout must be at least 1 millisecond, but got $value" }
            field = value
        }

    /**
     * Default collection for storing transaction metadata documents.
     *
     * The initial value is null. If null, the documents are stored in the default collection
     * of the bucket containing the first mutated document in the transaction.
     *
     * This collection is automatically added to the set of collections to clean up.
     */
    public var metadataCollection: Keyspace? = null

    /**
     * Configure options related to transaction cleanup.
     */
    private val cleanupConfigDlsBuilder = TransactionsCleanupConfigDslBuilder()
    public fun cleanup(initializer: TransactionsCleanupConfigDslBuilder.() -> Unit) {
        cleanupConfigDlsBuilder.initializer()
    }

    internal fun toCore(): CoreTransactionsConfig {
        return CoreTransactionsConfig(
            durabilityLevel.toCore().levelIfSynchronous().orElseThrow { NoSuchElementException() },
            timeout.toJavaDuration(),
            with(cleanupConfigDlsBuilder) {
                CoreTransactionsCleanupConfig(
                    cleanupLostAttempts,
                    cleanupClientAttempts,
                    cleanupWindow.toJavaDuration(),
                    collections.map { it.toCollectionIdentifier() }.toSet()
                )
            },
            null, // txn attempt context factory
            null, // cleaner factory
            null, // client record factory
            ActiveTransactionRecordIds.NUM_ATRS_DEFAULT,
            metadataCollection?.toCollectionIdentifier().toOptional(),
            Optional.empty(), // For Kotlin, scan consistency is specified on each query request.
            CoreTransactionsSupportedExtensions.ALL,
        )
    }
}

