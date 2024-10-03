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

package com.couchbase.client.kotlin.transactions

import com.couchbase.client.core.Core
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext
import com.couchbase.client.core.transaction.CoreTransactionsReactive
import com.couchbase.client.core.transaction.config.CoreTransactionOptions
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory
import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.env.dsl.checkTransactionDurability
import com.couchbase.client.kotlin.env.env
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.manager.bucket.levelIfSynchronous
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.mono
import java.util.Optional
import kotlin.time.Duration
import kotlin.time.toJavaDuration


public class Transactions internal constructor(internal val core: Core) {
    private val internal = CoreTransactionsReactive(core, core.env.transactionsConfig())

    /**
     * Runs supplied transactional logic until success or failure.
     *
     * The supplied transactional logic will be run if necessary multiple times, until either:
     *
     * - The transaction successfully commits
     * - The transactional logic requests an explicit rollback
     * - The transaction times out.
     * - An exception is thrown, either inside the transaction library or by the supplied transaction logic,
     *   that cannot be handled.
     *
     * The transaction logic {@link Consumer} is provided an {@link TransactionAttemptContext}, which contains methods allowing it
     * to read, mutate, insert and delete documents, as well as commit or rollback the transaction.
     *
     * If the transaction logic performs a commit or rollback it must be the last operation performed.
     * Else a [TransactionFailedException] will be thrown.
     * Similarly, there cannot be a commit followed by a rollback, or vice versa - this will also raise a [TransactionFailedException].
     *
     * If the transaction logic does not perform an explicit commit or rollback, then a commit will be performed
     * anyway.
     *
     * @param durability Durability level for this transaction, or null to use the cluster environment's default transaction durability.
     * Must not be [Durability.none] or [Durability.clientVerified], which are not compatible with transactions.
     *
     * @param timeout Time allowed for this transaction to complete, or null to use the cluster environment's default timeout duration.
     * @param metadataCollection The location in Couchbase to store metadata this transaction, or null to use the cluster environment's default metadata collection.
     *
     * @return The value returned by the transaction logic, along with diagnostic information.
     *
     * @throws TransactionFailedException or a derived exception if the transaction fails to commit for any reason, possibly
     *                           after multiple retries. The exception contains further details of the error
     */
    public suspend fun <V> run(
        timeout: Duration? = null,
        parentSpan: RequestSpan? = null,
        durability: Durability? = null,
        metadataCollection: Keyspace? = null,
        transactionLogic: suspend TransactionAttemptContext.() -> V,
    ): TransactionResult<V> {
        return runInternal(
            timeout, parentSpan, durability, metadataCollection, null, transactionLogic,
        )
    }

    internal suspend fun <V> runInternal(
        timeout: Duration? = null,
        parentSpan: RequestSpan? = null,
        durability: Durability? = null,
        metadataCollection: Keyspace? = null,
        attemptContextFactory: TransactionAttemptContextFactory?,
        transactionLogic: suspend TransactionAttemptContext.() -> V,
    ): TransactionResult<V> {

        checkTransactionDurability(durability)

        val perConfig = CoreTransactionOptions(
            durability?.levelIfSynchronous() ?: Optional.empty(),
            Optional.empty(), // scan consistency
            parentSpan.toOptional(),
            timeout?.toJavaDuration().toOptional(),
            metadataCollection?.toCollectionIdentifier().toOptional(),
            attemptContextFactory.toOptional(),
        )

        var value: V? = null
        val function = { ctx: CoreTransactionAttemptContext ->
            mono {
                value = transactionLogic(TransactionAttemptContext(ctx, core.env.jsonSerializer))
            }
        }

        try {
            val coreResult = internal.run(function, perConfig).awaitSingle()
            @Suppress("UNCHECKED_CAST")
            return TransactionResult(value as V, coreResult)
        } catch (t: CoreTransactionFailedException) {
            throw TransactionFailedException.convertTransactionFailedInternal(t)
        }
    }
}

internal fun Keyspace.toCollectionIdentifier() =
    CollectionIdentifier(
        bucket,
        Optional.of(scope),
        Optional.of(collection)
    )

internal fun CollectionIdentifier.toKeyspace() =
    Keyspace(
        bucket(),
        scope().orElse(DEFAULT_SCOPE),
        collection().orElse(DEFAULT_COLLECTION)
    )
