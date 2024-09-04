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

import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent
import com.couchbase.client.core.transaction.CoreTransactionResult
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

/**
 * Holds the value returned by the transaction lambda, as well as
 * debugging and logging facilities for tracking what happened during the transaction.
 *
 * Note that the success or failure of a transaction is determined solely by whether it threw a
 * [TransactionFailedException].
 *
 * @param V The type of value returned by the transaction lambda
 * @property value The value returned by the transaction lambda
 */
public class TransactionResult<V> internal constructor(
    public val value: V,
    internal val internal: CoreTransactionResult,
) {
    /**
     * The in-memory log built up during each transaction. The application may want to write this to their own logs,
     * for example upon transaction failure.
     */
    public val logs: List<TransactionLogEvent>
        get() = internal.log().logs()

    /**
     * Total time taken by a transaction.
     */
    public val timeTaken: Duration
        get() = internal.timeTaken().toKotlinDuration()

    /**
     * The ID of this transaction.
     */
    public val transactionId: String
        get() = internal.transactionId()

    /**
     * Indicates whether all documents were successfully unstaged (committed).
     *
     * This will only be true if the transaction reached the COMMIT point and then went on to reach
     * the COMPLETE point.
     *
     * It will be false for transactions that:
     * - Rolled back
     * - Were read-only
     */
    public val unstagingComplete: Boolean
        get() = internal.unstagingComplete()

    override fun toString(): String = "TransactionResult(value=$value, internal=$internal)"
}
