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
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.transaction.internal.CoreTransactionCommitAmbiguousException
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException

/**
 * The transaction failed to reach the Committed point.
 *
 * No actors can see any changes made by this transaction.
 */
public open class TransactionFailedException(
    private val wrapped: CoreTransactionFailedException,
) : CouchbaseException(wrapped.message, wrapped.cause) {

    /**
     * Returns the in-memory log built up during each transaction.
     * The application may want to write this to their own logs, for example upon transaction failure.
     */
    public val logs: List<TransactionLogEvent>
        get() = wrapped.logger().logs()

    public val transactionId: String
        get() = wrapped.transactionId()

    internal companion object {
        internal fun convertTransactionFailedInternal(err: Throwable) : Throwable {
            return when (err) {
                is CoreTransactionCommitAmbiguousException -> TransactionCommitAmbiguousException(err)
                is CoreTransactionExpiredException -> TransactionExpiredException(err)
                is CoreTransactionFailedException -> TransactionFailedException(err)
                else -> err
            }
        }
    }
}
