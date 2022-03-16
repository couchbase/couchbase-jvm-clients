/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.transactions;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.transaction.CoreTransactionResult;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Provides some debugging and logging facilities for tracking what happened during a transaction.
 * <p>
 * Note that the success or failure of a transaction is determined solely by whether an a {@link
 * com.couchbase.client.java.transactions.error.TransactionFailedException} exception is thrown by it.
 */
public class TransactionResult {
    private final CoreTransactionResult internal;

    @Stability.Internal
    public TransactionResult(final CoreTransactionResult internal) {
        this.internal = Objects.requireNonNull(internal);
    }

    /**
     * An in-memory log is built up during each transaction.  The application may want to write this to their own logs,
     * for example upon transaction failure.
     */
    public List<TransactionLogEvent> logs() {
        return internal.log().logs();
    }

    /**
     * Returns the total time taken by a transaction.
     */
    public Duration timeTaken() {
        return internal.timeTaken();
    }

    /**
     * Returns the id of this transaction.
     */
    public String transactionId() {
        return internal.transactionId();
    }

    /**
     * Returns whether all documents were successfully unstaged (committed).
     *
     * This will only return true if the transaction reached the COMMIT point and then went on to reach
     * the COMPLETE point.
     *
     * It will be false for transactions that:
     * - Rolled back
     * - Were read-only
     */
    public boolean unstagingComplete() {
        return internal.unstagingComplete();
    }

    @Override
    public String toString() {
        return internal.toString();
    }
}
