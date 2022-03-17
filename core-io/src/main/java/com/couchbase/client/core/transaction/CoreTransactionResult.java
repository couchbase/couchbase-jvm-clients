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

package com.couchbase.client.core.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;

import java.time.Duration;
import java.util.Objects;

@Stability.Internal
public class CoreTransactionResult {
    private final CoreTransactionLogger log;
    private final Duration timeTaken;
    private final String transactionId;
    private final boolean unstagingComplete;

    CoreTransactionResult(CoreTransactionLogger log,
                          Duration timeTaken,
                          String transactionId,
                          boolean unstagingComplete) {
        this.log = Objects.requireNonNull(log);
        this.timeTaken = Objects.requireNonNull(timeTaken);
        this.transactionId = Objects.requireNonNull(transactionId);
        this.unstagingComplete = unstagingComplete;
    }

    /**
     * An in-memory log is built up during each transaction.  The application may want to write this to their own logs,
     * for example upon transaction failure.
     */
    public CoreTransactionLogger log() {
        return log;
    }

    /**
     * Returns the total time taken by a transaction.
     */
    public Duration timeTaken() {
        return timeTaken;
    }

    /**
     * Returns the id of this transaction.
     */
    public String transactionId() {
        return transactionId;
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
        return unstagingComplete;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TransactionResult{");
        sb.append("unstagingComplete=");
        sb.append(unstagingComplete);
        sb.append(",totalTime=");
        sb.append(timeTaken.toMillis());
        sb.append("millis");
        sb.append("}");
        return sb.toString();
    }
}
