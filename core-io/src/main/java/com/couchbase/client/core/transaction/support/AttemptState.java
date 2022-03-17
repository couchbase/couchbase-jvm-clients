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

package com.couchbase.client.core.transaction.support;

import com.couchbase.client.core.annotation.Stability;

/**
 * The possible states for a transaction attempt.
 */
@Stability.Internal
public enum AttemptState {
    /**
     * The attempt did not create an ATR entry.
     *
     * This generally, but not always, implies a failure state - e.g. the ATR entry could not be written.  But, it could
     * have been a successful read-only transaction.  The ATR entry is only created for write transactions.
     */
    NOT_STARTED,

    /**
     * Any call to one of the mutation methods - <code>insert</code>, <code>replace</code>, <code>remove</code> - will update the
     * state to PENDING.
     */
    PENDING,

    /**
     * Set once the Active Transaction Record entry for this transaction has been updated to mark the transaction as
     * Aborted.
     */
    ABORTED,

    /**
     * Set once the Active Transaction Record entry for this transaction has been updated to mark the transaction as
     * Committed.
     */
    COMMITTED,

    /**
     * Set once the commit is fully completed.
     */
    COMPLETED,

    /**
     * Set once the commit is fully rolled back.
     */
    ROLLED_BACK,

    /**
     * A state this client doesn't recognise.
     */
    UNKNOWN;

    public static AttemptState convert(String input) {
        try {
            return AttemptState.valueOf(input);

        } catch (IllegalArgumentException err) {
            return AttemptState.UNKNOWN;
        }
    }
}
