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

package com.couchbase.client.java.transactions.error;

import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;

import java.util.List;

/**
 * The transaction failed to reach the Committed point.
 *
 * No actors can see any changes made by this transaction.
 */
public class TransactionFailedException extends CouchbaseException {
    private final CoreTransactionFailedException internal;

    public TransactionFailedException(CoreTransactionFailedException e) {
        super(e.getMessage(), e.getCause());
        this.internal = e;
    }

    /**
     * An in-memory log is built up during each transaction.  The application may want to write this to their own logs,
     * for example upon transaction failure.
     */
    public List<TransactionLogEvent> logs() {
        return internal.logger().logs();
    }

    public String transactionId() {
        return internal.transactionId();
    }
}
