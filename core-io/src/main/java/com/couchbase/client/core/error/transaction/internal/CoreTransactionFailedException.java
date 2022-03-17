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

package com.couchbase.client.core.error.transaction.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;

/**
 * The transaction failed to reach the Committed point.
 *
 * No actors can see any changes made by this transaction.
 */
@Stability.Internal
public class CoreTransactionFailedException extends CouchbaseException {
    private final CoreTransactionLogger logger;
    private final String transactionId;

    public CoreTransactionFailedException(Throwable cause, CoreTransactionLogger logger, String transactionId) {
        this(cause, logger, transactionId, ("Transaction has failed with cause '" + (cause == null ? "unknown" : cause.toString()) + "'"));
    }

    public CoreTransactionFailedException(Throwable cause, CoreTransactionLogger logger, String transactionId, String msg) {
        super(msg, cause);
        this.logger = logger;
        this.transactionId = transactionId;
    }

    public CoreTransactionLogger logger() {
        return logger;
    }

    public String transactionId() {
        return transactionId;
    }
}
