/*
 * Copyright 2025 Couchbase, Inc.
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
package com.couchbase.client.java.transactions.getmulti;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.getmulti.CoreTransactionGetMultiMode;

import static java.util.Objects.requireNonNull;

@Stability.Uncommitted
public enum TransactionGetMultiReplicasFromPreferredServerGroupMode {
    /**
     * Some time-bounded effort will be made to detect and avoid read skew.
     */
    PRIORITISE_LATENCY(CoreTransactionGetMultiMode.PRIORITISE_LATENCY),

    /**
     * No read skew detection will be attempted.  Once the documents are fetched they will be returned immediately.
     */
    DISABLE_READ_SKEW_DETECTION(CoreTransactionGetMultiMode.DISABLE_READ_SKEW_DETECTION),

    /**
     * Great effort will be made to detect and avoid read skew.
     */
    PRIORITISE_READ_SKEW_DETECTION(CoreTransactionGetMultiMode.PRIORITISE_READ_SKEW_DETECTION)
    ;

    private final CoreTransactionGetMultiMode mode;

    TransactionGetMultiReplicasFromPreferredServerGroupMode(CoreTransactionGetMultiMode mode) {
        this.mode = requireNonNull(mode);
    }

    @Stability.Internal
    public CoreTransactionGetMultiMode toCore() {
        return mode;
    }
}
