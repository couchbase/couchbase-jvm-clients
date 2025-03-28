/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.transactions

import com.couchbase.client.core.transaction.getmulti.CoreTransactionGetMultiMode

public sealed class TransactionGetMultiMode {
    internal abstract fun toCore() : CoreTransactionGetMultiMode

    private data object DisableReadSkewDetection : TransactionGetMultiMode() {
        override fun toCore(): CoreTransactionGetMultiMode = CoreTransactionGetMultiMode.DISABLE_READ_SKEW_DETECTION
    }

    private data object PrioritizeLatency : TransactionGetMultiMode() {
        override fun toCore(): CoreTransactionGetMultiMode = CoreTransactionGetMultiMode.PRIORITISE_LATENCY
    }

    private data object PrioritizeReadSkewDetection : TransactionGetMultiMode() {
        override fun toCore(): CoreTransactionGetMultiMode = CoreTransactionGetMultiMode.PRIORITISE_READ_SKEW_DETECTION
    }

    public companion object {
        /**
         * No read skew detection will be attempted. Once the documents are fetched they will be returned immediately.
         */
        public fun disableReadSkewDetection() : TransactionGetMultiMode = DisableReadSkewDetection

        /**
         * Some time-bounded effort will be made to detect and avoid read skew.
         */
        public fun prioritizeLatency() : TransactionGetMultiMode = PrioritizeLatency

        /**
         * Great effort will be made to detect and avoid read skew.
         */
        public fun prioritizeReadSkewDetection() : TransactionGetMultiMode = PrioritizeReadSkewDetection
    }
}
