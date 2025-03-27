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
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiOptions;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Customize how a getMultiReplicasFromPreferredServerGroup() operation runs.
 */
@Stability.Uncommitted
public class TransactionGetMultiReplicasFromPreferredServerGroupOptions {
    private @Nullable TransactionGetMultiReplicasFromPreferredServerGroupMode mode = null;

    private TransactionGetMultiReplicasFromPreferredServerGroupOptions() {
    }

    public static TransactionGetMultiReplicasFromPreferredServerGroupOptions transactionGetMultiReplicasFromPreferredServerGroupOptions() {
        return new TransactionGetMultiReplicasFromPreferredServerGroupOptions();
    }

    /**
     * Controls how the operation behaves - see {@link TransactionGetMultiMode} for details.
     * <p>
     * If not explicitly set, the default behaviour is intentionally unspecified, and may change in future versions.
     */
    public TransactionGetMultiReplicasFromPreferredServerGroupOptions mode(TransactionGetMultiReplicasFromPreferredServerGroupMode mode) {
        this.mode = requireNonNull(mode);
        return this;
    }


    @Stability.Internal
    public CoreGetMultiOptions build() {
        return new CoreGetMultiOptions(mode == null ? null : mode.toCore());
    }
}
