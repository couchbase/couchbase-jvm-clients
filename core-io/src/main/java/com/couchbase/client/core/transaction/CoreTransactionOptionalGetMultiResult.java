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

package com.couchbase.client.core.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.components.CoreTransactionGetMultiSpec;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a CoreTransactionGetResult with the CoreTransactionGetMultiSpec it's for, to make it easier
 * to sort the results just once at the end.
 * <p>
 * N.b. this is not the TransactionGetMultiResult required by the spec: that gets
 * produced later, in the wrappers.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Stability.Internal
public class CoreTransactionOptionalGetMultiResult implements Comparable<CoreTransactionOptionalGetMultiResult> {
    public final CoreTransactionGetMultiSpec spec;
    public final Optional<CoreTransactionGetResult> internal;

    public CoreTransactionOptionalGetMultiResult(CoreTransactionGetMultiSpec spec, Optional<CoreTransactionGetResult> r) {
        this.internal = requireNonNull(r);
        this.spec = requireNonNull(spec);
    }

    public CoreTransactionGetMultiResult get() {
        return new CoreTransactionGetMultiResult(spec, internal.get());
    }

    @Override
    public int compareTo(CoreTransactionOptionalGetMultiResult other) {
        return Integer.compare(spec.specIndex, other.spec.specIndex);
    }

    public boolean isPresent() {
        return internal.isPresent();
    }

    @SuppressWarnings({"OptionalGetWithoutIsPresent", "DataFlowIssue"}) // Ok due to isInTransaction check
    public CoreTransactionOptionalGetMultiResult convertToPostTransaction() {
        return new CoreTransactionOptionalGetMultiResult(spec, internal.map(r -> {
            if (r.isInTransaction()) {
                return CoreTransactionGetResult.createFrom(r, r.links().stagedContentJsonOrBinary().get());
            }
            else {
                return r;
            }
        }));
    }
}
