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
 * This is almost identical to CoreTransactionOptionalGetMultiResult.  It's present so we don't have to deal with optional everywhere.
 */
@Stability.Internal
public class CoreTransactionGetMultiResult {
    public final CoreTransactionGetMultiSpec spec;
    public final CoreTransactionGetResult internal;

    public CoreTransactionGetMultiResult(CoreTransactionGetMultiSpec spec, CoreTransactionGetResult r) {
        this.internal = requireNonNull(r);
        this.spec = requireNonNull(spec);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent") // Ok due to isInTransaction check
    public CoreTransactionGetMultiResult convertToPostTransaction() {
        return new CoreTransactionGetMultiResult(spec,
            internal.isInTransaction()
                    ? CoreTransactionGetResult.createFrom(internal, internal.links().stagedContentJsonOrBinary().get())
                    : internal);
    }

    public CoreTransactionOptionalGetMultiResult toOptional() {
        return new CoreTransactionOptionalGetMultiResult(spec, Optional.of(internal));
    }
}
