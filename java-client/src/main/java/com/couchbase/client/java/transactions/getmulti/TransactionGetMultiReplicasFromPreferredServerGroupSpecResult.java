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
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.transactions.TransactionGetResult;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class TransactionGetMultiReplicasFromPreferredServerGroupSpecResult {
    private final TransactionGetMultiReplicasFromPreferredServerGroupSpec spec;
    private final Optional<TransactionGetResult> internal;

    @Stability.Internal
    public TransactionGetMultiReplicasFromPreferredServerGroupSpecResult(TransactionGetMultiReplicasFromPreferredServerGroupSpec spec,
                                                                         Optional<CoreTransactionGetResult> r,
                                                                         JsonSerializer serializer,
                                                                         @Nullable Transcoder transcoder) {
        this.internal = requireNonNull(r).map(v -> new TransactionGetResult(v, serializer, transcoder));
        this.spec = requireNonNull(spec);
    }

    public TransactionGetMultiReplicasFromPreferredServerGroupSpec spec() {
        return spec;
    }

    public TransactionGetResult get() {
        if (!internal.isPresent()) {
            throw new CouchbaseException("Called get() on but document " + spec + " is not present!");
        }
        return internal.get();
    }

    public boolean exists() {
        return internal.isPresent();
    }
}
