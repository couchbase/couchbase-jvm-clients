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
package com.couchbase.client.core.transaction.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;

import java.util.Objects;
import java.util.Optional;

@Stability.Internal
public class CoreSingleQueryTransactionOptions {
    private final Optional<DurabilityLevel> durabilityLevel;
    private final Optional<TransactionAttemptContextFactory> attemptContextFactory;
    private final Optional<CollectionIdentifier> metadataCollection;

    public Optional<DurabilityLevel> durabilityLevel() {
        return durabilityLevel;
    }

    public Optional<TransactionAttemptContextFactory> attemptContextFactory() {
        return attemptContextFactory;
    }

    public Optional<CollectionIdentifier> metadataCollection() {
        return metadataCollection;
    }

    public CoreSingleQueryTransactionOptions(Optional<DurabilityLevel> durabilityLevel,
                                             Optional<TransactionAttemptContextFactory> attemptContextFactory,
                                             Optional<CollectionIdentifier> metadataCollection) {
        this.attemptContextFactory = Objects.requireNonNull(attemptContextFactory);
        this.metadataCollection = Objects.requireNonNull(metadataCollection);
        this.durabilityLevel = Objects.requireNonNull(durabilityLevel);
    }
}
