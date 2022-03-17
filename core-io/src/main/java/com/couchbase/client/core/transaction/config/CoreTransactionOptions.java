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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Tunables for an individual transaction.
 */
@Stability.Internal
public class CoreTransactionOptions {
    private final Optional<DurabilityLevel> durabilityLevel;
    // Note this isn't a top-level option in PerTransactionConfig.Builder (correctly).  Just saving creating a
    // PerTransactionConfigQuery internal object.
    private final Optional<String> scanConsistency;
    private final Optional<RequestSpan> parentSpan;
    private final Optional<Duration> timeout;
    private final Optional<TransactionAttemptContextFactory> attemptContextFactory;
    private final Optional<CollectionIdentifier> metadataCollection;

    public CoreTransactionOptions(Optional<DurabilityLevel> durabilityLevel,
                                  Optional<String> scanConsistency,
                                  Optional<RequestSpan> parentSpan,
                                  Optional<Duration> timeout,
                                  Optional<CollectionIdentifier> metadataCollection,
                                  Optional<TransactionAttemptContextFactory> attemptContextFactory) {
        this.timeout = Objects.requireNonNull(timeout);
        this.durabilityLevel = Objects.requireNonNull(durabilityLevel);
        this.scanConsistency = Objects.requireNonNull(scanConsistency);
        this.parentSpan = Objects.requireNonNull(parentSpan);
        this.attemptContextFactory = Objects.requireNonNull(attemptContextFactory);
        this.metadataCollection = Objects.requireNonNull(metadataCollection);
    }

    public static CoreTransactionOptions create(RequestSpan parentSpan) {
        return new CoreTransactionOptions(Optional.empty(),
                Optional.empty(),
                Optional.of(parentSpan),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public Optional<DurabilityLevel> durabilityLevel() {
        return durabilityLevel;
    }

    public Optional<String> scanConsistency() {
        return scanConsistency;
    }

    public Optional<RequestSpan> parentSpan() {
        return parentSpan;
    }

    public Optional<Duration> timeout() {
        return timeout;
    }

    public Optional<TransactionAttemptContextFactory> attemptContextFactory() {
        return attemptContextFactory;
    }

    public Optional<CollectionIdentifier> metadataCollection() {
        return metadataCollection;
    }
}
