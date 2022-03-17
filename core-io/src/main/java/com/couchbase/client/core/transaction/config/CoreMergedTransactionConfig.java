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
import com.couchbase.client.core.transaction.cleanup.CleanerFactory;
import com.couchbase.client.core.transaction.cleanup.ClientRecordFactory;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Merges {@link CoreTransactionOptions} and {@link CoreTransactionsConfig}.
 */
@Stability.Internal
public class CoreMergedTransactionConfig {
    private final CoreTransactionsConfig config;
    private final Optional<CoreTransactionOptions> perConfig;

    public CoreMergedTransactionConfig(CoreTransactionsConfig config) {
        this(config, Optional.empty());
    }

    public CoreMergedTransactionConfig(CoreTransactionsConfig config, Optional<CoreTransactionOptions> perConfig) {
        this.config = Objects.requireNonNull(config);
        this.perConfig = Objects.requireNonNull(perConfig);
    }

    public Optional<String> scanConsistency() {
        if (perConfig.flatMap(CoreTransactionOptions::scanConsistency).isPresent()) {
            return perConfig.flatMap(CoreTransactionOptions::scanConsistency);
        }
        return config.scanConsistency();
    }

    public Optional<RequestSpan> parentSpan() {
        return perConfig.flatMap(CoreTransactionOptions::parentSpan);
    }

    public Duration expirationTime() {
        return perConfig.flatMap(CoreTransactionOptions::timeout).orElse(config.transactionExpirationTime());
    }

    public CoreTransactionsCleanupConfig cleanupConfig() {
        return config.cleanupConfig();
    }

    public DurabilityLevel durabilityLevel() {
        return perConfig.flatMap(CoreTransactionOptions::durabilityLevel).orElse(config.durabilityLevel());
    }

    public TransactionAttemptContextFactory attemptContextFactory() { return perConfig.flatMap(CoreTransactionOptions::attemptContextFactory).orElse(config.attemptContextFactory()); }

    public CleanerFactory cleanerFactory() { return config.cleanerFactory(); }

    public ClientRecordFactory clientRecordFactory() { return config.clientRecordFactory(); }

    public int numAtrs() {
        return config.numAtrs();
    }

    public Optional<CollectionIdentifier> metadataCollection() {
        if (perConfig.isPresent() && perConfig.get().metadataCollection().isPresent()) {
            return perConfig.get().metadataCollection();
        }
        return config.metadataCollection();
    }
}
