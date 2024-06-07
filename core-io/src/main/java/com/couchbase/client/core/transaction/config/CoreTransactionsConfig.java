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
import com.couchbase.client.core.transaction.cleanup.CleanerFactory;
import com.couchbase.client.core.transaction.cleanup.ClientRecordFactory;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Stability.Internal
public class CoreTransactionsConfig {

    public static final Duration DEFAULT_TRANSACTION_TIMEOUT = Duration.ofSeconds(15);
    public static final DurabilityLevel DEFAULT_TRANSACTION_DURABILITY_LEVEL = DurabilityLevel.MAJORITY;

    private final DurabilityLevel durabilityLevel;
    private final Duration timeout;
    private final CoreTransactionsCleanupConfig cleanupConfig;
    private final TransactionAttemptContextFactory attemptContextFactory;
    private final CleanerFactory cleanerFactory;
    private final ClientRecordFactory clientRecordFactory;
    private final int numAtrs;
    private final Optional<CollectionIdentifier> metadataCollection;
    // Note this isn't a top-level option in TransactionConfig.Builder (correctly).  Just saving creating a
    // TransactionConfigQuery internal object.
    private final Optional<String> scanConsistency;
    private final CoreTransactionsSupportedExtensions supported;

    public CoreTransactionsConfig(DurabilityLevel durabilityLevel,
                                  Duration timeout,
                                  CoreTransactionsCleanupConfig cleanupConfig,
                                  @Nullable TransactionAttemptContextFactory attemptContextFactory,
                                  @Nullable CleanerFactory cleanerFactory,
                                  @Nullable ClientRecordFactory clientRecordFactory,
                                  int numAtrs,
                                  Optional<CollectionIdentifier> metadataCollection,
                                  Optional<String> scanConsistency,
                                  CoreTransactionsSupportedExtensions supported
    ) {
        this.durabilityLevel = Objects.requireNonNull(durabilityLevel);
        this.timeout = Objects.requireNonNull(timeout);
        this.cleanupConfig = Objects.requireNonNull(cleanupConfig);
        this.attemptContextFactory = attemptContextFactory == null ? new TransactionAttemptContextFactory() : attemptContextFactory;
        this.cleanerFactory = cleanerFactory == null ? new CleanerFactory() : cleanerFactory;
        this.clientRecordFactory = clientRecordFactory == null ? new ClientRecordFactory() : clientRecordFactory;
        this.numAtrs = numAtrs;
        this.metadataCollection = Objects.requireNonNull(metadataCollection);
        this.scanConsistency = Objects.requireNonNull(scanConsistency);
        this.supported = Objects.requireNonNull(supported);

        metadataCollection.ifPresent(mc -> cleanupConfig.cleanupSet().add(mc));
    }

    @Stability.Internal
    public static CoreTransactionsConfig createDefault(CoreTransactionsSupportedExtensions supported) {
        return new CoreTransactionsConfig(
                DEFAULT_TRANSACTION_DURABILITY_LEVEL,
                DEFAULT_TRANSACTION_TIMEOUT,
                CoreTransactionsCleanupConfig.createDefault(),
                new TransactionAttemptContextFactory(),
                new CleanerFactory(),
                new ClientRecordFactory(),
                ActiveTransactionRecordIds.NUM_ATRS_DEFAULT,
                Optional.empty(),
                Optional.empty(),
                supported
        );
    }

    public static CoreTransactionsConfig createForSingleQueryTransactions(DurabilityLevel durabilityLevel,
                                                                          Duration timeout,
                                                                          TransactionAttemptContextFactory transactionAttemptContextFactory,
                                                                          Optional<CollectionIdentifier> metadataCollection,
                                                                          CoreTransactionsSupportedExtensions supported) {
        return new CoreTransactionsConfig(durabilityLevel,
                timeout,
                CoreTransactionsCleanupConfig.createForSingleQueryTransactions(),
                transactionAttemptContextFactory,
                null,
                null,
                ActiveTransactionRecordIds.NUM_ATRS_DEFAULT,
                metadataCollection,
                Optional.empty(),
                supported);
    }

    public CoreTransactionsCleanupConfig cleanupConfig() {
        return cleanupConfig;
    }

    public Duration transactionExpirationTime() {
        return timeout;
    }

    public DurabilityLevel durabilityLevel() {
        return durabilityLevel;
    }

    public TransactionAttemptContextFactory attemptContextFactory() { return attemptContextFactory; }

    public CleanerFactory cleanerFactory() { return cleanerFactory; }

    public ClientRecordFactory clientRecordFactory() { return clientRecordFactory; }

    public int numAtrs() {
        return numAtrs;
    }

    public Optional<CollectionIdentifier> metadataCollection() {
        return metadataCollection;
    }

    public Optional<String> scanConsistency() {
        return scanConsistency;
    }

    public CoreTransactionsSupportedExtensions supported() {
        return supported;
    }

    @Stability.Volatile
    public Map<String, Object> exportAsMap() {
        Map<String, Object> export = new LinkedHashMap<>();
        export.put("durabilityLevel", durabilityLevel.name());
        export.put("timeoutMs", timeout.toMillis());
        export.put("cleanupConfig", cleanupConfig.exportAsMap());
        export.put("numAtrs", numAtrs);
        export.put("metadataCollection", metadataCollection.map(CollectionIdentifier::toString).orElse("none"));
        export.put("scanConsistency", scanConsistency.orElse("none"));
        return export;
    }
}
