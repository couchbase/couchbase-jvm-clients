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

package com.couchbase.client.java.transactions.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.cleanup.CleanerFactory;
import com.couchbase.client.core.transaction.cleanup.ClientRecordFactory;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.transactions.TransactionKeyspace;
import com.couchbase.client.java.transactions.internal.TransactionsSupportedExtensionsUtil;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Provides all configurable parameters for Couchbase transactions.
 */
public class TransactionsConfig {
    private static final Duration TRANSACTION_TIMEOUT_MSECS_DEFAULT = Duration.of(15, ChronoUnit.SECONDS);

    public static Builder builder() {
        return new TransactionsConfig.Builder();
    }

    /**
     * Configures transaction cleanup.
     *
     * @deprecated Instead of creating a new builder, please use
     * {@link ClusterEnvironment.Builder#transactionsConfig(Consumer)}
     * and configure the builder passed to the consumer.
     */
    @Deprecated
    public static Builder cleanupConfig(final TransactionsCleanupConfig.Builder config) {
        return builder().cleanupConfig(config);
    }

    /**
     * Sets the maximum time that transactions can run for.  The default is 15 seconds.
     * After this time, the transaction will abort.  Note that this could be mid-commit, in which case the cleanup process
     * will complete the transaction asynchronously at a later point.
     * <p>
     * Applications can increase or decrease this as desired.  The trade-off to understand is that documents that are
     * being mutated in a transaction A, are effectively locked from being updated by other transactions until
     * transaction A has completed - committed or rolled back.  If transaction A is unable to complete for whatever
     * reason, the document can be locked for this <code>timeout</code> time.
     *
     * @deprecated Instead of creating a new builder, please use
     * {@link ClusterEnvironment.Builder#transactionsConfig(Consumer)}
     * and configure the builder passed to the consumer.
     */
    @Deprecated
    public static Builder timeout(Duration timeout) {
        return builder().timeout(timeout);
    }

    /**
     * All transaction writes will be performed with this durability setting.
     * <p>
     * The default setting is DurabilityLevel.MAJORITY, meaning a transaction will pause on each write
     * until it is available in-memory on a majority of configured replicas.
     * <p>
     * DurabilityLevel.NONE is not supported and provides no ACID transactional guarantees.
     *
     * @deprecated Instead of creating a new builder, please use
     * {@link ClusterEnvironment.Builder#transactionsConfig(Consumer)}
     * and configure the builder passed to the consumer.
     */
    @Deprecated
    public static Builder durabilityLevel(DurabilityLevel level) {
        return builder().durabilityLevel(level);
    }

    /**
     * Allows setting a custom collection to use for any transactional metadata documents.
     * <p>
     * If not set, it will default to creating these documents in the default collection of the bucket that the first
     * mutated document in the transaction is on.
     * <p>
     * This collection will be added to the set of collections being cleaned up.
     *
     * @deprecated Instead of creating a new builder, please use
     * {@link ClusterEnvironment.Builder#transactionsConfig(Consumer)}
     * and configure the builder passed to the consumer.
     */
    @Deprecated
    public static Builder metadataCollection(TransactionKeyspace collection) {
        return builder().metadataCollection(collection);
    }

    /**
     * Sets the default query configuration for all transactions.
     *
     * @param queryConfig the query configuration to use
     * @return this, for chaining
     * @deprecated Instead of creating a new builder, please use
     * {@link TransactionsConfig.Builder#queryConfig(Consumer)}
     * and configure the builder passed to the consumer.
     */
    @Deprecated
    public static Builder queryConfig(TransactionsQueryConfig.Builder queryConfig) {
        return builder().queryConfig(queryConfig);
    }

    public static class Builder {
        private DurabilityLevel level = DurabilityLevel.MAJORITY;
        private Optional<Duration> timeout = Optional.empty();
        private TransactionsCleanupConfig.Builder cleanupConfig = new TransactionsCleanupConfig.Builder();
        private Optional<TransactionAttemptContextFactory> attemptContextFactory = Optional.empty();
        private Optional<CleanerFactory> cleanerFactory = Optional.empty();
        private Optional<ClientRecordFactory> clientRecordFactory = Optional.empty();
        private Optional<Integer> numAtrs = Optional.empty();
        private Optional<CollectionIdentifier> metadataCollection = Optional.empty();
        private TransactionsQueryConfig.Builder queryConfig = new TransactionsQueryConfig.Builder();

        @Stability.Internal
        public CoreTransactionsConfig build() {
            return new CoreTransactionsConfig(
                    level,
                    timeout.orElse(TRANSACTION_TIMEOUT_MSECS_DEFAULT),
                    cleanupConfig.build(),
                    attemptContextFactory.orElse(new TransactionAttemptContextFactory()),
                    cleanerFactory.orElse(new CleanerFactory()),
                    clientRecordFactory.orElse(new ClientRecordFactory()),
                    numAtrs.orElse(ActiveTransactionRecordIds.NUM_ATRS_DEFAULT),
                    metadataCollection,
                    queryConfig.scanConsistency().map(Enum::name),
                    TransactionsSupportedExtensionsUtil.SUPPORTED
            );
        }

        /**
         * Configures transaction cleanup.
         *
         * @deprecated This method clobbers any previously configured values. Please use {@link #cleanupConfig(Consumer)} instead.
         */
        @Deprecated
        public Builder cleanupConfig(final TransactionsCleanupConfig.Builder config) {
            notNull(config, "cleanupConfig");
            this.cleanupConfig = config;
            return this;
        }

        /**
         * Passes the {@link TransactionsCleanupConfig.Builder} to the provided consumer.
         * <p>
         * Allows customizing transaction cleanup options.
         *
         * @param builderConsumer a callback that configures options.
         * @return this builder for chaining purposes.
         */
        @Stability.Uncommitted
        public Builder cleanupConfig(Consumer<TransactionsCleanupConfig.Builder> builderConsumer) {
            builderConsumer.accept(this.cleanupConfig);
            return this;
        }

        /**
         * Sets the maximum time that transactions can run for.  The default is 15 seconds.
         * After
         * this time, transactions will throw a {@link com.couchbase.client.java.transactions.error.TransactionExpiredException} error.
         * <p>
         * Applications can increase or decrease this as desired.  The trade-off to understand is that documents that are
         * being mutated in a transaction A, are effectively locked from being updated by other transactions until
         * transaction A has completed - committed or rolled back.  If transaction A is unable to complete for whatever
         * reason, the document can be locked for this <code>expirationTime</code> time.
         * <p>
         * It is worth noting that this setting does not completely guarantee that the transaction will immediately be
         * completed after that time.  In some rare cases, such as application crashes, it may take longer as the lost transactions
         * cleanup process will be involved.
         */
        public Builder timeout(Duration timeout) {
            notNull(timeout, "timeout");
            this.timeout = Optional.of(timeout);
            return this;
        }

        /**
         * All transaction writes will be performed with this durability setting.
         * <p>
         * All writes in Couchbase go initially to one primary node, and from their fan-out to any configured replicas.
         * <p>
         * If durability is disabled then the transaction will continue as soon as the write is available on the primary
         * node.  The durability setting will ensure the transaction does not continue until the write is available in
         * more places.  This can provide a small degree of extra security in the advent of node loss.
         * <p>
         * The default setting is DurabilityLevel.MAJORITY, meaning a transaction will pause on each write
         * until it is available in-memory on a majority of configured replicas.
         */
        public Builder durabilityLevel(DurabilityLevel level) {
            notNull(level, "durabilityLevel");
            this.level = level;
            return this;
        }

        /**
         * For internal testing.  Applications should not require this.
         */
        @Stability.Internal
        Builder testFactories(@Nullable TransactionAttemptContextFactory attemptContextFactory,
                                                @Nullable CleanerFactory cleanerFactory,
                                                @Nullable ClientRecordFactory clientRecordFactory) {
            this.attemptContextFactory = Optional.ofNullable(attemptContextFactory);
            this.cleanerFactory = Optional.ofNullable(cleanerFactory);
            this.clientRecordFactory = Optional.ofNullable(clientRecordFactory);
            return this;
        }

        /**
         * Allows setting a custom collection to use for any transactional metadata documents.
         * <p>
         * If not set, it will default to creating these documents in the default collection of the bucket that the first
         * mutated document in the transaction is on.
         * <p>
         * This collection will be added to the set of collections being cleaned up.
         */
        public Builder metadataCollection(TransactionKeyspace collection) {
            notNull(collection, "metadataCollection");
            CollectionIdentifier coll = new CollectionIdentifier(collection.bucket(), Optional.of(collection.scope()), Optional.of(collection.collection()));
            this.metadataCollection = Optional.ofNullable(coll);
            return this;
        }

        /**
         * Sets the default query configuration for all transactions.
         *
         * @param queryConfig the query configuration to use
         * @return this, for chaining
         * @deprecated This method clobbers any previously configured values. Please use {@link #queryConfig(Consumer)} instead.
         */
        @Deprecated
        public Builder queryConfig(TransactionsQueryConfig.Builder queryConfig) {
            notNull(queryConfig, "queryConfig");
            this.queryConfig = queryConfig;
            return this;
        }

        /**
         * Passes the {@link TransactionsQueryConfig.Builder} to the provided consumer.
         * <p>
         * Allows customizing the default options for all transactional queries.
         *
         * @param builderConsumer a callback that configures options.
         */
        public Builder queryConfig(Consumer<TransactionsQueryConfig.Builder> builderConsumer) {
            builderConsumer.accept(this.queryConfig);
            return this;
        }
    }
}

