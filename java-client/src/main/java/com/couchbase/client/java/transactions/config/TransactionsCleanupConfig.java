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
import com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig;
import com.couchbase.client.java.transactions.TransactionKeyspace;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig.TRANSACTIONS_CLEANUP_LOST_PROPERTY;
import static com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig.TRANSACTIONS_CLEANUP_REGULAR_PROPERTY;
import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Provides all configurable parameters for Couchbase transactions cleanup.
 */
public class TransactionsCleanupConfig {
    private static final Duration CLEANUP_WINDOW_SECS = Duration.of(60, ChronoUnit.SECONDS);


    public static Builder builder() {
        return new TransactionsCleanupConfig.Builder();
    }

    /**
     * Controls where a background thread is created to cleanup any transaction attempts made by this client.
     * <p>
     * The default is true and users should generally not change this: cleanup is an essential part of Couchbase
     * transactions.
     */
    public static Builder cleanupClientAttempts(boolean cleanupClientAttempts) {
        return builder().cleanupClientAttempts(cleanupClientAttempts);
    }

    /**
     * Controls where a background process is created to cleanup any 'lost' transaction attempts.
     * <p>
     * The default is true and users should generally not change this: cleanup is an essential part of Couchbase
     * transactions.
     */
    public static Builder cleanupLostAttempts(boolean cleanupLostAttempts) {
        return builder().cleanupLostAttempts(cleanupLostAttempts);
    }

    /**
     * Part of the lost attempts background cleanup process.  Specifies the window during which the cleanup
     * process is sure to discover all lost transactions.
     * <p>
     * The default setting of 60 seconds is tuned to balance how quickly such transactions are discovered, while
     * minimising impact on the cluster.  If the application would prefer to discover
     * lost transactions more swiftly, but at the cost of increased impact, it can feel free to reduce this
     * parameter.
     */
    public static Builder cleanupWindow(Duration cleanupWindow) {
        return builder().cleanupWindow(cleanupWindow);
    }

    /**
     * Adds a collection to the set of metadata collections that will be cleaned up automatically.
     * <p>
     * This will also start cleanup immediately rather than on the first transaction (unless cleanup has been
     * explicitly disabled.)
     */
    public static Builder addCollection(TransactionKeyspace collection) {
        return builder().addCollection(collection);
    }

    /**
     * Adds collections to the set of metadata collections that will be cleaned up automatically.
     * <p>
     * Collections will be added automatically to this 'cleanup set' as transactions are performed, so generally
     * an application will not need to change this.
     * <p>
     * Setting this parameter will also start cleanup immediately rather than on the first transaction.
     */
    public Builder addCollections(java.util.Collection<TransactionKeyspace> collections) {
        return builder().addCollections(collections);
    }

    public static class Builder {
        private Optional<Boolean> cleanupLostAttempts = Optional.empty();
        private Optional<Boolean> cleanupClientAttempts = Optional.empty();
        private Optional<Duration> cleanupWindow = Optional.empty();
        private Set<CollectionIdentifier> cleanupSet = new HashSet<>();

        @Stability.Internal
        public CoreTransactionsCleanupConfig build() {
            return new CoreTransactionsCleanupConfig(
                    cleanupLostAttempts.orElse(Boolean.parseBoolean(System.getProperty(TRANSACTIONS_CLEANUP_LOST_PROPERTY, "true"))),
                    cleanupClientAttempts.orElse(Boolean.parseBoolean(System.getProperty(TRANSACTIONS_CLEANUP_REGULAR_PROPERTY, "true"))),
                    cleanupWindow.orElse(CLEANUP_WINDOW_SECS),
                    cleanupSet
            );
        }

        /**
         * Controls where a background thread is created to cleanup any transaction attempts made by this client.
         * <p>
         * This should be left at its default of true.  Without this, this client's transactions will only be cleaned up
         * by the lost attempts cleanup process, which is by necessity slower.
         */
        public Builder cleanupClientAttempts(boolean cleanupClientAttempts) {
            this.cleanupClientAttempts = Optional.of(cleanupClientAttempts);
            return this;
        }

        /**
         * Controls where a background process is created to cleanup any 'lost' transaction attempts: that is, those for
         * which the regular cleanup process has failed.
         * <p>
         * This should be left at its default of true.  Without at least one client performing this cleanup, 'lost'
         * transactions will not be removed.
         */
        public Builder cleanupLostAttempts(boolean cleanupLostAttempts) {
            this.cleanupLostAttempts = Optional.of(cleanupLostAttempts);
            return this;
        }

        /**
         * Part of the lost attempts background cleanup process.  Specifies the window during which the
         * cleanup
         * process is sure to discover all lost transactions.
         * <p>
         * This process is an implementation detail, but currently consists of polling multiple documents.  The default
         * setting of 60 seconds is tuned to reduce impact on the cluster.  If the application would prefer to discover
         * lost transactions more swiftly, but at the cost of more frequent polling, it can feel free to reduce this
         * parameter, while monitoring resource usage.
         * <p>
         * The trade-off to appreciate is that if a document is in a transaction A, it is effectively locked from being
         * updated by another transaction until transaction A has been completed - that is, committed or rolled back.  In
         * rare cases such as application crashes, the transaction will remain incomplete - that is, it will be lost - until
         * the lost transactions process discovers it.
         */
        public Builder cleanupWindow(Duration cleanupWindow) {
            notNull(cleanupWindow, "cleanupWindow");
            if (cleanupWindow.isZero()) {
                throw new IllegalArgumentException("cleanupWindow must be > 0");
            }
            this.cleanupWindow = Optional.of(cleanupWindow);
            return this;
        }

        /**
         * Adds a collection to the set of metadata collections that will be cleaned up automatically.
         * <p>
         * This will also start cleanup immediately rather than on the first transaction (unless cleanup has been
         * explicitly disabled.)
         */
        public Builder addCollection(TransactionKeyspace collection) {
            notNull(collection, "collection");
            cleanupSet.add(new CollectionIdentifier(collection.bucket(),
                    Optional.ofNullable(collection.scope()),
                    Optional.ofNullable(collection.collection())));
            return this;
        }

        /**
         * Adds collections to the set of metadata collections that will be cleaned up automatically.
         * <p>
         * This will also start cleanup immediately rather than on the first transaction (unless cleanup has been
         * explicitly disabled.)
         */
        public Builder addCollections(java.util.Collection<TransactionKeyspace> collections) {
            notNull(collections, "collections");
            collections.forEach(this::addCollection);
            return this;
        }
    }
}

