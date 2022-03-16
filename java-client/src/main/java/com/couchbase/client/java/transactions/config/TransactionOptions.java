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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.config.CoreTransactionOptions;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.java.Collection;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Provides all configurable parameters for a single Couchbase transaction.
 */
public class TransactionOptions {
    private Optional<DurabilityLevel> durabilityLevel = Optional.empty();
    private Optional<RequestSpan> parentSpan = Optional.empty();
    private Optional<Duration> timeout = Optional.empty();
    private Optional<TransactionAttemptContextFactory> attemptContextFactory = Optional.empty();
    private Optional<CollectionIdentifier> metadataCollection = Optional.empty();

    /**
     * Returns a new <code>TransactionOptions.Builder</code>, which can be used to build up and create a
     * {@link CoreTransactionOptions}.
     */
    public static TransactionOptions transactionOptions() {
        return new TransactionOptions();
    }

    @Stability.Internal
    public CoreTransactionOptions build() {
        return new CoreTransactionOptions(
                durabilityLevel,
                Optional.empty(),
                parentSpan,
                timeout,
                metadataCollection,
                attemptContextFactory);
    }

    private TransactionOptions() {
    }

    /**
     * Overrides the default durability set, for this transaction.  The level will be used for all operations inside the transaction.
     *
     * @param durabilityLevel the durability level to set
     * @return this, for chaining
     */
    public TransactionOptions durabilityLevel(DurabilityLevel durabilityLevel) {
        notNull(durabilityLevel, "durabilityLevel");
        this.durabilityLevel = Optional.of(durabilityLevel);
        return this;
    }

    /**
     * Specifies the RequestSpan that's a parent for this transaction.
     * <p>
     * RequestSpan is a Couchbase Java SDK abstraction over an underlying tracing implementation such as OpenTelemetry
     * or OpenTracing.
     * <p>
     * @return this, for chaining
     */
    public TransactionOptions parentSpan(RequestSpan parentSpan) {
        notNull(parentSpan, "parentSpan");
        this.parentSpan = Optional.of(parentSpan);
        return this;
    }

    /**
     * Overrides the default timeout set, for this transaction.
     *
     * @return this, for chaining
     */
    public TransactionOptions timeout(Duration timeout) {
        notNull(timeout, "timeout");
        this.timeout = Optional.of(timeout);
        return this;
    }

    /**
     * Allows setting a custom collection to use for any transactional metadata documents created by this transaction.
     * <p>
     * If not set, it will default to creating these documents in the default collection of the bucket that the first
     * mutated document in the transaction is on.
     */
    public TransactionOptions metadataCollection(Collection collection) {
        notNull(collection, "metadataCollection");
        CollectionIdentifier coll = new CollectionIdentifier(collection.bucketName(), Optional.of(collection.scopeName()), Optional.of(collection.name()));
        this.metadataCollection = Optional.of(coll);
        return this;
    }

    /**
     * For internal testing.  Applications should not require this.
     */
    @Stability.Internal
    TransactionOptions testFactory(TransactionAttemptContextFactory attemptContextFactory) {
        this.attemptContextFactory = Optional.of(attemptContextFactory);
        return this;
    }
}

