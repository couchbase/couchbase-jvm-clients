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
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.java.Collection;

import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Allows configuring a single-query-transaction.
 */
@Stability.Uncommitted
public class SingleQueryTransactionOptions {
    private Optional<DurabilityLevel> durabilityLevel = Optional.empty();
    private Optional<TransactionAttemptContextFactory> attemptContextFactory = Optional.empty();
    private Optional<CollectionIdentifier> metadataCollection = Optional.empty();

    public static SingleQueryTransactionOptions singleQueryTransactionOptions() {
        return new SingleQueryTransactionOptions();
    }

    @Stability.Internal
    public CoreSingleQueryTransactionOptions build() {
        return new CoreSingleQueryTransactionOptions(durabilityLevel, attemptContextFactory, metadataCollection);
    }

    /**
     * Overrides the default durability level set, for this transaction.
     *
     * @param durabilityLevel the durability level to set
     * @return this, for chaining
     */
    public SingleQueryTransactionOptions durabilityLevel(DurabilityLevel durabilityLevel) {
        notNull(durabilityLevel, "durabilityLevel");
        this.durabilityLevel = Optional.of(durabilityLevel);
        return this;
    }

    /**
     * Allows setting a custom collection to use for any transactional metadata documents created by this transaction.
     * <p>
     * If not set, it will default to creating these documents in the default collection of the bucket that the first
     * mutated document in the transaction is on.
     *
     * Note this is currently package private and Stability.Internal, as it's now decided to be a forthcoming feature.
     */
    @Stability.Internal
    SingleQueryTransactionOptions metadataCollection(Collection collection) {
        notNull(collection, "collection");
        CollectionIdentifier coll = new CollectionIdentifier(collection.bucketName(), Optional.of(collection.scopeName()), Optional.of(collection.name()));
        this.metadataCollection = Optional.of(coll);
        return this;
    }

    /**
     * For internal testing.  Applications should not require this.
     */
    @Stability.Internal
    SingleQueryTransactionOptions testFactory(TransactionAttemptContextFactory attemptContextFactory) {
        this.attemptContextFactory = Optional.of(attemptContextFactory);
        return this;
    }
}
