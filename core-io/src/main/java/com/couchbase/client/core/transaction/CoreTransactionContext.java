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

package com.couchbase.client.core.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Stores some context related to a transaction.
 * <p>
 * This is an immutable value class.
 */
@Stability.Internal
public class CoreTransactionContext {
    // The System.nanoTime() this overall transaction started
    private final long startTimeClient;
    public final CoreTransactionLogger LOGGER;
    private final String transactionId;
    private final SpanWrapper transactionSpan;
    private final CoreMergedTransactionConfig config;
    private final CoreTransactionsCleanup cleanup;
    private int numAttempts = 0;

    public CoreTransactionContext(RequestTracer tracer,
                                  EventBus eventBus,
                                  String transactionId,
                                  CoreMergedTransactionConfig config,
                                  CoreTransactionsCleanup cleanup) {
        this.config = Objects.requireNonNull(config);
        this.cleanup = Objects.requireNonNull(cleanup);
        SpanWrapper pspan = config.parentSpan().map(sp -> new SpanWrapper(tracer, sp)).orElse(null);
        this.transactionSpan = SpanWrapperUtil.basic(SpanWrapper.create(tracer, "transaction", pspan), "transaction")
                .attribute("db.couchbase.transactions.transaction_id", transactionId);

        this.transactionId = Objects.requireNonNull(transactionId);
        this.startTimeClient = System.nanoTime();
        LOGGER = new CoreTransactionLogger(Objects.requireNonNull(eventBus), transactionId);
    }

    public Duration expirationTime() {
        return config.expirationTime();
    }

    public long timeSinceStartOfTransactionsMillis(long now) {
        long expiredNanos = (now - startTimeClient);
        return TimeUnit.NANOSECONDS.toMillis(expiredNanos);
    }

    public boolean hasExpiredClientSide() {
        long now = System.nanoTime();
        long expiredMillis = timeSinceStartOfTransactionsMillis(now);
        boolean isExpired = expiredMillis > expirationTime().toMillis();
        if (isExpired) {
            LOGGER.info("Has expired client side (now=%dns start=%dns expired=%dmillis config=%dms)",
                    now, startTimeClient, expiredMillis, expirationTime().toMillis());
        }
        return isExpired;
    }

    public String transactionId() {
        return transactionId;
    }

    public long startTimeClient() {
        return startTimeClient;
    }

    public SpanWrapper span() {
        return transactionSpan;
    }

    public int numAttempts() {
        return numAttempts;
    }

    public void incAttempts() {
        numAttempts += 1;
    }

    public CoreTransactionsCleanup cleanup() {
        return cleanup;
    }
}
