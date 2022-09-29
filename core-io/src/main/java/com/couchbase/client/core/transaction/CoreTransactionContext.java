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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.components.CoreTransactionRequest;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import reactor.util.annotation.Nullable;

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
    private final CoreTransactionRequest req;

    public CoreTransactionContext(CoreContext coreContext,
                                  String transactionId,
                                  CoreMergedTransactionConfig config,
                                  CoreTransactionsCleanup cleanup) {
        this.config = Objects.requireNonNull(config);
        this.cleanup = Objects.requireNonNull(cleanup);
        RequestTracer tracer = coreContext.environment().requestTracer();
        SpanWrapper pspan = config.parentSpan().map(sp -> new SpanWrapper(sp)).orElse(null);
        this.transactionSpan = SpanWrapper.create(tracer, TracingIdentifiers.TRANSACTION_OP, pspan);
        SpanWrapperUtil.setAttributes(this.transactionSpan, null, null, null)
                .attribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.TRANSACTION_OP)
                .attribute(TracingIdentifiers.ATTR_TRANSACTION_ID, transactionId);

        req = new CoreTransactionRequest(config.expirationTime(), coreContext, transactionSpan.span());

        this.transactionId = Objects.requireNonNull(transactionId);
        this.startTimeClient = System.nanoTime();
        LOGGER = new CoreTransactionLogger(Objects.requireNonNull(coreContext.environment().eventBus()), transactionId);
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

    public void incrementRetryAttempts(final Duration lastRetryDuration, final RetryReason reason) {
        req.context().incrementRetryAttempts(lastRetryDuration, reason);
    }

    public void finish(@Nullable Throwable err) {
        req.context().logicallyComplete(err);
    }
}
