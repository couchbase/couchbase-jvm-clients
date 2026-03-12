/*
 * Copyright 2026 Couchbase, Inc.
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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryOptionsTransactions;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionOptions;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.util.BlockingRetryHandler;
import com.couchbase.client.core.transaction.util.DebugUtil;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

@Stability.Internal
public class CoreTransactions {
    // This is a safety-guard against bugs.  The txn will be aborted before this, when it expires.
    static final int MAX_ATTEMPTS = 100;
    private final Core core;
    private final CoreTransactionsConfig config;

    public CoreTransactions(Core core, CoreTransactionsConfig config) {
        this.core = core;
        this.config = config;
    }

    public Core core() {
        return core;
    }

    public CoreTransactionsConfig config() {
        return config;
    }

    public CoreTransactionResult executeTransaction(Supplier<CoreTransactionAttemptContext> createAttempt,
                                                    CoreTransactionContext overall,
                                                    Function<CoreTransactionAttemptContext, Void> transactionLogic,
                                                    boolean singleQueryTransactionMode) {
        long startTime = System.nanoTime();
        BlockingRetryHandler retry = BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ofMillis(100))
                .jitter(0.5)
                .maxRetries(MAX_ATTEMPTS)
                .build();

        do {
            retry.shouldRetry(false);
            overall.incAttempts();
            CoreTransactionAttemptContext ctx = createAttempt.get();
            ctx.LOGGER.info(ctx.attemptId(), "starting attempt {}/{}/{}", overall.numAttempts(), ctx.transactionId(), ctx.attemptId());

            Throwable failure = null;

            try {
                transactionLogic.apply(ctx);
            } catch (Exception err) {
                failure = ctx.convertToOperationFailedIfNeeded(err, singleQueryTransactionMode);
            }

            if (failure == null) {
                try {
                    ctx.implicitCommit(singleQueryTransactionMode);
                } catch (Exception e) {
                    failure = e;
                }
            }

            // This will rollback if needed.
            CoreTransactionAttemptContext.LambdaEndNext next = ctx.lambdaEnd(core().transactionsCleanup(), failure, singleQueryTransactionMode);

            if (next instanceof CoreTransactionAttemptContext.LambdaEndRetry) {
                retry.sleepForNextBackoffAndSetShouldRetry();
            } else if (next instanceof CoreTransactionAttemptContext.LambdaEndSuccess) {
                overall.finish(null);
                CoreTransactionResult result = ctx.transactionEnd(null, singleQueryTransactionMode);

                long elapsed = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime);
                overall.LOGGER.info("finished txn successfully in {}us", elapsed);

                return result;
            } else {
                Throwable err = ((CoreTransactionAttemptContext.LambdaEndFailed) next).error;
                overall.finish(err);

                CoreTransactionResult result = ctx.transactionEnd(err, singleQueryTransactionMode);

                long elapsed = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime);
                overall.LOGGER.info("finished txn in {}us with failure", elapsed, DebugUtil.dbg(err));

                return result;
            }
        } while (retry.shouldRetry());

        // Should not be possible to reach.
        CoreTransactionFailedException err = new CoreTransactionFailedException(new IllegalStateException("Retry attempts exceeded"), overall.LOGGER, overall.transactionId());
        overall.finish(err);
        throw err;
    }

    public CoreTransactionAttemptContext createAttemptContext(CoreTransactionContext overall,
                                                              CoreMergedTransactionConfig config,
                                                              String attemptId) {
        return config.attemptContextFactory()
                .create(core(), overall, config, attemptId, Optional.of(overall.span()));
    }

    public CoreTransactionResult run(Function<CoreTransactionAttemptContext, ?> transactionLogic,
                                     @Nullable CoreTransactionOptions perConfig) {
        CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(config(), Optional.ofNullable(perConfig));

        CoreTransactionContext overall =
                new CoreTransactionContext(core().context(),
                        UUID.randomUUID().toString(),
                        merged,
                        core().transactionsCleanup());

        overall.LOGGER.info(configDebug(config(), perConfig, core()));

        Supplier<CoreTransactionAttemptContext> createAttempt = () -> {
            String attemptId = UUID.randomUUID().toString();
            return createAttemptContext(overall, merged, attemptId);
        };

        Function<CoreTransactionAttemptContext, Void> runLogic = (ctx) -> {
            transactionLogic.apply(ctx);
            return null;
        };

        return executeTransaction(createAttempt, overall, runLogic, false);
    }

    static public String configDebug(CoreTransactionsConfig config, @Nullable CoreTransactionOptions perConfig, Core core) {
        StringBuilder sb = new StringBuilder();
        sb.append("SDK version: ");
        sb.append(core.context().environment().clientVersion().orElse("-"));
        sb.append(" config: ");
        sb.append("atrs=");
        sb.append(config.numAtrs());
        sb.append(", metadataCollection=");
        sb.append(config.metadataCollection());
        sb.append(", expiry=");
        if (perConfig != null) {
            sb.append(perConfig.timeout().orElse(config.transactionExpirationTime()).toMillis());
        } else {
            sb.append(config.transactionExpirationTime().toMillis());
        }
        sb.append("ms durability=");
        sb.append(config.durabilityLevel());
        if (perConfig != null) {
            sb.append(" per-txn config=");
            sb.append(" durability=");
            sb.append(perConfig.durabilityLevel());
        }
        sb.append(", supported=");
        sb.append(config.supported());
        return sb.toString();
    }

    // Used only for single query transactions.
    public CoreQueryResult queryBlocking(String statement,
                                         @Nullable CoreQueryContext qc,
                                         CoreQueryOptions queryOptions,
                                         Optional<RequestSpan> parentSpan) {
        CoreQueryOptionsTransactions optionsCopy = new CoreQueryOptionsTransactions(queryOptions);
        optionsCopy.put("tximplicit", TextNode.valueOf("true"));

        CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(config(), parentSpan.map(CoreTransactionOptions::create));
        CoreTransactionContext overall =
                new CoreTransactionContext(core().context(),
                        UUID.randomUUID().toString(),
                        merged,
                        core().transactionsCleanup());
        overall.LOGGER.info(configDebug(config(), null, core()));

        Supplier<CoreTransactionAttemptContext> createAttempt = () -> {
            String attemptId = UUID.randomUUID().toString();
            return createAttemptContext(overall, merged, attemptId);
        };

        AtomicReference<CoreQueryResult> qr = new AtomicReference<>();

        Function<CoreTransactionAttemptContext, Void> runLogic = (ctx) -> {
            CoreQueryResult ret = ctx.queryBlocking(statement, qc, optionsCopy, true);
            qr.set(ret);
            return null;
        };

        executeTransaction(createAttempt, overall, runLogic, true);

        if (qr.get() != null) {
            return qr.get();
        }

        throw new CoreTransactionFailedException(new IllegalStateException("No query has been run"), overall.LOGGER, overall.transactionId());
    }
}
