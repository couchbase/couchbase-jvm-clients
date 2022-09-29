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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionCommitAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.reactor.DefaultRetry;
import com.couchbase.client.core.retry.reactor.Jitter;
import com.couchbase.client.core.retry.reactor.RetryContext;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionOptions;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.forwards.Supported;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.threadlocal.TransactionMarker;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.QueryUtil;
import com.couchbase.client.core.error.transaction.RetryTransactionException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@Stability.Internal
public class CoreTransactionsReactive {
    // This is a safety-guard against bugs.  The txn will be aborted when it expires.
    static final int MAX_ATTEMPTS = 100;
    private final Core core;
    private final CoreTransactionsConfig config;

    public CoreTransactionsReactive(Core core, CoreTransactionsConfig config) {
        this.core = Objects.requireNonNull(core);
        this.config = Objects.requireNonNull(config);
    }

    /**
     * The main transactions 'engine', responsible for attempting the transaction logic as many times as required,
     * until the transaction commits, is explicitly rolled back, or expires.
     */
    public Mono<CoreTransactionResult>
    executeTransaction(Mono<CoreTransactionAttemptContext> createAttempt,
                                                               CoreMergedTransactionConfig config,
                                                               CoreTransactionContext overall,
                                                               Function<CoreTransactionAttemptContext, Mono<Void>> transactionLogic,
                                                               boolean singleQueryTransactionMode) {
        AtomicReference<Long> startTime = new AtomicReference<>();

        return createAttempt

                // TXNJ-50: Make sure we run user's blocking logic on a scheduler that can take it
                .publishOn(core.context().environment().transactionsSchedulers().schedulerBlocking())

                .doOnSubscribe(v -> {
                    if (startTime.get() == null) startTime.set(System.nanoTime());
                })

                // Where the magic happens: execute the app's transaction logic
                // A AttemptContextReactive gets created in here.  Rollback requires one of these (so it knows what
                // to rollback), so only errors thrown inside this block can trigger rollback.
                // So, expiry checks only get done inside this block.
                .doOnNext(ctx -> {
                    overall.incAttempts();
                    ctx.LOGGER.info(ctx.attemptId(), "starting attempt %d/%s/%s", overall.numAttempts(), ctx.transactionId(), ctx.attemptId());
                })

                .flatMap(ctx -> transactionLogic.apply(ctx)

                        // Remember that contextWrite is subscribe based so it will only be 'seen' by operators above
                        // this point in the code - e.g. the lambda.  We don't have to unset the context after this point
                        // as it effectively doesn't exist.
                        .contextWrite(reactiveContext -> {
                            TransactionMarker marker = new TransactionMarker(ctx);
                            return reactiveContext.put(TransactionMarker.class, marker);
                        })

                        .onErrorResume(err -> Mono.error(ctx.convertToOperationFailedIfNeeded(err, singleQueryTransactionMode)))

                        .then(ctx.implicitCommit(singleQueryTransactionMode))

                        // lambdaEnd either propagates `err` or throws RetryTransaction.
                        // This works around reactive's lack of a true `finally` equivalent.
                        .onErrorResume(err -> ctx.lambdaEnd(core().transactionsCleanup(), err, singleQueryTransactionMode))

                        .then(ctx.lambdaEnd(core().transactionsCleanup(), null, singleQueryTransactionMode))

                        .then(ctx.transactionEnd(null, singleQueryTransactionMode))

                        .onErrorResume(err -> {
                            if (err instanceof RetryTransactionException) {
                                return Mono.error(err);
                            }
                            else if (err instanceof CoreTransactionFailedException) {
                                // Must have come from transactionEnd, so just propagate.
                                return Mono.error(err);
                            }

                            return ctx.transactionEnd(err, singleQueryTransactionMode);
                        }))

                // Retry transaction if required - controlled by a RetryTransaction exception.
                .retryWhen(executeCreateRetryWhen(overall, startTime))

                .doOnNext(v -> overall.finish(null))
                .doOnError(err -> overall.finish(err))

                // If we get here, success
                .doOnTerminate(() -> {
                    long elapsed = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTime.get());
                    overall.LOGGER.info("finished txn in %dus", elapsed);
                });
    }

    private reactor.util.retry.Retry executeCreateRetryWhen(CoreTransactionContext overall, AtomicReference<Long> startTime) {
        Predicate<? super RetryContext<Object>> predicate = context -> {
            Throwable exception = context.exception();
            return exception instanceof RetryTransactionException;
        };

        return DefaultRetry.create(predicate)

                .exponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(100))

                .doOnRetry(v -> {
                    Duration ofLastAttempt = Duration.ofNanos(System.nanoTime() - startTime.get());
                    overall.LOGGER.info("<>", "retrying transaction after backoff %dmillis", v.backoff().toMillis());
                    overall.incrementRetryAttempts(ofLastAttempt, RetryReason.UNKNOWN);
                })

                // Add some jitter so two txns don't livelock each other
                .jitter(Jitter.random())

                .retryMax(MAX_ATTEMPTS)

                .toReactorRetry();
    }

    public CoreTransactionAttemptContext createAttemptContext(CoreTransactionContext overall,
                                                              CoreMergedTransactionConfig config,
                                                              String attemptId) {
        return config.attemptContextFactory()
                .create(core, overall, config, attemptId, this, Optional.of(overall.span()));
    }

    /**
     * Runs the supplied transactional logic until success or failure.
     * <ul>
     * <li>The transaction logic is supplied with a {@link CoreTransactionAttemptContext}, which contains asynchronous
     * methods to allow it to read, mutate, insert and delete documents, as well as commit or rollback the
     * transactions.</li>
     * <li>The transaction logic should run these methods as a Reactor chain.</li>
     * <li>The transaction logic should return a <code>Mono{@literal <}Void{@literal >}</code>.  Any
     * <code>Flux</code> or <code>Mono</code> can be converted to a <code>Mono{@literal <}Void{@literal >}</code> by
     * calling <code>.then()</code> on it.</li>
     * <li>This method returns a <code>Mono{@literal <}TransactionResult{@literal >}</code>, which should be handled
     * as a normal Reactor Mono.</li>
     * </ul>
     *
     * @param transactionLogic the application's transaction logic
     * @param perConfig        the configuration to use for this transaction
     * @return there is no need to check the returned {@link CoreTransactionResult}, as success is implied by the lack of a
     * thrown exception.  It contains information useful only for debugging and logging.
     * @throws CoreTransactionFailedException or a derived exception if the transaction fails to commit for any reason, possibly
     *                           after multiple retries.  The exception contains further details of the error.  Not
     */
    public Mono<CoreTransactionResult> run(Function<CoreTransactionAttemptContext, Mono<?>> transactionLogic,
                                           @Nullable CoreTransactionOptions perConfig) {
        return Mono.defer(() -> {
            CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(config, Optional.ofNullable(perConfig));

            CoreTransactionContext overall =
                    new CoreTransactionContext(core.context(),
                            UUID.randomUUID().toString(),
                            merged,
                            core.transactionsCleanup());

            overall.LOGGER.info(configDebug(config, perConfig, core));

            Mono<CoreTransactionAttemptContext> createAttempt = Mono.fromCallable(() -> {
                String attemptId = UUID.randomUUID().toString();
                return createAttemptContext(overall, merged, attemptId);
            });

            Function<CoreTransactionAttemptContext, Mono<Void>> runLogic = (ctx) -> Mono.defer(() -> {
                return transactionLogic.apply(ctx);
            }).then();

            return executeTransaction(createAttempt, merged, overall, runLogic, false);
        });
    }

    // Printing the stacktrace is expensive in terms of log noise, but has been a life saver on many debugging
    // encounters.  Strike a balance by eliding the more useless elements.
    private void logElidedStacktrace(CoreTransactionAttemptContext ctx, Throwable err) {
        ctx.LOGGER.info(ctx.attemptId(), DebugUtil.createElidedStacktrace(err));
    }

    static private String configDebug(CoreTransactionsConfig config, @Nullable CoreTransactionOptions perConfig, Core core) {
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
        }
        else {
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
        sb.append(Supported.SUPPORTED);
        return sb.toString();
    }

    public CoreTransactionsConfig config() {
        return config;
    }

    public Core core() {
        return core;
    }

    /**
     * Performs a single query transaction, with a scope context and custom configuration.
     */
    @Stability.Uncommitted
    public Mono<QueryResponse> query(String statement,
                                     String bucketName,
                                     String scopeName,
                                     ObjectNode queryOptions,
                                     Optional<RequestSpan> parentSpan,
                                     Consumer<RuntimeException> errorConverter) {
        return Mono.defer(() -> {
            CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(config, Optional.empty());
            CoreTransactionContext overall =
                    new CoreTransactionContext(core.context(),
                            UUID.randomUUID().toString(),
                            merged,
                            core.transactionsCleanup());
            overall.LOGGER.info(configDebug(config, null, core));

            Mono<CoreTransactionAttemptContext> createAttempt = Mono.fromCallable(() -> {
                String attemptId = UUID.randomUUID().toString();
                return createAttemptContext(overall, merged, attemptId);
            });

            AtomicReference<QueryResponse> qr = new AtomicReference<>();

            Function<CoreTransactionAttemptContext, Mono<Void>> runLogic = (ctx) -> Mono.defer(() -> {
                return ctx.doQueryOperation("single query streaming", statement, parentSpan.map(SpanWrapper::new).orElse(null),
                                (sidx, lockToken, span) -> {
                                    span.attribute(TracingIdentifiers.ATTR_TRANSACTION_SINGLE_QUERY, true);
                                    return ctx.queryWrapperLocked(0,
                                                    bucketName,
                                                    scopeName,
                                                    statement,
                                                    queryOptions,
                                                    CoreTransactionAttemptContextHooks.HOOK_QUERY,
                                                    false,
                                                    true,
                                                    null,
                                                    null,
                                                    span,
                                                    true,
                                                    null,
                                                    true)
                                            .doOnNext(ret -> qr.set(ret));
                                })
                        .then();
            });

            Consumer<Throwable> errorHandler = singleQueryHandleErrorDuringRowStreaming(overall, errorConverter);

            return executeTransaction(createAttempt, merged, overall, runLogic, true)
                    .then(Mono.defer(() -> {
                        QueryResponse orig = qr.get();

                        if (orig == null) {
                            // It's a bug to reach here.  If the query errored then that should have been raised.
                            return Mono.error(new CoreTransactionFailedException(new IllegalStateException("No query has been run"), overall.LOGGER, overall.transactionId()));
                        }

                        Flux<QueryChunkRow> rows = orig.rows()
                                .onErrorResume(err -> {
                                    // Will throw
                                    errorHandler.accept(err);
                                    return Mono.empty();
                                });

                        QueryResponse wrapped = new QueryResponse(orig.status(), orig.header(), rows, orig.trailer());
                        return Mono.just(wrapped);
                    }));
        });
    }

    private static Consumer<Throwable> singleQueryHandleErrorDuringRowStreaming(CoreTransactionContext overall,
                                                                                Consumer<RuntimeException> errorConverter) {
        return (err) -> {
            RuntimeException converted = QueryUtil.convertQueryError(err);

            overall.LOGGER.warn("", "got error on rows stream %s, converted from %s",
                    DebugUtil.dbg(converted), DebugUtil.dbg(err));

            RuntimeException ret = converted;

            if (converted instanceof TransactionOperationFailedException) {
                TransactionOperationFailedException tof = (TransactionOperationFailedException) converted;

                switch (tof.toRaise()) {
                    case TRANSACTION_FAILED_POST_COMMIT:
                        ret = new CoreTransactionFailedException(tof, overall.LOGGER, overall.transactionId());
                        break;

                    case TRANSACTION_EXPIRED: {
                        String msg = "Transaction has expired configured timeout of " + overall.expirationTime().toMillis() + "ms.  The transaction is not committed.";
                        ret = new CoreTransactionExpiredException(err, overall.LOGGER, overall.transactionId(), msg);
                        break;
                    }
                    case TRANSACTION_COMMIT_AMBIGUOUS: {
                        String msg = "It is ambiguous whether the transaction committed";
                        ret = new CoreTransactionCommitAmbiguousException(err, overall.LOGGER, overall.transactionId(), msg);
                        break;
                    }
                    default:
                        ret = new CoreTransactionFailedException(err, overall.LOGGER, overall.transactionId());
                        break;
                }
            }

            // This will throw
            errorConverter.accept(ret);
        };
    }

    // Used only by single query transactions
    public Mono<CoreTransactionAttemptContext.BufferedQueryResponse> queryBlocking(String statement,
                                                                                   String bucketName,
                                                                                   String scopeName,
                                                                                   ObjectNode queryOptions,
                                                                                   Optional<RequestSpan> parentSpan) {
        return Mono.defer(() -> {
            queryOptions.put("tximplicit", true);

            CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(config, parentSpan.map(CoreTransactionOptions::create));
            CoreTransactionContext overall =
                    new CoreTransactionContext(core.context(),
                            UUID.randomUUID().toString(),
                            merged,
                            core.transactionsCleanup());
            overall.LOGGER.info(configDebug(config, null, core));

            Mono<CoreTransactionAttemptContext> createAttempt = Mono.fromCallable(() -> {
                String attemptId = UUID.randomUUID().toString();
                return createAttemptContext(overall, merged, attemptId);
            });

            AtomicReference<CoreTransactionAttemptContext.BufferedQueryResponse> qr = new AtomicReference<>();

            Function<CoreTransactionAttemptContext, Mono<Void>> runLogic = (ctx) -> Mono.defer(() -> {
                return ctx.queryBlocking(statement, bucketName, scopeName, queryOptions, true)
                        // All rows have already been streamed and buffered, so it's ok to save this
                        .doOnNext(ret -> qr.set(ret))
                        .then();
            });

            return executeTransaction(createAttempt, merged, overall, runLogic, true)
                    .then(Mono.defer(() -> {
                        if (qr.get() != null) {
                            return Mono.just(qr.get());
                        }

                        // It's a bug to reach here.  If the query errored then that should have been raised.
                        return Mono.error(new CoreTransactionFailedException(new IllegalStateException("No query has been run"), overall.LOGGER, overall.transactionId()));
                    }));
        });
    }
}
