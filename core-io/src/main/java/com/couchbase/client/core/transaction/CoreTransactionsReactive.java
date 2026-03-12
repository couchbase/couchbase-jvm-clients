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
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.api.query.CoreReactiveQueryResult;
import com.couchbase.client.core.classic.query.ClassicCoreReactiveQueryResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionCommitAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionOptions;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.threadlocal.TransactionMarker;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.QueryUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.couchbase.client.core.transaction.CoreTransactions.configDebug;

@Stability.Internal
public class CoreTransactionsReactive {
    private final Core core;
    private final CoreTransactionsConfig config;
    private final CoreTransactions blocking;

    public CoreTransactionsReactive(Core core, CoreTransactionsConfig config) {
        this.core = core;
        this.config = config;
        this.blocking = new CoreTransactions(core, config);
    }

    public Mono<CoreTransactionResult> run(Function<CoreTransactionAttemptContext, Mono<?>> transactionLogic,
                                           @Nullable CoreTransactionOptions perConfig) {
        return Mono.fromCallable(() -> blocking.run(ctx -> {
                    transactionLogic.apply(ctx)
                            // Remember that contextWrite is subscribe based so it will only be 'seen' by operators above
                            // this point in the code - e.g. the lambda.  We don't have to unset the context after this point
                            // as it effectively doesn't exist.
                            .contextWrite(reactiveContext -> {
                                TransactionMarker marker = new TransactionMarker(ctx);
                                return reactiveContext.put(TransactionMarker.class, marker);
                            })
                            .block();
                    return null;
                }, perConfig))
                // This will put the transaction onto a thread on transactionsSchedulers().schedulerBlocking(),
                // blocking that thread until the transaction is complete.
                // Each operation inside the transaction will use a thread also (handled elsewhere).
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    public CoreTransactionsConfig config() {
        return config;
    }

    public Core core() {
        return core;
    }

    /**
     * Performs a single query transaction, with a scope context and custom configuration.
     * Results are streaming, hence `errorConverter` is required to handle any errors during streaming.
     */
    public Mono<CoreReactiveQueryResult> query(String statement,
                                               @Nullable CoreQueryContext queryContext,
                                               CoreQueryOptions queryOptions,
                                               Optional<RequestSpan> parentSpan,
                                               Function<Throwable, RuntimeException> errorConverter) {
        Mono<CoreReactiveQueryResult> out = Mono.fromSupplier(() -> {
            CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(config, Optional.empty());
            CoreTransactionContext overall =
                    new CoreTransactionContext(core.context(),
                            UUID.randomUUID().toString(),
                            merged,
                            core.transactionsCleanup());
            overall.LOGGER.info(configDebug(config, null, core));

            Supplier<CoreTransactionAttemptContext> createAttempt = () -> {
                String attemptId = UUID.randomUUID().toString();
                return blocking.createAttemptContext(overall, merged, attemptId);
            };

            AtomicReference<ClassicCoreReactiveQueryResult> qr = new AtomicReference<>();

            Function<CoreTransactionAttemptContext, Void> runLogic = (ctx) -> {
                ctx.doQueryOperation("single query streaming", statement, queryOptions, parentSpan.map(SpanWrapper::new).orElse(null),
                        (sidx, span) -> {
                            core.coreResources().tracingDecorator().provideAttr(TracingAttribute.TRANSACTION_SINGLE_QUERY, span.span(), true);
                            ClassicCoreReactiveQueryResult ret = ctx.queryWrapperLocked(0,
                                    queryContext,
                                    statement,
                                    queryOptions,
                                    CoreTransactionAttemptContextHooks.HOOK_QUERY,
                                    false,
                                    true,
                                    null,
                                    null,
                                    span,
                                    true,
                                    true);
                            qr.set(ret);
                            return ret;
                        });
                return null;
            };

            Function<Throwable, RuntimeException> errorHandler = singleQueryHandleErrorDuringRowStreaming(overall, errorConverter);

            blocking.executeTransaction(createAttempt, overall, runLogic, true);
            ClassicCoreReactiveQueryResult orig = qr.get();

            if (orig == null) {
                // It's a bug to reach here.  If the query errored then that should have been raised.
                throw new CoreTransactionFailedException(new IllegalStateException("No query has been run"), overall.LOGGER, overall.transactionId());
            }

            // Need to return the original result, but customised to call our errorHandler during the row streaming.
            return new CoreReactiveQueryResult() {
                @Override
                public Flux<QueryChunkRow> rows() {
                    return orig.rows()
                            .onErrorResume(err -> {
                                return Mono.error(errorHandler.apply(err));
                            });
                }

                @Override
                public Mono<CoreQueryMetaData> metaData() {
                    return orig.metaData();
                }

                @Override
                public NodeIdentifier lastDispatchedTo() {
                    return orig.lastDispatchedTo();
                }
            };
        });
        return out.publishOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    private static Function<Throwable, RuntimeException> singleQueryHandleErrorDuringRowStreaming(CoreTransactionContext overall,
                                                                                                  Function<Throwable, RuntimeException> errorConverter) {
        return (err) -> {
            RuntimeException converted = QueryUtil.convertQueryError(err);

            overall.LOGGER.warn("", "got error on rows stream {}, converted from {}",
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

            return errorConverter.apply(ret);
        };
    }

    // Used only by single query transactions
    public Mono<CoreQueryResult> queryBlocking(String statement,
                                               @Nullable CoreQueryContext qc,
                                               CoreQueryOptions queryOptions,
                                               Optional<RequestSpan> parentSpan) {
        return Mono.fromCallable(() -> blocking.queryBlocking(statement, qc, queryOptions, parentSpan))
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }
}
