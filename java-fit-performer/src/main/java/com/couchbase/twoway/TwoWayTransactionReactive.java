/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// [skip:<3.3.0]
package com.couchbase.twoway;

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.transaction.internal.TestFailOtherException;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
// [if:3.3.2]
import com.couchbase.client.core.transaction.threadlocal.TransactionMarkerOwner;
// [end]
import com.couchbase.client.core.transaction.util.MonoBridge;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionQueryResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.protocol.shared.API;
import com.couchbase.client.protocol.transactions.CommandBatch;
import com.couchbase.client.protocol.transactions.CommandGet;
import com.couchbase.client.protocol.transactions.CommandGetOptional;
import com.couchbase.client.protocol.transactions.CommandInsert;
import com.couchbase.client.protocol.transactions.CommandInsertRegularKV;
import com.couchbase.client.protocol.transactions.CommandQuery;
import com.couchbase.client.protocol.transactions.CommandRemove;
import com.couchbase.client.protocol.transactions.CommandRemoveRegularKV;
import com.couchbase.client.protocol.transactions.CommandReplace;
import com.couchbase.client.protocol.transactions.CommandReplaceRegularKV;
import com.couchbase.client.protocol.transactions.CommandSetLatch;
import com.couchbase.client.protocol.transactions.CommandWaitOnLatch;
import com.couchbase.client.protocol.transactions.ExpectedResult;
import com.couchbase.client.protocol.transactions.TransactionAttemptRequest;
import com.couchbase.client.protocol.transactions.TransactionCommand;
import com.couchbase.client.protocol.transactions.TransactionCreateRequest;
import com.couchbase.client.protocol.transactions.TransactionStreamPerformerToDriver;
import com.couchbase.utils.ClusterConnection;
import com.couchbase.utils.OptionsUtil;
import com.couchbase.utils.ResultValidation;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Version of TwoWayTransaction that uses the reactive API.
 */
public class TwoWayTransactionReactive extends TwoWayTransactionShared {
    public TwoWayTransactionReactive() {
        super(null);
    }

    /**
     * Starts a transaction that will run until completion.
     */
    public static com.couchbase.client.protocol.transactions.TransactionResult run(ClusterConnection connection,
                                                                                   TransactionCreateRequest req,
                                                                                   ConcurrentHashMap<String, RequestSpan> spans) {
        TwoWayTransactionReactive txn = new TwoWayTransactionReactive();
        return txn.run(connection, req, null, false, spans);
    }


    /**
     * Starts a two-way transaction.
     */
    protected com.couchbase.client.java.transactions.TransactionResult runInternal(ClusterConnection connection,
                                            TransactionCreateRequest req,
                                            @Nullable StreamObserver<TransactionStreamPerformerToDriver> toTest,
                                            boolean performanceMode,
                                            ConcurrentHashMap<String, RequestSpan> spans) {
        if (req.getApi() != API.ASYNC) {
            throw new InternalPerformerFailure(new IllegalStateException("Unexpected API"));
        }

        AtomicInteger attemptCount = new AtomicInteger(-1);
        TransactionOptions ptcb = OptionsUtil.makeTransactionOptions(connection, req, spans);

        return connection.cluster().reactive().transactions().run((ctx) -> {
            if (testFailure.get() != null) {
                logger.info("Test failure is set at start of new attempt, fast failing transaction");
                throw testFailure.get();
            }

            final int count = attemptCount.incrementAndGet();
            logger.info("Starting attempt {}", count);

            int attemptToUse = Math.min(count, req.getAttemptsCount() - 1);
            TransactionAttemptRequest attempt = req.getAttempts(attemptToUse);

            return Flux.fromIterable(attempt.getCommandsList())
                    .concatMap(command -> performOperation(connection, ctx, command, toTest, performanceMode, ""))
                    .then();
        }, ptcb)
                // [if:3.3.2]
                .flatMap(result -> {
                    return TransactionMarkerOwner.get()
                            .doOnNext(v -> {
                                if (v.isPresent()) {
                                    throw new InternalPerformerFailure(new IllegalStateException("Still in reactive transaction context after completion"));
                                }
                            })
                            .thenReturn(result);
                })
                // [end]
                // Use a unique scheduler so can find the thread in the debugger
                .subscribeOn(Schedulers.newBoundedElastic(4, Integer.MAX_VALUE, "java-performer"))
                .block();
    }

    private Mono<?> performOperation(ClusterConnection connection,
                                  ReactiveTransactionAttemptContext ctx,
                                  TransactionCommand op,
                                  @Nullable StreamObserver<TransactionStreamPerformerToDriver> toTest,
                                  boolean performanceMode,
                                  String dbg) {
        Mono<Void> waitIfNeeded = op.getWaitMsecs() == 0 ? Mono.empty()
                : Mono.delay(Duration.ofMillis(op.getWaitMsecs())).then();

        if (op.hasInsert()) {
            final CommandInsert request = op.getInsert();
            var content = readContent(request.hasContentJson() ? request.getContentJson() : null, request.hasContent() ? request.getContent() : null);
            var options = TransactionOptionsUtil.transactionInsertOptions(request);

            final Collection collection = connection.collection(request.getDocId());
            return performOperation(waitIfNeeded, dbg + "insert " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(),op.getDoNotPropagateError(), performanceMode,
                () -> {
                    logger.info("Performing insert operation on {} on bucket {} on collection {}",
                            request.getDocId().getDocId(), request.getDocId().getBucketName(), request.getDocId().getCollectionName());
                    // [if:3.6.2]
                    if (options != null) {
                        return ctx.insert(collection.reactive(), request.getDocId().getDocId(), content, options).then();
                    }
                    // [end]
                    return ctx.insert(collection.reactive(), request.getDocId().getDocId(), content).then();
                });
        } else if (op.hasReplace()) {
            final CommandReplace request = op.getReplace();
            var content = readContent(request.hasContentJson() ? request.getContentJson() : null, request.hasContent() ? request.getContent() : null);
            var options = TransactionOptionsUtil.transactionReplaceOptions(request);

            return performOperation(waitIfNeeded, dbg + "replace " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(), performanceMode,
                () -> {
                    if (request.getUseStashedResult()) {
                        throw new IllegalStateException("Concept of stashed result does not apply in batch/async mode");
                    } else if (request.hasUseStashedSlot()) {
                        if (!stashedGetMap.containsKey(request.getUseStashedSlot())) {
                            throw new IllegalStateException("Do not have a stashed get in slot " + request.getUseStashedSlot());
                        }
                        // [if:3.6.2]
                        if (options != null) {
                            return ctx.replace(stashedGetMap.get(request.getUseStashedSlot()), content, options);
                        }
                        // [end]
                        return ctx.replace(stashedGetMap.get(request.getUseStashedSlot()), content);
                    } else {
                        final Collection collection = connection.collection(request.getDocId());
                        logger.info("Performing replace operation on docId {} to new content {} on collection {}",
                                request.getDocId().getDocId(), request.getContentJson(),request.getDocId().getCollectionName());
                        return ctx.get(collection.reactive(), request.getDocId().getDocId())
                                .flatMap(r -> {
                                    // [if:3.6.2]
                                    if (options != null) {
                                        return ctx.replace(r, content, options);
                                    }
                                    // [end]
                                    return ctx.replace(r, content);
                                })
                                .then();
                    }
                });
        } else if (op.hasRemove()) {
            final CommandRemove request = op.getRemove();

            return performOperation(waitIfNeeded, dbg + "remove " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(), performanceMode,
                () -> {
                    if (request.getUseStashedResult()) {
                        throw new IllegalStateException("Concept of stashed result does not apply in batch/async mode");
                    } else if (request.hasUseStashedSlot()) {
                        if (!stashedGetMap.containsKey(request.getUseStashedSlot())) {
                            throw new IllegalStateException("Do not have a stashed get in slot " + request.getUseStashedSlot());
                        }
                        return ctx.remove(stashedGetMap.get(request.getUseStashedSlot()));
                    } else {
                        final Collection collection = connection.collection(request.getDocId());

                        logger.info("Performing remove operation on docId {} on collection {}",
                                request.getDocId().getDocId(), request.getDocId().getCollectionName());
                        return ctx.get(collection.reactive(), request.getDocId().getDocId())
                                        .flatMap(r -> ctx.remove(r));
                    }
                });
        } else if (op.hasCommit()) {
            // Ignoring - explicit commit removed in ExtSDKIntegration
            return Mono.empty();
        } else if (op.hasRollback()) {
            return Mono.error(new RuntimeException("Driver has requested app-rollback"));
        } else if (op.hasGet()) {
            final CommandGet request = op.getGet();
            final Collection collection = connection.collection(request.getDocId());
            var options = TransactionOptionsUtil.transactionGetOptions(request);

            return performOperation(waitIfNeeded, dbg + "get " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(), performanceMode,
                    () -> {
                        logger.info("Performing get operation on {} on bucket {} on collection {}", request.getDocId().getDocId(), request.getDocId().getBucketName(), request.getDocId().getCollectionName());
                        return Mono.defer(() -> {
                                    // [if:3.6.2]
                                    if (options != null) {
                                        return ctx.get(collection.reactive(), request.getDocId().getDocId(), options);
                                    }
                                    // [end]
                                    return ctx.get(collection.reactive(), request.getDocId().getDocId());
                                })
                                .doOnNext(out -> handleGetResult(request, out, connection, request.hasContentAsValidation() ? request.getContentAsValidation() : null))
                                .then();
                    });
        } else if (op.hasGetOptional()) {
            final CommandGetOptional req = op.getGetOptional();
            final CommandGet request = req.getGet();
            final Collection collection = connection.collection(request.getDocId());
            var options = TransactionOptionsUtil.transactionGetOptions(request);

            return performOperation(waitIfNeeded, dbg + "get optional " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(), performanceMode,
                    () -> {
                        logger.info("Performing getOptional operation on {} on bucket {} on collection {} ", request.getDocId().getDocId(),request.getDocId().getBucketName(),request.getDocId().getCollectionName());
                        return Mono.defer(() -> {
                                    // [if:3.6.2]
                                    if (options != null) {
                                        return ctx.get(collection.reactive(), request.getDocId().getDocId(), options);
                                    }
                                    // [end]
                                    return ctx.get(collection.reactive(), request.getDocId().getDocId());
                                })
                                .doOnNext(doc -> handleGetOptionalResult(request, req, Optional.of(doc), connection, request.hasContentAsValidation() ? request.getContentAsValidation() : null))
                                .onErrorResume(err -> {
                                    if (err instanceof DocumentNotFoundException) {
                                        handleGetOptionalResult(request, req, Optional.empty(), connection, request.hasContentAsValidation() ? request.getContentAsValidation() : null);
                                        return Mono.empty();
                                    }
                                    else {
                                        return Mono.error(err);
                                    }
                                })
                                .then();
                    });
        } else if (op.hasWaitOnLatch()) {
            final CommandWaitOnLatch request = op.getWaitOnLatch();
            final String latchName = request.getLatchName();
            return performOperation(waitIfNeeded, dbg + "wait on latch " + latchName, ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(), performanceMode,
                () -> Mono.fromRunnable(() -> handleWaitOnLatch(request, getLogger(ctx))));
        } else if (op.hasSetLatch()) {
            final CommandSetLatch request = op.getSetLatch();
            final String latchName = request.getLatchName();
            return performOperation(waitIfNeeded, dbg + "set latch " + latchName, ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(), performanceMode,
                    () -> Mono.fromRunnable(() -> handleSetLatch(request, toTest, getLogger(ctx))));
        } else if (op.hasParallelize()) {
            final CommandBatch request = op.getParallelize();

            return waitIfNeeded.then(performCommandBatch(request, (parallelOp) -> performOperation(connection, ctx, parallelOp.getT2(), toTest, performanceMode, parallelOp.getT1() + " ")));
        } else if (op.hasInsertRegularKv()) {
            final CommandInsertRegularKV request = op.getInsertRegularKv();
            final Collection collection = connection.collection(request.getDocId());

            return performOperation(waitIfNeeded, dbg + "KV insert " + request.getDocId().getDocId(), ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(), performanceMode,
                    () -> {
                        JsonObject content = JsonObject.fromJson(request.getContentJson());
                        return collection.reactive().insert(request.getDocId().getDocId(), content).then();
                    });
        } else if (op.hasReplaceRegularKv()) {
            final CommandReplaceRegularKV request = op.getReplaceRegularKv();
            final Collection collection = connection.collection(request.getDocId());

            return performOperation(waitIfNeeded, dbg + "KV replace " + request.getDocId().getDocId(), ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(), performanceMode,
                    () -> {
                        JsonObject content = JsonObject.fromJson(request.getContentJson());
                        return collection.reactive().replace(request.getDocId().getDocId(), content).then();
                    });
        } else if (op.hasRemoveRegularKv()) {
            final CommandRemoveRegularKV request = op.getRemoveRegularKv();
            final Collection collection = connection.collection(request.getDocId());

            return performOperation(waitIfNeeded, dbg + "KV remove " + request.getDocId().getDocId(), ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(), performanceMode,
                    () -> collection.reactive().remove(request.getDocId().getDocId()).then());
        } else if (op.hasThrowException()) {
            getLogger(ctx).info("Test throwing a TestFailOther exception here");
            logger.info("Throwing exception");
            return waitIfNeeded.then(Mono.error(new TestFailOtherException()));
        } else if (op.hasQuery()) {
            final CommandQuery request = op.getQuery();

            return performOperation(waitIfNeeded, dbg + "Query " + request.getStatement(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(), performanceMode,
                    () -> {
                        com.couchbase.client.java.transactions.TransactionQueryOptions queryOptions = OptionsUtil.transactionQueryOptions(request);

                        Mono<TransactionQueryResult> next;

                        if (request.hasScope()) {
                            var bucketName = request.getScope().getBucketName();
                            var scopeName = request.getScope().getScopeName();
                            var scope = connection.cluster().bucket(bucketName).scope(scopeName).reactive();

                            if (queryOptions != null) {
                                next = ctx.query(scope, request.getStatement(), queryOptions);
                            } else {
                                next = ctx.query(scope, request.getStatement());
                            }
                        } else {
                            if (queryOptions != null) {
                                next = ctx.query(request.getStatement(), queryOptions);
                            } else {
                                // Passing null should work fine, but let's test the full pathway
                                next = ctx.query(request.getStatement());
                            }
                        }

                        return waitIfNeeded
                                .then(next)
                                .doOnNext(qr -> ResultValidation.validateQueryResult(request, qr))
                                .then();
                    });
        } else if (op.hasTestFail()) {
            return Mono.defer(() -> {
                String msg = "Should not reach here";
                RuntimeException error = new InternalPerformerFailure(new IllegalStateException(msg));
                // Make absolutely certain the test fails
                testFailure.set(error);
                return Mono.error(error);
            });
        } else {
            throw new InternalPerformerFailure(new IllegalArgumentException("Unknown operation"));
        }
    }

  private static CoreTransactionLogger getLogger(ReactiveTransactionAttemptContext ctx) {
        try {
            var method = ReactiveTransactionAttemptContext.class.getDeclaredMethod("logger");
            method.setAccessible(true);
            return (CoreTransactionLogger) method.invoke(ctx);
        } catch (Throwable e) {
            throw new InternalPerformerFailure(new RuntimeException(e));
        }
    }

    private Mono<?> performOperation(Mono<Void> preOp,
                                        String opDebug,
                                        ReactiveTransactionAttemptContext ctx,
                                        List<ExpectedResult> expectedResults,
                                        boolean doNotPropagateError,
                                        boolean performanceMode,
                                        Supplier<Mono<?>> op) {
        return Mono.defer(() -> {
            long now = System.currentTimeMillis();

            if (!performanceMode) {
                logger.info("Running command '{}'", opDebug);
            }

            return preOp
                    .then(op.get())
                    .then(Mono.defer(() -> {
                        if (!performanceMode) {
                            logger.info("Took {} millis to run command '{}'", System.currentTimeMillis() - now, opDebug);
                        }
                        handleIfResultSucceededWhenShouldNot(opDebug, () -> getLogger(ctx), expectedResults);
                        return Mono.empty();
                    }))

                    .onErrorResume(err -> {
                        // This will rethrow the error unless doNotPropagateError is set
                        handleOperationError(opDebug, () -> dump(getLogger(ctx)), expectedResults, doNotPropagateError, (RuntimeException) err);
                        return Mono.empty();
                    })

                    .then();
        });
    }

    private Mono<?> performCommandBatch(CommandBatch request, Function<Tuple2<Long, TransactionCommand>, Mono<?>> call) {
        return Flux.fromIterable(request.getCommandsList())
                .doOnSubscribe(s -> logger.info("Running {} operations, concurrency={}",
                        request.getCommandsCount(), request.getParallelism()))
                .index()
                .parallel(request.getParallelism())
                .runOn(Schedulers.boundedElastic())
                .concatMap(parallelOp -> new MonoBridge<>(call.apply(parallelOp), "not-used", this, null).external()
                        .doOnNext(v -> logger.info("{} A parallel op {} has finished", parallelOp.getT1(), parallelOp.getT2().getCommandCase()))
                        .doOnCancel(() -> logger.info("{} A parallel op {} has been cancelled", parallelOp.getT1(), parallelOp.getT2().getCommandCase()))
                        .doOnError(err -> logger.info("{} A parallel op {} has errored with {}", parallelOp.getT1(), parallelOp.getT2().getCommandCase(), err.getMessage())))
                .sequential()
                .then(Mono.fromRunnable(() -> {
                    logger.info("Reached end of operations with nothing throwing");
                }))
                .doOnError(e -> logger.info("An op threw {}", e.toString()))
                .then()
                .doOnNext(v -> logger.info("All parallel ops have finished"))
                .doOnCancel(() -> logger.info("Parallel ops have been cancelled"))
                .doOnError(v -> logger.info("Parallel ops have errored"));
    }
}
