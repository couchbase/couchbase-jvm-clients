/*
 * Copyright (c) 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.twoway;

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.transaction.internal.TestFailOtherException;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.threadlocal.TransactionMarkerOwner;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionQueryResult;
import com.couchbase.client.java.transactions.TransactionResult;
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
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Version of TwoWayTransaction that uses the blocking API.
 */
public class TwoWayTransactionBlocking extends TwoWayTransactionShared {
    /**
     * Starts a transaction that will run until completion.
     */
    public static com.couchbase.client.protocol.transactions.TransactionResult run(
            ClusterConnection connection,
            TransactionCreateRequest req) {
        TwoWayTransactionBlocking txn = new TwoWayTransactionBlocking();
        return txn.run(connection, req, null);
    }

    /**
     * Starts a two-way transaction.
     */
    public com.couchbase.client.java.transactions.TransactionResult runInternal(
        ClusterConnection connection,
        TransactionCreateRequest req,
        @Nullable StreamObserver<TransactionStreamPerformerToDriver> toTest
    ) {
        AtomicInteger attemptCount = new AtomicInteger(-1);

        if (req.getApi() != API.DEFAULT) {
            throw new InternalPerformerFailure(new IllegalStateException("Unexpected API"));
        }

        TransactionOptions ptcb = OptionsUtil.makeTransactionOptions(connection, req);

        TransactionResult out = connection.cluster().transactions().run((ctx) -> {
            if (testFailure.get() != null) {
                logger.info("Test failure is set at start of new attempt, fast failing transaction");
                throw testFailure.get();
            }

            final int count = attemptCount.incrementAndGet();
            // logger.info("Starting attempt {} {}", count, ctx.attemptId());

            int attemptToUse = Math.min(count, req.getAttemptsCount() - 1);
            TransactionAttemptRequest attempt = req.getAttempts(attemptToUse);

            for (TransactionCommand command : attempt.getCommandsList()) {
                performOperation(connection, ctx, command, toTest);
            }

            logger.info("Reached end of all operations and lambda");
        }, ptcb);

        if (TransactionMarkerOwner.get().block().isPresent()) {
            throw new InternalPerformerFailure(new IllegalStateException("Still in blocking transaction context after completion"));
        }

        return out;
    }

    private static CoreTransactionLogger getLogger(TransactionAttemptContext ctx) {
        try {
            var method = TransactionAttemptContext.class.getDeclaredMethod("logger");
            method.setAccessible(true);
            return (CoreTransactionLogger) method.invoke(ctx);
        } catch (Throwable e) {
            throw new InternalPerformerFailure(new RuntimeException(e));
        }
    }

    private void performOperation(ClusterConnection connection,
                                  TransactionAttemptContext ctx,
                                  TransactionCommand op,
                                  @Nullable StreamObserver<TransactionStreamPerformerToDriver> toTest) {
        if (op.getWaitMSecs() != 0) {
            try {
                logger.info("Sleeping for Msecs: "+op.getWaitMSecs());
                Thread.sleep(op.getWaitMSecs());
            } catch (InterruptedException e) {
                throw new InternalPerformerFailure(new RuntimeException(e));
            }
        }

        if (op.hasInsert()) {
            final CommandInsert request = op.getInsert();
            var content = readJson(request.getContentJson());
            final Collection collection = connection.collection(request.getDocId());

            performOperation("insert " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(),op.getDoNotPropagateError(),
                () -> {
                    logger.info("Performing insert operation on {} on bucket {} on collection {}",
                            request.getDocId().getDocId(),request.getDocId().getBucketName(), request.getDocId().getCollectionName());
                    ctx.insert(collection,
                        request.getDocId().getDocId(),
                        content);
                });
        } else if (op.hasReplace()) {
            final CommandReplace request = op.getReplace();
            var content = readJson(request.getContentJson());

            performOperation("replace " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(),
                () -> {
                    if (request.getUseStashedResult()) {
                        if (stashedGet.get() == null) {
                            throw new IllegalStateException("Test has not performed a get");
                        }

                        logger.info("Performing replace operation on stashed get {} to new content {}",
                                stashedGet.get(), request.getContentJson());
                        ctx.replace(stashedGet.get(), content);
                    } else if (request.hasUseStashedSlot()) {
                        if (!stashedGetMap.containsKey(request.getUseStashedSlot())) {
                            throw new IllegalStateException("Do not have a stashed get in slot " + request.getUseStashedSlot());
                        }
                        ctx.replace(stashedGetMap.get(request.getUseStashedSlot()), content);
                    } else {
                        final Collection collection = connection.collection(request.getDocId());
                        logger.info("Performing replace operation on docId {} to new content {} on collection {}",
                                request.getDocId().getDocId(), request.getContentJson(),request.getDocId().getCollectionName());
                        final TransactionGetResult r = ctx.get(collection, request.getDocId().getDocId());
                        ctx.replace(r, content);
                    }
                });
        } else if (op.hasRemove()) {
            final CommandRemove request = op.getRemove();

            performOperation("remove " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(),
                () -> {
                    if (request.getUseStashedResult()) {
                        if (stashedGet.get() == null) {
                            throw new IllegalStateException("Test has not performed a get");
                        }

                        logger.info("Performing remove operation on stashed get {}", stashedGet.get());
                        ctx.remove(stashedGet.get());
                    } else if (request.hasUseStashedSlot()) {
                        if (!stashedGetMap.containsKey(request.getUseStashedSlot())) {
                            throw new IllegalStateException("Do not have a stashed get in slot " + request.getUseStashedSlot());
                        }
                        ctx.remove(stashedGetMap.get(request.getUseStashedSlot()));
                    } else {
                        final Collection collection = connection.collection(request.getDocId());
                        logger.info("Performing remove operation on docId on {}", request.getDocId().getDocId());
                        TransactionGetResult r = ctx.get(collection, request.getDocId().getDocId());
                        ctx.remove(r);
                    }
                });
        } else if (op.hasCommit()) {
            // Ignoring - explicit commit removed in ExtSDKIntegration
        } else if (op.hasRollback()) {
            throw new RuntimeException("Driver has requested app-rollback");
        } else if (op.hasGet()) {
            final CommandGet request = op.getGet();
            final Collection collection = connection.collection(request.getDocId());

            performOperation("get " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(),
                    () -> {
                        logger.info("Performing get operation on {} on bucket {} on collection {}", request.getDocId().getDocId(),request.getDocId().getBucketName(),request.getDocId().getCollectionName());
                        TransactionGetResult out = ctx.get(collection, request.getDocId().getDocId());
                        handleGetResult(request, out, connection);
                    });
        } else if (op.hasGetOptional()) {
            final CommandGetOptional req = op.getGetOptional();
            final CommandGet request = req.getGet();
            final Collection collection = connection.collection(request.getDocId());

            performOperation("get optional " + request.getDocId().getDocId(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(),
                    () -> {
                        logger.info("Performing getOptional operation on {} on bucket {} on collection {} ", request.getDocId().getDocId(),request.getDocId().getBucketName(),request.getDocId().getCollectionName());
                        Optional<TransactionGetResult> out = Optional.empty();
                        try {
                            out = Optional.of(ctx.get(collection, request.getDocId().getDocId()));
                        }
                        catch (DocumentNotFoundException ignored) {
                        }
                        handleGetOptionalResult(request, req, out, connection);
                    });
        } else if (op.hasWaitOnLatch()) {
            final CommandWaitOnLatch request = op.getWaitOnLatch();
            final String latchName = request.getLatchName();
            performOperation("wait on latch " + latchName, ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(),
                () -> handleWaitOnLatch(request, getLogger(ctx)));
        } else if (op.hasSetLatch()) {
            final CommandSetLatch request = op.getSetLatch();
            final String latchName = request.getLatchName();
            performOperation("set latch " + latchName, ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(),
                    () -> handleSetLatch(request, toTest, getLogger(ctx)));
        } else if (op.hasParallelize()) {
            final CommandBatch request = op.getParallelize();

            performCommandBatch(request, (parallelOp) -> Mono.fromRunnable(() -> {
                performOperation(connection, ctx, parallelOp, toTest);
            }).then()).block();
        } else if (op.hasInsertRegularKV()) {
            final CommandInsertRegularKV request = op.getInsertRegularKV();
            final Collection collection = connection.collection(request.getDocId());

            performOperation("KV insert " + request.getDocId().getDocId(), ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(),
                    () -> {
                        JsonObject content = JsonObject.fromJson(request.getContentJson());
                        collection.insert(request.getDocId().getDocId(), content);
                    });
        } else if (op.hasReplaceRegularKV()) {
            final CommandReplaceRegularKV request = op.getReplaceRegularKV();
            final Collection collection = connection.collection(request.getDocId());

            performOperation("KV replace " + request.getDocId().getDocId(), ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(),
                    () -> {
                        JsonObject content = JsonObject.fromJson(request.getContentJson());
                        collection.replace(request.getDocId().getDocId(), content);
                    });
        } else if (op.hasRemoveRegularKV()) {
            final CommandRemoveRegularKV request = op.getRemoveRegularKV();
            final Collection collection = connection.collection(request.getDocId());

            performOperation("KV remove " + request.getDocId().getDocId(), ctx, Collections.singletonList(EXPECT_SUCCESS), op.getDoNotPropagateError(),
                    () -> {
                        collection.remove(request.getDocId().getDocId());
                    });
        } else if (op.hasThrowException()) {
            getLogger(ctx).info("Test throwing a TestFailOther exception here");
            logger.info("Throwing exception");

            throw new TestFailOtherException();
        } else if (op.hasQuery()) {
            final CommandQuery request = op.getQuery();

            performOperation("Query " + request.getStatement(), ctx, request.getExpectedResultList(), op.getDoNotPropagateError(),
                    () -> {
                        com.couchbase.client.java.transactions.TransactionQueryOptions queryOptions = OptionsUtil.transactionQueryOptions(request);

                        TransactionQueryResult qr;

                        if (request.hasScope()) {
                            String bucketName = request.getScope().getBucketName();
                            String scopeName = request.getScope().getScopeName();
                            Scope scope = connection.cluster().bucket(bucketName).scope(scopeName);

                            logger.info("Using Custom Scope in this Query : {} ", scopeName);

                            if (queryOptions != null) {
                                qr = ctx.query(scope, request.getStatement(), queryOptions);
                            }
                            else {
                                qr = ctx.query(scope, request.getStatement());
                            }
                        } else {
                            if (queryOptions != null) {
                                qr = ctx.query(request.getStatement(), queryOptions);
                            }
                            else {
                                // Passing null should work fine, but let's test the full pathway
                                qr = ctx.query(request.getStatement());
                            }
                        }

                        ResultValidation.validateQueryResult(request, qr);
                    });
        } else if (op.hasTestFail()) {
            String msg = "Should not reach here";
            RuntimeException error = new InternalPerformerFailure(new IllegalStateException(msg));
            // Make absolutely certain the test fails
            testFailure.set(error);
            throw error;
        } else {
            throw new InternalPerformerFailure(new IllegalArgumentException("Unknown operation"));
        }
    }

    private void performOperation(String opDebug,
                                  TransactionAttemptContext ctx,
                                  List<ExpectedResult> expectedResults,
                                  boolean doNotPropagateError,
                                  Runnable op) {
        try {
            long now = System.currentTimeMillis();

            logger.info("Running command '{}'", opDebug);

            op.run();

            logger.info("Took {} millis to run command '{}'", System.currentTimeMillis() - now, opDebug);

            handleIfResultSucceededWhenShouldNot(opDebug, () -> getLogger(ctx), expectedResults);

        } catch (RuntimeException err) {
            handleOperationError(opDebug, () -> dump(getLogger(ctx)), expectedResults, doNotPropagateError, err);
        }
    }

}
