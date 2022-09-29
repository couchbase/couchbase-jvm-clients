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

package com.couchbase.client.java.transactions;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionContext;
import com.couchbase.client.core.transaction.CoreTransactionsReactive;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionOptions;
import com.couchbase.client.core.transaction.threadlocal.TransactionMarker;
import com.couchbase.client.core.transaction.threadlocal.TransactionMarkerOwner;
import com.couchbase.client.core.transaction.util.CoreTransactionsSchedulers;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.couchbase.client.java.transactions.internal.ErrorUtil;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An asynchronous version of {@link Transactions}, allowing transactions to be created and run in an asynchronous
 * manner.
 * <p>
 * The main method to run transactions is {@link ReactiveTransactions#run}.
 */
public class ReactiveTransactions {
    private final CoreTransactionsReactive internal;
    private final JsonSerializer serializer;

    @Stability.Internal
    public ReactiveTransactions(Core core, JsonSerializer serializer) {
        Objects.requireNonNull(core);

        this.internal = new CoreTransactionsReactive(core, core.context().environment().transactionsConfig());
        this.serializer = Objects.requireNonNull(serializer);
    }

    /**
     * Runs the supplied transactional logic until success or failure.
     * <p>
     * This is the asynchronous version of {@link Transactions#run}, so to cover the differences:
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
     * @param options        the configuration to use for this transaction
     * @return there is no need to check the returned {@link TransactionResult}, as success is implied by the lack of a
     * thrown exception.  It contains information useful only for debugging and logging.
     * @throws TransactionFailedException or a derived exception if the transaction fails to commit for any reason, possibly
     *                           after multiple retries.  The exception contains further details of the error.
     */
    public Mono<TransactionResult> run(Function<ReactiveTransactionAttemptContext, Mono<?>> transactionLogic,
                                               @Nullable TransactionOptions options) {
        return internal.run((ctx) -> transactionLogic.apply(new ReactiveTransactionAttemptContext(ctx, serializer)), options == null ? null : options.build())
                .onErrorResume(ErrorUtil::convertTransactionFailedInternal)
                .map(TransactionResult::new);
    }

    /**
     * Convenience overload that runs {@link ReactiveTransactions#run} with a default <code>PerTransactionConfig</code>.
     */
    public Mono<TransactionResult> run(Function<ReactiveTransactionAttemptContext, Mono<?>> transactionLogic) {
        return run(transactionLogic, null);
    }

    TransactionResult runBlocking(Consumer<TransactionAttemptContext> transactionLogic, @Nullable CoreTransactionOptions perConfig) {
        return Mono.defer(() -> {
            CoreMergedTransactionConfig merged = new CoreMergedTransactionConfig(internal.config(), Optional.ofNullable(perConfig));
            CoreTransactionContext overall =
                    new CoreTransactionContext(internal.core().context(),
                            UUID.randomUUID().toString(),
                            merged,
                            internal.core().transactionsCleanup());
            // overall.LOGGER.info(configDebug(config, perConfig, cleanup.clusterData().cluster().core()));

            Mono<CoreTransactionAttemptContext> createAttempt = Mono.defer(() -> {
                String attemptId = UUID.randomUUID().toString();
                return Mono.just(internal.createAttemptContext(overall, merged, attemptId));
            });

            Function<CoreTransactionAttemptContext, Mono<Void>> newTransactionLogic = (ctx) -> Mono.defer(() -> {
                TransactionAttemptContext ctxBlocking = new TransactionAttemptContext(ctx, serializer);
                return Mono.fromRunnable(() -> {
                            TransactionMarkerOwner.set(new TransactionMarker(ctx));
                            try {
                                transactionLogic.accept(ctxBlocking);
                            }
                            finally {
                                TransactionMarkerOwner.clear();
                            }
                        })
                        .subscribeOn(internal.core().context().environment().transactionsSchedulers().schedulerBlocking())
                        .then();
            });

            return internal.executeTransaction(createAttempt, merged, overall, newTransactionLogic, false)
                    .onErrorResume(ErrorUtil::convertTransactionFailedInternal);
        }).map(TransactionResult::new)
                .publishOn(internal.core().context().environment().transactionsSchedulers().schedulerBlocking()).block();
    }
}
