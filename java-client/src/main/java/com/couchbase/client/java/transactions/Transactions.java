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
import com.couchbase.client.core.transaction.CoreTransactionResult;
import com.couchbase.client.core.transaction.CoreTransactions;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import com.couchbase.client.java.transactions.internal.ErrorUtil;
import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * The starting point for accessing Couchbase transactions.
 * <p>
 * The main methods to run transactions are {@link Transactions#run} and {@link Transactions#reactive}.
 */
public class Transactions {
    private final CoreTransactions internal;
    private final JsonSerializer serializer;

    @Stability.Internal
    public Transactions(Core core, JsonSerializer serializer) {
        this.internal = new CoreTransactions(core, core.context().environment().transactionsConfig());
        this.serializer = Objects.requireNonNull(serializer);
    }

    /**
     * Runs supplied transactional logic until success or failure.
     * <p>
     * The supplied transactional logic will be run if necessary multiple times, until either:
     * <ul>
     * <li>The transaction successfully commits</li>
     * <li>The transactional logic requests an explicit rollback</li>
     * <li>The transaction timesout.</li>
     * <li>An exception is thrown, either inside the transaction library or by the supplied transaction logic, that
     * cannot be handled.</li>
     * </ul>
     * <p>
     * The transaction logic {@link Consumer} is provided a {@link TransactionAttemptContext}, which contains methods allowing it
     * to perform all operations that are possible inside a transaction.
     * <p>
     * Commit will be performed automatically if the lambda successfully reaches its end.
     *
     * @param transactionLogic the application's transaction logic
     * @param options        the configuration to use for this transaction
     * @return there is no need to check the returned {@link TransactionResult}, as success is implied by the lack of a
     * thrown exception.  It contains information useful only for debugging and logging.
     * @throws TransactionFailedException or a derived exception if the transaction fails to commit for any reason, possibly
     *                           after multiple retries.  The exception contains further details of the error
     */
    public TransactionResult run(Consumer<TransactionAttemptContext> transactionLogic, @Nullable TransactionOptions options) {
        try {
            CoreTransactionResult res = internal.run(ctx -> {
                TransactionAttemptContext ctxBlocking = new TransactionAttemptContext(ctx, serializer);
                transactionLogic.accept(ctxBlocking);
                return null;
            }, options == null ? null : options.build());
            return new TransactionResult(res);
        }
        catch (RuntimeException err) {
            throw ErrorUtil.convertTransactionFailedInternalBlocking(err);
        }
    }

    /**
     * Runs supplied transactional logic until success or failure.
     *
     * A convenience overload for {@link Transactions#run} that provides a default <code>TransactionOptions</code>
     */
    public TransactionResult run(Consumer<TransactionAttemptContext> transactionLogic) {
        return run(transactionLogic, null);
    }
}
