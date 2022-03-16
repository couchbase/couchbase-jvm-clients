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
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.core.transaction.CoreTransactionResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;

/**
 * The starting point for creating Couchbase transactions.
 * <p>
 * The main methods to run transactions are {@link Transactions#run} and {@link Transactions#reactive}.
 * <p>
 * Certain configurations, including the default one, will create background resources including cleanup threads, so it
 * is highly recommended that an application create and reuse just one Transactions object.
 *
 * @author Graham Pople
 */
public class Transactions {
    private final ReactiveTransactions reactive;

    @Stability.Internal
    public Transactions(Core core, JsonSerializer serializer) {
        this.reactive = new ReactiveTransactions(core, serializer);
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
     * cannot be handled.
     * </ul>
     * <p>
     * The transaction logic {@link Consumer} is provided an {@link TransactionAttemptContext}, which contains methods allowing it
     * to read, mutate, insert and delete documents, as well as commit or rollback the transaction.
     * <p>
     * If the transaction logic performs a commit or rollback it must be the last operation performed.  Else a {@link
     * com.couchbase.client.java.transactions.error.TransactionFailedException} will be thrown.  Similarly, there cannot be a commit
     * followed
     * by a rollback, or vice versa - this will also raise a {@link CoreTransactionFailedException}.
     * <p>
     * If the transaction logic does not perform an explicit commit or rollback, then a commit will be performed
     * anyway.
     *
     * @param transactionLogic the application's transaction logic
     * @param options        the configuration to use for this transaction
     * @return there is no need to check the returned {@link CoreTransactionResult}, as success is implied by the lack of a
     * thrown exception.  It contains information useful only for debugging and logging.
     * @throws TransactionFailedException or a derived exception if the transaction fails to commit for any reason, possibly
     *                           after multiple retries.  The exception contains further details of the error
     */
    public TransactionResult run(Consumer<TransactionAttemptContext> transactionLogic, @Nullable TransactionOptions options) {
        return reactive.runBlocking(transactionLogic, options == null ? null : options.build());
    }

    /**
     * Runs supplied transactional logic until success or failure.
     *
     * A convenience overload for {@link Transactions#run} that provides a default <code>PerTransactionConfig</code>
     */
    public TransactionResult run(Consumer<TransactionAttemptContext> transactionLogic) {
        return run(transactionLogic, null);
    }
}
