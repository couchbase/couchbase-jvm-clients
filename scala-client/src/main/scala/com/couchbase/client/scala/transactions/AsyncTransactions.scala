/*
 * Copyright 2024 Couchbase, Inc.
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
package com.couchbase.client.scala.transactions

import com.couchbase.client.core.transaction.CoreTransactionsReactive
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.transactions.config.TransactionOptions
import com.couchbase.client.scala.transactions.internal.ErrorUtil
import com.couchbase.client.scala.util.FutureConversions
import reactor.core.scala.publisher.SMono

import scala.concurrent.Future

/**
  * An asynchronous version of [[Transactions]], allowing transactions to be created and run in an asynchronous
  * manner using [[scala.concurrent.Future]].
  */
class AsyncTransactions private[scala] (
    private val internal: CoreTransactionsReactive,
    private val env: ClusterEnvironment
) {

  /**
    * Runs the supplied transactional logic until success or failure.
    * <p>
    * This is the asynchronous version of [[Transactions.run()]], so to cover the differences:
    * <ul>
    * <li>The transaction logic is supplied with a [[AsyncTransactionAttemptContext]], which contains asynchronous
    * methods to allow it to read, mutate, insert and delete documents.</li>
    * <li>The transaction logic should return a <code>Future[Unit]</code>.</li>
    * <li>This method returns a <code>Future[TransactionResult]</code>, which should be handled as a normal Scala Future.</li>
    * </ul>
    *
    * @param transactionLogic the application's transaction logic
    * @param options        the configuration to use for this transaction
    * @return there is no need to check the returned [[TransactionResult]], as success is implied by the lack of a
    *         raised Future.failed().  It contains information useful only for debugging and logging.
    *         Will return Future.failed([[TransactionFailedException]]) or a derived exception if the transaction fails
    *         for any reason, possibly after multiple retries.  The exception contains further details of the error.
    */
  def run(
      transactionLogic: (AsyncTransactionAttemptContext) => Future[Unit],
      options: TransactionOptions
  ): Future[TransactionResult] = {
    run(transactionLogic, Some(options))
  }

  /**
    * A convenience overload of [[AsyncTransactions.run()]] that provides default options.
    */
  def run(
      transactionLogic: (AsyncTransactionAttemptContext) => Future[Unit]
  ): Future[TransactionResult] = {
    run(transactionLogic, None)
  }

  private def run(
      transactionLogic: (AsyncTransactionAttemptContext) => Future[Unit],
      options: Option[TransactionOptions] = None
  ): Future[TransactionResult] = {
    val opts = options.map(v => v.toCore).orNull

    FutureConversions
      .javaMonoToScalaMono(
        internal
          .run(ctx => {
            val lambdaResult = transactionLogic(new AsyncTransactionAttemptContext(ctx, env))
            SMono.fromFuture(lambdaResult)(env.ec).asJava
          }, opts)
      )
      .map(TransactionResult)
      .onErrorResume(ErrorUtil.convertTransactionFailedInternal)
      .toFuture
  }
}
