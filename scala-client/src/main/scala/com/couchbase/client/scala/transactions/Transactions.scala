/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

import com.couchbase.client.core.transaction.{CoreTransactionResult, CoreTransactions}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.transactions.config.TransactionOptions
import com.couchbase.client.scala.transactions.internal.ErrorUtil

import scala.util.{Failure, Success, Try}

/** The starting point for accessing Couchbase transactions.
  */
class Transactions private[scala] (
    private val internal: CoreTransactions,
    private val environment: ClusterEnvironment
) {

  /** Runs supplied transactional logic until success or failure.
    * <p>
    * The supplied transactional logic will be run if necessary multiple times, until either:
    * <ul>
    * <li>The transaction successfully commits</li>
    * <li>The transaction fails, e.g. the logic returns a Failure, or throws.</li>
    * <li>The transaction timesout.</li>
    * <li>An exception is raised, either inside the transaction library or by the supplied transaction logic, that
    * cannot be handled.</li>
    * </ul>
    * <p>
    * The transaction logic lambda is provided a [[TransactionAttemptContext]], which contains methods allowing it
    * to perform all operations that are possible inside a transaction.
    *
    * If the lambda returns a [[Success]], the transaction will commit.
    *
    * @param transactionLogic the application's transaction logic
    * @param options          the configuration to use for this transaction
    * @return there is no need to check the returned {@link CoreTransactionResult}, as success is implied by the lack of a
    *         thrown exception.  It contains information useful only for debugging and logging.
    *         Raises TransactionFailedException or a derived exception if the transaction fails to commit for any reason, possibly
    *         after multiple retries.
    */
  def run(
      transactionLogic: (TransactionAttemptContext) => Try[Unit],
      options: TransactionOptions = TransactionOptions.Default
  ): Try[TransactionResult] = {
    Try(
      internal.run(
        (internalCtx) => {
          val async = new AsyncTransactionAttemptContext(internalCtx, environment)
          val ctx = new TransactionAttemptContext(async)
          transactionLogic(ctx)
        },
        options.toCore
      )
    ) match {
      case Success(value) => Success(TransactionResult(value))
      case Failure(err)   => Failure(ErrorUtil.convertTransactionFailedInternal(err))
    }
  }
}
