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

import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig
import com.couchbase.client.core.transaction.threadlocal.{TransactionMarker, TransactionMarkerOwner}
import com.couchbase.client.core.transaction.{CoreTransactionAttemptContext, CoreTransactionContext, CoreTransactionResult, CoreTransactionsReactive}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.transactions.config.TransactionOptions
import com.couchbase.client.scala.transactions.internal.ErrorUtil
import reactor.core.publisher.Mono

import java.util.UUID
import java.util.function.{Consumer, Function}
import scala.compat.java8.OptionConverters._
import scala.util.{Failure, Success, Try}

/**
  * The starting point for accessing Couchbase transactions.
  */
class Transactions private[scala] (
                                    private val internal: CoreTransactionsReactive,
                                    private val environment: ClusterEnvironment) {

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
    * @param options        the configuration to use for this transaction
    * @return there is no need to check the returned {@link CoreTransactionResult}, as success is implied by the lack of a
    * thrown exception.  It contains information useful only for debugging and logging.
    * Raises TransactionFailedException or a derived exception if the transaction fails to commit for any reason, possibly
    * after multiple retries.
    */
  def run(
      transactionLogic: (TransactionAttemptContext) => Try[Unit],
      options: TransactionOptions = TransactionOptions.Default
  ): Try[TransactionResult] = {
      val scheduler = internal.core.context.environment.transactionsSchedulers.schedulerBlocking

      val x: Mono[CoreTransactionResult] = Mono.defer(() => {
          val merged =
              new CoreMergedTransactionConfig(internal.config(), java.util.Optional.of(options.toCore))
          val overall =
              new CoreTransactionContext(
                  internal.core().context(),
                  UUID.randomUUID().toString(),
                  merged,
                  internal.core().transactionsCleanup()
              )

          val createAttempt: Mono[CoreTransactionAttemptContext] = Mono.defer(() => {
              val attemptId = UUID.randomUUID().toString()
              val ctx       = internal.createAttemptContext(overall, merged, attemptId)
              Mono.just(ctx)
          })

          val newTransactionLogic: Function[CoreTransactionAttemptContext, Mono[Void]] =
              (ctx: CoreTransactionAttemptContext) =>
                  Mono.defer(() => {
                      val async       = new AsyncTransactionAttemptContext(ctx, environment)
                      val ctxBlocking = new TransactionAttemptContext(async)
                      Mono
                              .fromCallable(() => {
                                  TransactionMarkerOwner.set(new TransactionMarker(ctx))
                                  val out = Try(transactionLogic(ctxBlocking))
                                  TransactionMarkerOwner.clear()
                                  out match {
                                      // If the lambda has thrown an exception directly.
                                      case Failure(exception) => throw exception

                                      // If the user has returned a Try wrapping an exception.
                                      case Success(Failure(exception)) => throw exception

                                      // Lambda ended successfully
                                      case _ =>
                                  }
                              })
                              .subscribeOn(scheduler)
                              .`then`()
                  })

          internal
                  .executeTransaction(createAttempt, merged, overall, newTransactionLogic, false)
                  .onErrorResume((err: Throwable) => ErrorUtil.convertTransactionFailedInternal[CoreTransactionResult](err))
      })

      Try(
          x.map[TransactionResult](new Function[CoreTransactionResult, TransactionResult] {
                    override def apply(result: CoreTransactionResult): TransactionResult = TransactionResult(result)
                })
                  .publishOn(scheduler)
                  .block()
      )
  }
}
