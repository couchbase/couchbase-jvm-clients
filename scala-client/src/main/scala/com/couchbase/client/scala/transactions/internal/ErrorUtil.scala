/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.client.scala.transactions.internal

import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.error.transaction.internal.{
  CoreTransactionCommitAmbiguousException,
  CoreTransactionExpiredException,
  CoreTransactionFailedException
}
import com.couchbase.client.scala.transactions.error.{
  TransactionCommitAmbiguousException,
  TransactionExpiredException,
  TransactionFailedException
}
import reactor.core.publisher.Mono

private[scala] object ErrorUtil {

  def convertTransactionFailedInternal[T](err: Throwable): Mono[T] = {
    Mono.error[T](err match {
      case e: CoreTransactionCommitAmbiguousException =>
        new TransactionCommitAmbiguousException(e)
      case e: CoreTransactionExpiredException =>
        new TransactionExpiredException(e)
      case e: CoreTransactionFailedException =>
        new TransactionFailedException(e)
    })
  }

  def convertTransactionFailedSingleQueryMono[T](err: Throwable): Mono[T] = {
    convertTransactionFailedInternal(err)
      .onErrorResume {
        // From a cluster.query() transaction the user will be expecting the traditional SDK errors
        case ex: TransactionExpiredException =>
          Mono.error[T](new UnambiguousTimeoutException(ex.getMessage, null))
        case ex => Mono.error[T](ex)
      };
  }

  def convertTransactionFailedSingleQuery[T](err: RuntimeException): RuntimeException = {
    convertTransactionFailedSingleQueryMono(err).block()
  }
}
