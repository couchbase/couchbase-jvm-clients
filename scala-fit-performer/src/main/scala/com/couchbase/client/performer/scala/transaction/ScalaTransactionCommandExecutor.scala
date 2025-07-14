/*
 * Copyright 2023 Couchbase, Inc.
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
// [skip:<1.5.0]
package com.couchbase.client.performer.scala.transaction

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor
import com.couchbase.client.performer.core.perf.Counters
import com.couchbase.client.performer.core.util.TimeUtil
import com.couchbase.client.performer.scala.util.ClusterConnection
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.shared.API
import com.couchbase.client.protocol.transactions.{TransactionCreateRequest, TransactionResult}

class ScalaTransactionCommandExecutor(
    connection: ClusterConnection,
    counters: Counters,
    spans: collection.Map[String, RequestSpan]
) extends TransactionCommandExecutor(counters) {
  override protected def performOperation(
      request: TransactionCreateRequest,
      performanceMode: Boolean
  ): Result = {
    var response: TransactionResult = null
    val initiated                   = TimeUtil.getTimeNow
    val startNanos                  = System.nanoTime
    if (request.getApi eq API.DEFAULT)
      response = TransactionBlocking.run(connection, request, Some(this), performanceMode, spans)
    else throw new UnsupportedOperationException
    val elapsedNanos = System.nanoTime - startNanos
    com.couchbase.client.protocol.run.Result.newBuilder
      .setElapsedNanos(elapsedNanos)
      .setInitiated(initiated)
      .setTransaction(response)
      .build
  }
}
