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
package com.couchbase.client.performer.scala.util

import com.couchbase.client.performer.scala.transaction.TestFailure
import com.couchbase.client.protocol.transactions.{CommandQuery, ExpectedResult}
import com.couchbase.client.scala.json
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query.{QueryMetaData, QueryResult, ReactiveQueryResult}
import com.couchbase.client.scala.transactions.TransactionQueryResult
import reactor.core.scala.publisher.SMono

object ResultValidation {
  private val AnythingAllowed =
    ExpectedResult.newBuilder.setAnythingAllowed(true).build
  def validateQueryResult(request: CommandQuery, qr: TransactionQueryResult): Unit = {
    validateQueryResult(request, qr.rowsAs[JsonObject].get, qr.metaData.metrics.get.mutationCount)
  }
  def validateQueryResult(request: CommandQuery, qr: QueryResult): Unit = {
    validateQueryResult(request, qr.rowsAs[JsonObject].get, qr.metaData.metrics.get.mutationCount)
  }
  def validateQueryResult(
      request: CommandQuery,
      rows: Seq[JsonObject],
      mutationCount: Long
  ): Unit = {
    if (request.getCheckRowCount)
      if (rows.size != request.getExpectedRowCount)
        throw new TestFailure(
          new IllegalStateException(
            "Expected %d rows but only got %d".format(request.getExpectedRowCount, rows.size)
          )
        )
    if (request.getCheckMutations)
      if (mutationCount != request.getExpectedMutations)
        throw new TestFailure(
          new IllegalStateException(
            "Expected %d mutations but only got %d"
              .format(request.getExpectedMutations, mutationCount)
          )
        )
    if (request.getCheckRowContent) {
      if (rows.size != request.getExpectedRowCount)
        throw new TestFailure(
          new IllegalStateException(
            "Expected %d rows but only got %d".format(request.getExpectedRowCount, rows.size)
          )
        )
      for (i <- rows.indices) {
        val expected = JsonObject.fromJson(request.getExpectedRowsList.get(i))
        val row      = rows(i)
        if (expected.size != row.size)
          throw new TestFailure(
            new IllegalStateException(
              "Expected %s fields in row %d but only got %d".format(expected.size, i, row.size)
            )
          )
        for (name <- expected.names) {
          if (!row.containsKey(name))
            throw new TestFailure(
              new IllegalStateException(
                "Expected field %s in row %d but not found".format(name, i)
              )
            )
          if (!(row.get(name) == expected.get(name)))
            throw new TestFailure(
              new IllegalStateException(
                "Expected %s=%s in row %d but got %s"
                  .format(name, expected.get(name), i, row.get(name))
              )
            )
        }
      }
    }
  }
  def validateQueryResult(request: CommandQuery, qr: ReactiveQueryResult): SMono[Unit] =
    qr.rowsAs[json.JsonObject]
      .collectSeq()
      .flatMap((rows) =>
        qr.metaData.doOnNext((metaData: QueryMetaData) =>
          validateQueryResult(request, rows, metaData.metrics.get.mutationCount)
        )
      )
      .`then`()

  def anythingAllowed(expectedResults: Seq[ExpectedResult]): Boolean =
    expectedResults.isEmpty || expectedResults.contains(AnythingAllowed)

  def dbg(expectedResults: Seq[ExpectedResult]): String =
    expectedResults.mkString("one of (", ",", ")")
}
