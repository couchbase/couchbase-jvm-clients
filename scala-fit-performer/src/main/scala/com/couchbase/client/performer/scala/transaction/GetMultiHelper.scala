/*
 * Copyright (c) 2025 Couchbase, Inc.
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
// [skip:<3.9.0]
package com.couchbase.client.performer.scala.transaction

import com.couchbase.client.core.transaction.log.CoreTransactionLogger
import com.couchbase.client.performer.scala.error.InternalPerformerFailure

import com.couchbase.client.performer.scala.util.ContentAsUtil
import com.couchbase.client.protocol.transactions.CommandGetMulti
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.transactions.getmulti.{
  TransactionGetMultiMode,
  TransactionGetMultiOptions,
  TransactionGetMultiReplicasFromPreferredServerGroupMode,
  TransactionGetMultiReplicasFromPreferredServerGroupOptions,
  TransactionGetMultiReplicasFromPreferredServerGroupResult,
  TransactionGetMultiResult
}
import com.couchbase.client.protocol.transactions.TransactionGetMultiOptions.{
  TransactionGetMultiMode => ProtocolGetMultiMode
}

object GetMultiHelper {
  def convertToGetMulti(
      in: com.couchbase.client.protocol.transactions.TransactionGetMultiOptions
  ): TransactionGetMultiOptions = {
    var out = TransactionGetMultiOptions()
    if (in.hasMode) {
      out = out.mode(in.getMode match {
        case ProtocolGetMultiMode.PRIORITISE_LATENCY => TransactionGetMultiMode.PrioritiseLatency
        case ProtocolGetMultiMode.DISABLE_READ_SKEW_DETECTION =>
          TransactionGetMultiMode.DisableReadSkewDetection
        case ProtocolGetMultiMode.PRIORITISE_READ_SKEW_DETECTION =>
          TransactionGetMultiMode.PrioritiseReadSkewDetection
        case _ =>
          throw new InternalPerformerFailure(new RuntimeException("Unknown mode: " + in.getMode))
      })
    }
    out
  }

  def convertToGetMultiReplicas(
      in: com.couchbase.client.protocol.transactions.TransactionGetMultiOptions
  ): TransactionGetMultiReplicasFromPreferredServerGroupOptions = {
    var out = TransactionGetMultiReplicasFromPreferredServerGroupOptions()
    if (in.hasMode) {
      out = out.mode(in.getMode match {
        case ProtocolGetMultiMode.PRIORITISE_LATENCY =>
          TransactionGetMultiReplicasFromPreferredServerGroupMode.PrioritiseLatency
        case ProtocolGetMultiMode.DISABLE_READ_SKEW_DETECTION =>
          TransactionGetMultiReplicasFromPreferredServerGroupMode.DisableReadSkewDetection
        case ProtocolGetMultiMode.PRIORITISE_READ_SKEW_DETECTION =>
          TransactionGetMultiReplicasFromPreferredServerGroupMode.PrioritiseReadSkewDetection
        case _ =>
          throw new InternalPerformerFailure(new RuntimeException("Unknown mode: " + in.getMode))
      })
    }
    out
  }

  def handleGetMultiResult(request: CommandGetMulti, results: TransactionGetMultiResult): Unit = {
    if (request.getSpecsCount != results.size) {
      throw new TestFailure(
        new RuntimeException(s"Expected ${request.getSpecsCount} results but got ${results.size}")
      )
    }

    for (i <- 0 until request.getSpecsCount) {
      val specIndex     = i
      val requestedSpec = request.getSpecs(i)

      if (requestedSpec.getExpectPresent && !results.exists(i)) {
        throw new TestFailure(new RuntimeException(s"Require spec $i to be present but it was not"))
      }
      if (!requestedSpec.getExpectPresent && results.exists(i)) {
        throw new TestFailure(new RuntimeException(s"Require spec $i to be missing but it was not"))
      }
      if (results.exists(i)) {
        if (requestedSpec.hasContentAsValidation) {
          val expectedContent = requestedSpec.getContentAsValidation
          val actualContent   = ContentAsUtil.contentType(
            expectedContent.getContentAs,
            () => scala.util.Try(results.contentAs[Array[Byte]](specIndex).get),
            () => scala.util.Try(results.contentAs[String](specIndex).get),
            () => scala.util.Try(results.contentAs[JsonObject](specIndex).get),
            () => scala.util.Try(results.contentAs[JsonArray](specIndex).get),
            () => scala.util.Try(results.contentAs[Boolean](specIndex).get),
            () => scala.util.Try(results.contentAs[Int](specIndex).get),
            () => scala.util.Try(results.contentAs[Double](specIndex).get)
          )

          if (expectedContent.getExpectSuccess != actualContent.isSuccess) {
            throw new TestFailureRaiseFailedPrecondition(
              s"ContentAs result $actualContent did not equal expected result ${expectedContent.getExpectSuccess}"
            )
          }

          if (expectedContent.hasExpectedContentBytes) {
            val bytes = ContentAsUtil.convert(actualContent.get)
            if (
              !java.util.Arrays.equals(expectedContent.getExpectedContentBytes.toByteArray, bytes)
            ) {
              throw new TestFailureRaiseFailedPrecondition(
                s"Content bytes ${java.util.Arrays.toString(bytes)} did not equal expected bytes ${expectedContent.getExpectedContentBytes}"
              )
            }
          }
        }
      }
    }
  }

  def handleGetMultiFromPreferredServerGroupResult(
      request: CommandGetMulti,
      results: TransactionGetMultiReplicasFromPreferredServerGroupResult,
      logger: CoreTransactionLogger
  ): Unit = {
    if (request.getSpecsCount != results.size) {
      throw new TestFailure(
        new RuntimeException(s"Expected ${request.getSpecsCount} results but got ${results.size}")
      )
    }

    for (i <- 0 until request.getSpecsCount) {
      val specIndex     = i
      val requestedSpec = request.getSpecs(i)

      if (requestedSpec.getExpectPresent && !results.exists(i)) {
        throw new TestFailure(new RuntimeException(s"Require spec $i to be present but it was not"))
      }
      if (!requestedSpec.getExpectPresent && results.exists(i)) {
        throw new TestFailure(new RuntimeException(s"Require spec $i to be missing but it was not"))
      }
      if (results.exists(i)) {
        if (requestedSpec.hasContentAsValidation) {
          val expectedContent = requestedSpec.getContentAsValidation
          val actualContent   = ContentAsUtil.contentType(
            expectedContent.getContentAs,
            () => scala.util.Try(results.contentAs[Array[Byte]](specIndex).get),
            () => scala.util.Try(results.contentAs[String](specIndex).get),
            () => scala.util.Try(results.contentAs[JsonObject](specIndex).get),
            () => scala.util.Try(results.contentAs[JsonArray](specIndex).get),
            () => scala.util.Try(results.contentAs[Boolean](specIndex).get),
            () => scala.util.Try(results.contentAs[Int](specIndex).get),
            () => scala.util.Try(results.contentAs[Double](specIndex).get)
          )

          if (expectedContent.getExpectSuccess != actualContent.isSuccess) {
            throw new TestFailureRaiseFailedPrecondition(
              s"ContentAs result $actualContent did not equal expected result ${expectedContent.getExpectSuccess}"
            )
          }

          if (expectedContent.hasExpectedContentBytes) {
            val bytes = ContentAsUtil.convert(actualContent.get)
            if (
              !java.util.Arrays.equals(expectedContent.getExpectedContentBytes.toByteArray, bytes)
            ) {
              throw new TestFailureRaiseFailedPrecondition(
                s"Content bytes ${java.util.Arrays.toString(bytes)} did not equal expected bytes ${expectedContent.getExpectedContentBytes}"
              )
            }
          }
        }
      }
    }
  }
}
