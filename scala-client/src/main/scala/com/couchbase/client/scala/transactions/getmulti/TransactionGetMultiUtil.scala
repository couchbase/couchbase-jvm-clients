/*
 * Copyright 2025 Couchbase, Inc.
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
package com.couchbase.client.scala.transactions.getmulti

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.transaction.CoreTransactionOptionalGetMultiResult
import com.couchbase.client.core.transaction.components.CoreTransactionGetMultiSpec
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.transactions.TransactionGetResult

import java.util.Optional
import scala.jdk.CollectionConverters._

private[transactions] object TransactionGetMultiUtil {

  private def toCollectionIdentifier(collection: Collection): CollectionIdentifier = {
    new CollectionIdentifier(
      collection.bucketName,
      Optional.of(collection.async.scopeName),
      Optional.of(collection.async.name)
    )
  }

  def convert(specs: Seq[TransactionGetMultiSpec]): java.util.List[CoreTransactionGetMultiSpec] = {
    specs.zipWithIndex.map { case (spec, idx) =>
      new CoreTransactionGetMultiSpec(toCollectionIdentifier(spec.collection), spec.id, idx)
    }.asJava
  }

  def convertReplica(
      specs: Seq[TransactionGetMultiReplicasFromPreferredServerGroupSpec]
  ): java.util.List[CoreTransactionGetMultiSpec] = {
    specs.zipWithIndex.map { case (spec, idx) =>
      new CoreTransactionGetMultiSpec(toCollectionIdentifier(spec.collection), spec.id, idx)
    }.asJava
  }

  def convert(
      result: java.util.List[CoreTransactionOptionalGetMultiResult],
      specs: Seq[TransactionGetMultiSpec]
  ): TransactionGetMultiResult = {
    val buf = Vector.newBuilder[TransactionGetMultiSpecResult]
    for (i <- 0 until result.size()) {
      val spec = specs(i)
      val r    = Option(result.get(i).internal.orElse(null)).map(res =>
        TransactionGetResult(res, spec.transcoder)
      )
      buf += TransactionGetMultiSpecResult(spec, r)
    }
    TransactionGetMultiResult(buf.result())
  }

  def convertReplica(
      result: java.util.List[CoreTransactionOptionalGetMultiResult],
      specs: Seq[TransactionGetMultiReplicasFromPreferredServerGroupSpec]
  ): TransactionGetMultiReplicasFromPreferredServerGroupResult = {
    val buf = Vector.newBuilder[TransactionGetMultiReplicasFromPreferredServerGroupSpecResult]
    for (i <- 0 until result.size()) {
      val spec = specs(i)
      val r    = Option(result.get(i).internal.orElse(null)).map(res =>
        TransactionGetResult(res, spec.transcoder)
      )
      buf += TransactionGetMultiReplicasFromPreferredServerGroupSpecResult(spec, r)
    }
    TransactionGetMultiReplicasFromPreferredServerGroupResult(buf.result())
  }
}
