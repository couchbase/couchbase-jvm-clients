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
import com.couchbase.client.core.error.{DocumentNotFoundException, InvalidArgumentException}
import com.couchbase.client.scala.codec.JsonDeserializer

import scala.reflect.ClassTag

@Stability.Uncommitted
case class TransactionGetMultiReplicasFromPreferredServerGroupResult private[transactions] (
    private val results: Vector[TransactionGetMultiReplicasFromPreferredServerGroupSpecResult]
) {
  private def validateIndex(idx: Int): Unit = {
    if (idx < 0) throw new InvalidArgumentException("Index must be >= 0", null, null)
    if (idx >= results.size) throw new InvalidArgumentException("Index must be < size", null, null)
  }

  /** True if the document at `specIndex` existed. */
  def exists(specIndex: Int): Boolean = {
    validateIndex(specIndex)
    results(specIndex).exists
  }

  private def result(
      specIndex: Int
  ): TransactionGetMultiReplicasFromPreferredServerGroupSpecResult = {
    if (!exists(specIndex)) throw new DocumentNotFoundException(null)
    results(specIndex)
  }

  /** Decode content into target class. */
  def contentAs[T](
      specIndex: Int
  )(implicit serializer: JsonDeserializer[T], tag: ClassTag[T]): scala.util.Try[T] =
    result(specIndex).get.contentAs[T](serializer, tag)

  /** Number of results. */
  def size: Int = results.size
}
