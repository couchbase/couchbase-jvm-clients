/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.client.scala

import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.TimeoutUtil
import reactor.core.scala.publisher.SMono

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

/** Operations on non-JSON Couchbase documents.
  *
  * This is a reactive version of the [[BinaryCollection]] API.  See also [[AsyncBinaryCollection]].
  *
  * @define Same             This reactive version performs the same functionality and takes the same parameters,
  *                          but returns the same result object asynchronously in a `SMono`.  See the equivalent
  *                          method in [[BinaryCollection]] for details.
  * @author Graham Pople
  * @since 1.0.0
  */
class ReactiveBinaryCollection(private val async: AsyncBinaryCollection) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  private[scala] val kvTimeout: Durability => Duration = TimeoutUtil.kvTimeout(async.environment)
  private[scala] val environment                       = async.environment

  /** Add bytes to the end of a Couchbase binary document.
    *
    * $Same
    */
  def append(
      id: String,
      content: Array[Byte],
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): SMono[MutationResult] = {
    SMono.defer(() => SMono.fromFuture(async.append(id, content, cas, durability, timeout)))
  }

  /** Add bytes to the end of a Couchbase binary document.
    *
    * $Same
    */
  def append(
      id: String,
      content: Array[Byte],
      options: AppendOptions
  ): SMono[MutationResult] = {
    SMono.defer(() => SMono.fromFuture(async.append(id, content, options)))
  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * $Same
    * */
  def prepend(
      id: String,
      content: Array[Byte],
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): SMono[MutationResult] = {
    SMono.defer(() => SMono.fromFuture(async.prepend(id, content, cas, durability, timeout)))
  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * $Same
    * */
  def prepend(
      id: String,
      content: Array[Byte],
      options: PrependOptions
  ): SMono[MutationResult] = {
    SMono.defer(() => SMono.fromFuture(async.prepend(id, content, options)))
  }

  /** Increment a Couchbase 'counter' document.
    *
    * $Same
    * */
  def increment(
      id: String,
      delta: Long,
      initial: Option[Long] = None,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): SMono[CounterResult] = {
    SMono.defer(
      () => SMono.fromFuture(async.increment(id, delta, initial, durability, timeout))
    )
  }

  /** Increment a Couchbase 'counter' document.
    *
    * $Same
    * */
  def increment(
      id: String,
      delta: Long,
      options: IncrementOptions
  ): SMono[CounterResult] = {
    SMono.defer(() => SMono.fromFuture(async.increment(id, delta, options)))
  }

  /** Decrement a Couchbase 'counter' document.
    *
    * $Same
    * */
  def decrement(
      id: String,
      delta: Long,
      initial: Option[Long] = None,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): SMono[CounterResult] = {
    SMono.defer(
      () => SMono.fromFuture(async.decrement(id, delta, initial, durability, timeout))
    )
  }

  /** Decrement a Couchbase 'counter' document.
    *
    * $Same
    * */
  def decrement(
      id: String,
      delta: Long,
      options: DecrementOptions
  ): SMono[CounterResult] = {
    SMono.defer(() => SMono.fromFuture(async.decrement(id, delta, options)))
  }
}
