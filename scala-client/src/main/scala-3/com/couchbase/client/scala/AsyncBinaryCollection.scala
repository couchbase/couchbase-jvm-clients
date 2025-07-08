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
package com.couchbase.client.scala

import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import com.couchbase.client.scala.util.ExpiryUtil

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** Operations on non-JSON Couchbase documents.
  *
  * @define Same             This asynchronous version performs the same functionality and takes the same parameters,
  *                          but returns the same result object asynchronously in a `Future`.  See the equivalent
  *                          method in [[BinaryCollection]] for details.
  * @author Graham Pople
  * @since 1.0.0
  */
class AsyncBinaryCollection(private[scala] val async: AsyncCollection) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  private[scala] val environment                       = async.environment
  private[scala] val kvTimeout: Durability => Duration = async.kvTimeout
  private[scala] val kvBinaryOps                       = async.couchbaseOps.kvBinaryOps(async.keyspace)

  /** Add bytes to the end of a Couchbase binary document.
    *
    * $Same
    */
  def append(
      id: String,
      content: Array[Byte],
      options: AppendOptions = AppendOptions.Default
  ): Future[MutationResult] = {
    convert(
      kvBinaryOps.appendAsync(
        id,
        content,
        convert(options),
        options.cas,
        convert(options.durability)
      )
    ).map(result => convert(result))
  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * $Same
    * */
  def prepend(
      id: String,
      content: Array[Byte],
      options: PrependOptions = PrependOptions.Default
  ): Future[MutationResult] = {
    convert(
      kvBinaryOps.prependAsync(
        id,
        content,
        convert(options),
        options.cas,
        convert(options.durability)
      )
    ).map(result => convert(result))
  }

  /** Increment a Couchbase 'counter' document.
    *
    * $Same
    * */
  def increment(
      id: String,
      delta: Long,
      options: IncrementOptions = IncrementOptions.Default
  ): Future[CounterResult] = {
    convert(
      kvBinaryOps.incrementAsync(
        id,
        convert(options),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime),
        delta,
        options.initial.map(v => Long.box(v)).asJava,
        convert(options.durability)
      )
    ).map(result => convert(result))
  }

  /** Decrement a Couchbase 'counter' document.
    *
    * $Same
    * */
  def decrement(
      id: String,
      delta: Long,
      options: DecrementOptions = DecrementOptions.Default
  ): Future[CounterResult] = {
    convert(
      kvBinaryOps.decrementAsync(
        id,
        convert(options),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime),
        delta,
        options.initial.map(v => Long.box(v)).asJava,
        convert(options.durability)
      )
    ).map(result => convert(result))
  }
}
