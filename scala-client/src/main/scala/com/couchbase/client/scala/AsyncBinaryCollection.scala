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
import com.couchbase.client.scala.kv.handlers.{
  BinaryAppendHandler,
  BinaryDecrementHandler,
  BinaryIncrementHandler,
  BinaryPrependHandler
}
import com.couchbase.client.scala.kv._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** Operations on non-JSON Couchbase documents.
  *
  * This is an asynchronous version of the [[BinaryCollection]] API.  See also [[ReactiveBinaryCollection]].
  *
  * @param ec    an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *              opened in the normal way, starting from functions in [[Cluster]]
  *
  * @define Same             This asynchronous version performs the same functionality and takes the same parameters,
  *                          but returns the same result object asynchronously in a `Future`.  See the equivalent
  *                          method in [[BinaryCollection]] for details.
  * @author Graham Pople
  * @since 1.0.0
  */
class AsyncBinaryCollection(private[scala] val async: AsyncCollection) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  import com.couchbase.client.scala.util.DurationConversions._

  private[scala] val environment                       = async.environment
  private[scala] val kvTimeout: Durability => Duration = async.kvTimeout
  private[scala] val binaryAppendHandler               = new BinaryAppendHandler(async.hp)
  private[scala] val binaryPrependHandler              = new BinaryPrependHandler(async.hp)
  private[scala] val binaryIncrementHandler            = new BinaryIncrementHandler(async.hp)
  private[scala] val binaryDecrementHandler            = new BinaryDecrementHandler(async.hp)

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
  ): Future[MutationResult] = {
    append(
      id,
      content,
      AppendOptions()
        .cas(cas)
        .durability(durability)
        .timeout(timeout)
    )
  }

  /** Add bytes to the end of a Couchbase binary document.
    *
    * $Same
    */
  def append(
      id: String,
      content: Array[Byte],
      options: AppendOptions
  ): Future[MutationResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req =
      binaryAppendHandler.request(
        id,
        content,
        options.cas,
        options.durability,
        timeoutActual,
        options.retryStrategy.getOrElse(environment.retryStrategy),
        options.parentSpan
      )
    async.wrapWithDurability(req, id, binaryAppendHandler, options.durability, false, timeoutActual)
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
  ): Future[MutationResult] = {
    prepend(
      id,
      content,
      PrependOptions()
        .cas(cas)
        .durability(durability)
        .timeout(timeout)
    )
  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * $Same
    * */
  def prepend(
      id: String,
      content: Array[Byte],
      options: PrependOptions
  ): Future[MutationResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req =
      binaryPrependHandler.request(
        id,
        content,
        options.cas,
        options.durability,
        timeoutActual,
        options.retryStrategy.getOrElse(environment.retryStrategy),
        options.parentSpan
      )
    async.wrapWithDurability(
      req,
      id,
      binaryPrependHandler,
      options.durability,
      false,
      timeoutActual
    )
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
  ): Future[CounterResult] = {
    var opts = IncrementOptions()
      .durability(durability)
      .timeout(timeout)
    initial.foreach(v => opts = opts.initial(v))
    increment(id, delta, opts)
  }

  /** Increment a Couchbase 'counter' document.
    *
    * $Same
    * */
  def increment(
      id: String,
      delta: Long,
      options: IncrementOptions
  ): Future[CounterResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req = binaryIncrementHandler.request(
      id,
      delta,
      options.initial,
      options.durability,
      options.expiry,
      timeoutActual,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    async.wrapWithDurability(
      req,
      id,
      binaryIncrementHandler,
      options.durability,
      false,
      timeoutActual
    )
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
  ): Future[CounterResult] = {
    var opts = DecrementOptions()
      .durability(durability)
      .timeout(timeout)
    initial.foreach(v => opts = opts.initial(v))
    decrement(id, delta, opts)
  }

  /** Decrement a Couchbase 'counter' document.
    *
    * $Same
    * */
  def decrement(
      id: String,
      delta: Long,
      options: DecrementOptions
  ): Future[CounterResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req = binaryDecrementHandler.request(
      id,
      delta,
      options.initial,
      options.durability,
      options.expiry,
      timeoutActual,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    async.wrapWithDurability(
      req,
      id,
      binaryDecrementHandler,
      options.durability,
      false,
      timeoutActual
    )
  }
}
