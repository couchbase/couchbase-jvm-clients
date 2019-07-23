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

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api.{CounterResult, MutationResult}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv.handlers.{BinaryAppendHandler, BinaryDecrementHandler, BinaryIncrementHandler,
  BinaryPrependHandler}

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}

/** Operations on non-JSON Couchbase documents.
  *
  * This is an asynchronous version of the [[BinaryCollection]] API.  See also [[ReactiveBinaryCollection]].
  *
  * @param ec    an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *              opened in the normal way, starting from functions in [[Cluster]]
  *
  * @define Same             This asynchronous version performs the same functionality and takes the same parameters,
  *                          but returns the same result object asynchronously in a `Future`.
  * @author Graham Pople
  * @since 1.0.0
  */
class AsyncBinaryCollection(private[scala] val async: AsyncCollection) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  import com.couchbase.client.scala.util.DurationConversions._

  private[scala] val environment = async.environment
  private[scala] val kvTimeout = async.kvTimeout
  private[scala] val binaryAppendHandler = new BinaryAppendHandler(async.hp)
  private[scala] val binaryPrependHandler = new BinaryPrependHandler(async.hp)
  private[scala] val binaryIncrementHandler = new BinaryIncrementHandler(async.hp)
  private[scala] val binaryDecrementHandler = new BinaryDecrementHandler(async.hp)

  /** Add bytes to the end of a Couchbase binary document.
    *
    * See [[BinaryCollection.append]] for details.  $Same
    */
  def append(id: String,
             content: Array[Byte],
             cas: Long = 0,
             durability: Durability = Disabled,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy): Future[MutationResult] = {
    val req = binaryAppendHandler.request(id, content, cas, durability, timeout, retryStrategy)
    async.wrapWithDurability(req, id, binaryAppendHandler, durability, false, timeout)
  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * See [[BinaryCollection.prepend]] for details.  $Same
    * */
  def prepend(id: String,
              content: Array[Byte],
              cas: Long = 0,
              durability: Durability = Disabled,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = environment.retryStrategy): Future[MutationResult] = {
    val req = binaryPrependHandler.request(id, content, cas, durability, timeout, retryStrategy)
    async.wrapWithDurability(req, id, binaryPrependHandler, durability, false, timeout)
  }

  /** Increment a Couchbase 'counter' document.
    *
    * See [[BinaryCollection.increment]] for details.  $Same
    * */
  def increment(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy): Future[CounterResult] = {
    val req = binaryIncrementHandler.request(id, delta, initial, cas, durability, expiration, timeout,
      retryStrategy)
    async.wrapWithDurability(req, id, binaryIncrementHandler, durability, false, timeout)
  }

  /** Decrement a Couchbase 'counter' document.
    *
    * See [[BinaryCollection.increment]] for details.  $Same
    * */
  def decrement(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy): Future[CounterResult] = {
    val req = binaryDecrementHandler.request(id, delta, initial, cas, durability, expiration, timeout,
      retryStrategy)
    async.wrapWithDurability(req, id, binaryDecrementHandler, durability, false, timeout)
  }
}
