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

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.msg.kv.KeyValueRequest
import com.couchbase.client.core.msg.{Request, Response}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api.{CounterResult, MutationResult}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv.handlers.KeyValueRequestHandler
import com.couchbase.client.scala.util.{FutureConversions, TimeoutUtil}
import reactor.core.scala.publisher.SMono

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Operations on non-JSON Couchbase documents.
  *
  * This is a reactive version of the [[BinaryCollection]] API.  See also [[AsyncBinaryCollection]].
  *
  * @param ec    an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *              opened in the normal way, starting from functions in [[Cluster]]
  *
  * @define Same             This reactive version performs the same functionality and takes the same parameters,
  *                          but returns the same result object asynchronously in a `SMono`.
  * @author Graham Pople
  * @since 1.0.0
  */
class ReactiveBinaryCollection(private val async: AsyncBinaryCollection) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  import com.couchbase.client.scala.util.DurationConversions._

  private[scala] val kvTimeout: Durability => Duration = TimeoutUtil.kvTimeout(async.environment)
  private[scala] val environment                       = async.environment

  private def wrap[Resp <: Response, Res](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandler[Resp, Res]
  ): SMono[Res] = {
    in match {
      case Success(request) =>
        SMono.defer(() => {
          async.async.core.send[Resp](request)

          FutureConversions
            .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
            .map(r => handler.response(request, id, r))
        })

      case Failure(err) => SMono.raiseError(err)
    }
  }

  /** Add bytes to the end of a Couchbase binary document.
    *
    * See [[BinaryCollection.append]] for details.  $Same
    */
  def append(
      id: String,
      content: Array[Byte],
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[MutationResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req =
      async.binaryAppendHandler.request(
        id,
        content,
        cas,
        durability,
        timeoutActual,
        retryStrategy,
        parentSpan
      )
    wrap(req, id, async.binaryAppendHandler)
  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * See [[BinaryCollection.prepend]] for details.  $Same
    * */
  def prepend(
      id: String,
      content: Array[Byte],
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[MutationResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req =
      async.binaryPrependHandler.request(
        id,
        content,
        cas,
        durability,
        timeoutActual,
        retryStrategy,
        parentSpan
      )
    wrap(req, id, async.binaryPrependHandler)
  }

  /** Increment a Couchbase 'counter' document.
    *
    * See [[BinaryCollection.increment]] for details.  $Same
    * */
  def increment(
      id: String,
      delta: Long,
      initial: Option[Long] = None,
      cas: Long = 0,
      durability: Durability = Disabled,
      expiry: Duration = 0.seconds,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[CounterResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req = async.binaryIncrementHandler.request(
      id,
      delta,
      initial,
      cas,
      durability,
      expiry,
      timeoutActual,
      retryStrategy,
      parentSpan
    )
    wrap(req, id, async.binaryIncrementHandler)
  }

  /** Decrement a Couchbase 'counter' document.
    *
    * See [[BinaryCollection.increment]] for details.  $Same
    * */
  def decrement(
      id: String,
      delta: Long,
      initial: Option[Long] = None,
      cas: Long = 0,
      durability: Durability = Disabled,
      expiry: Duration = 0.seconds,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[CounterResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req = async.binaryDecrementHandler.request(
      id,
      delta,
      initial,
      cas,
      durability,
      expiry,
      timeoutActual,
      retryStrategy,
      parentSpan
    )
    wrap(req, id, async.binaryDecrementHandler)
  }
}
