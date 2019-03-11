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

import com.couchbase.client.core.msg.{Request, Response}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api.{CounterResult, MutationResult}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.FutureConversions
import io.opentracing.Span
import reactor.core.scala.publisher.Mono
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv.handlers.RequestHandler

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class ReactiveBinaryCollection(private val async: AsyncBinaryCollection)
                              (implicit ec: ExecutionContext) {

  import DurationConversions._

  private[scala] val kvTimeout = async.kvTimeout
  private[scala] val environment = async.environment

  private def wrap[Resp <: Response,Res](in: Try[Request[Resp]], id: String, handler: RequestHandler[Resp,Res]): Mono[Res] = {
    in match {
      case Success(request) =>
        async.async.core.send[Resp](request)

        FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => handler.response(id, r))

      case Failure(err) => Mono.error(err)
    }
  }

  def append(id: String,
             content: Array[Byte],
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()): Future[MutationResult] = {
    val req = async.binaryAppendHandler.request(id, content, cas, durability, parentSpan, timeout, retryStrategy)
    async.async.wrapWithDurability(req, id, async.binaryAppendHandler, durability, false, timeout)
  }

  def prepend(id: String,
              content: Array[Byte],
              cas: Long = 0,
              durability: Durability = Disabled,
              parentSpan: Option[Span] = None,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = environment.retryStrategy()): Future[MutationResult] = {
    val req = async.binaryPrependHandler.request(id, content, cas, durability, parentSpan, timeout, retryStrategy)
    async.async.wrapWithDurability(req, id, async.binaryPrependHandler, durability, false, timeout)
  }

  def increment(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Future[CounterResult] = {
    val req = async.binaryIncrementHandler.request(id, delta, initial, cas, durability, expiration, parentSpan, timeout, retryStrategy)
    async.async.wrapWithDurability(req, id, async.binaryIncrementHandler, durability, false, timeout)
  }

  def decrement(id: String,
                delta: Long,
                initial: Option[Long] = None,
                cas: Long = 0,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Future[CounterResult] = {
    val req = async.binaryDecrementHandler.request(id, delta, initial, cas, durability, expiration, parentSpan, timeout, retryStrategy)
    async.async.wrapWithDurability(req, id, async.binaryDecrementHandler, durability, false, timeout)
  }
}
