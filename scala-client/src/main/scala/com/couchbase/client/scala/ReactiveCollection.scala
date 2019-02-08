/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import java.util.Optional
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.Reactor
import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.document.GetResult
import com.couchbase.client.scala.durability.{Disabled, Durability}
import com.couchbase.client.scala.kv.RequestHandler
import com.couchbase.client.scala.util.FutureConversions
import io.opentracing.Span
import reactor.core.scala.publisher.Mono

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class ReactiveCollection(async: AsyncCollection) {
  private val kvTimeout = async.kvTimeout
  private val environment = async.environment
  private val core = async.core

  implicit def scalaDurationToJava(in: scala.concurrent.duration.FiniteDuration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def javaDurationToScala(in: java.time.Duration): scala.concurrent.duration.FiniteDuration = {
    FiniteDuration.apply(in.toNanos, TimeUnit.NANOSECONDS)
  }

  private def wrap[Resp,Res](in: Try[Request[Resp]], handler: RequestHandler[Resp,Res]): Mono[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => handler.response(r))

      case Failure(err) => Mono.error(err)
    }
  }

  def exists[T](id: String,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[ExistsResult] = {
    val request = async.existsHandler.request(id, parentSpan, timeout, retryStrategy)
    wrap(request, async.existsHandler)
  }

  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: FiniteDuration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy())
               (implicit ev: Conversions.Encodable[T])
  : Future[MutationResult] = {
    val req = async.insertHandler.request(id, content, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, async.insertHandler)
  }

}
