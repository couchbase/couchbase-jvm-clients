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
import com.couchbase.client.core.msg.{Request, Response}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.document.{GetResult, LookupInResult, MutateInResult}
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

  private def wrap[Resp <: Response,Res](in: Try[Request[Resp]], id: String, handler: RequestHandler[Resp,Res]): Mono[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => handler.response(id, r))

      case Failure(err) => Mono.error(err)
    }
  }

  def exists[T](id: String,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[ExistsResult] = {
    val req = async.existsHandler.request(id, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.existsHandler)
  }

  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: FiniteDuration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy())
               (implicit ev: Conversions.Encodable[T]): Mono[MutationResult] = {
    val req = async.insertHandler.request(id, content, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.insertHandler)
  }


  def replace[T](id: String,
                 content: T,
                 cas: Long = 0,
                 durability: Durability = Disabled,
                 expiration: FiniteDuration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: FiniteDuration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy())
                (implicit ev: Conversions.Encodable[T]): Mono[MutationResult] = {
    val req = async.replaceHandler.request(id, content, cas, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.replaceHandler)
  }

  def upsert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: FiniteDuration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy())
               (implicit ev: Conversions.Encodable[T]): Mono[MutationResult] = {
    val req = async.upsertHandler.request(id, content, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.upsertHandler)
  }


  def remove(id: String,
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: FiniteDuration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[MutationResult] = {
    val req = async.removeHandler.request(id, cas, durability, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.removeHandler)
  }

  def get(id: String,
          withExpiration: Boolean = false,
          parentSpan: Option[Span] = None,
          timeout: FiniteDuration = kvTimeout,
          retryStrategy: RetryStrategy = environment.retryStrategy())
  : Mono[GetResult] = {
    if (withExpiration) {
      getSubDoc(id, LookupInSpec.getDoc, withExpiration, parentSpan, timeout, retryStrategy).map(lookupInResult =>
        GetResult(id, lookupInResult.documentAsBytes.get, lookupInResult.flags, lookupInResult.cas, lookupInResult.expiration))
    }
    else {
      getFullDoc(id, parentSpan, timeout, retryStrategy)
    }
  }

  private def getFullDoc(id: String,
                         parentSpan: Option[Span] = None,
                         timeout: FiniteDuration = kvTimeout,
                         retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[GetResult] = {
    val req = async.getFullDocHandler.request(id, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.getFullDocHandler)
  }


  private def getSubDoc(id: String,
                        spec: LookupInSpec,
                        withExpiration: Boolean,
                        parentSpan: Option[Span] = None,
                        timeout: FiniteDuration = kvTimeout,
                        retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[LookupInResult] = {
    val req = async.getSubDocHandler.request(id, spec, withExpiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.getSubDocHandler)
  }
  
  def mutateIn(id: String,
               spec: MutateInSpec,
               cas: Long = 0,
               insertDocument: Boolean = false,
               durability: Durability = Disabled,
               parentSpan: Option[Span] = None,
               expiration: FiniteDuration,
               timeout: FiniteDuration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[MutateInResult] = {
    val req = async.mutateInHandler.request(id, spec, cas, insertDocument, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.mutateInHandler)
  }

  def getAndLock(id: String,
                 expiration: FiniteDuration = 30.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: FiniteDuration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy()
                ): Mono[GetResult] = {
    val req = async.getAndLockHandler.request(id, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.getAndLockHandler)
  }

  def getAndTouch(id: String,
                  expiration: FiniteDuration,
                  durability: Durability = Disabled,
                  parentSpan: Option[Span] = None,
                  timeout: FiniteDuration = kvTimeout,
                  retryStrategy: RetryStrategy = environment.retryStrategy()
                 ): Mono[GetResult] = {
    val req = async.getAndTouchHandler.request(id, expiration, durability, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.getAndTouchHandler)
  }


  def lookupIn(id: String,
               spec: LookupInSpec,
               parentSpan: Option[Span] = None,
               timeout: FiniteDuration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()
              ): Mono[LookupInResult] = {
    // Set withExpiration to false as it makes all subdoc lookups multi operations, which changes semantics - app
    // may expect error to be raised and it won't
    getSubDoc(id, spec, withExpiration = false, parentSpan, timeout, retryStrategy)
  }

}
