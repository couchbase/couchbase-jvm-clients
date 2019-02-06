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

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.document._
import com.couchbase.client.scala.durability.{Disabled, Durability}
import io.opentracing.Span
//import com.couchbase.client.scala.query.N1qlQueryResult

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.reflect.runtime.universe._

class Collection(val async: AsyncCollection,
                 bucketName: String)
                (implicit ec: ExecutionContext) {
  private val config: CouchbaseEnvironment = null // scope.cluster.env
  //  private val asyncCollection = new AsyncCollection(this)
  //  private val reactiveColl = new ReactiveCollection(this)
  // TODO binary collection
  // TODO MVP reactive collection
  private val SafetyTimeout = 1.second
  //  val kvTimeout = FiniteDuration(config.kvTimeout(), TimeUnit.MILLISECONDS)
  private[scala] val kvTimeout = FiniteDuration(2500, TimeUnit.MILLISECONDS)

  private def block[T](in: Future[T], timeout: FiniteDuration): Try[T] = {
    try {
      Try(Await.result(in, timeout + SafetyTimeout))
    }
    catch {
      case NonFatal(err) => Failure(err)
    }
  }

  def exists[T](id: String,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = async.environment.retryStrategy()
               )
  : Try[ExistsResult] = {
    block(async.exists(id, parentSpan, timeout, retryStrategy), timeout)
  }

  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: FiniteDuration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = async.environment.retryStrategy(),
               )
               (implicit ev: Conversions.Encodable[T])
  : Try[MutationResult] = {
    block(async.insert(id, content, durability, expiration, parentSpan, timeout, retryStrategy), timeout)
  }

  def replace[T](id: String,
                 content: T,
                 cas: Long = 0,
                 durability: Durability = Disabled,
                 expiration: FiniteDuration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: FiniteDuration = kvTimeout,
                 retryStrategy: RetryStrategy = async.environment.retryStrategy()
                )
                (implicit ev: Conversions.Encodable[T]): Try[MutationResult] = {
    block(async.replace(id, content, cas, durability, expiration, parentSpan, timeout, retryStrategy), timeout)
  }

  def upsert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: FiniteDuration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: FiniteDuration = kvTimeout,
                retryStrategy: RetryStrategy = async.environment.retryStrategy()
               )
               (implicit ev: Conversions.Encodable[T]): Try[MutationResult] = {
    block(async.upsert(id, content, durability, expiration, parentSpan, timeout, retryStrategy), timeout)
  }

  def remove(id: String,
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: FiniteDuration = kvTimeout,
             retryStrategy: RetryStrategy = async.environment.retryStrategy()
            ): Try[MutationResult] = {
    block(async.remove(id, cas, durability, parentSpan, timeout, retryStrategy), timeout)
  }

  def mutateIn(id: String,
               // TODO PENDING this may change to a list
               spec: MutateInSpec,
               cas: Long = 0,
               insertDocument: Boolean = false,
               durability: Durability = Disabled,
               parentSpan: Option[Span] = None,
               expiration: FiniteDuration = 0.seconds,
               timeout: FiniteDuration = kvTimeout,
               retryStrategy: RetryStrategy = async.environment.retryStrategy()
              ): Try[MutateInResult] = {
    block(async.mutateIn(id, spec, cas, insertDocument, durability, parentSpan, expiration, timeout, retryStrategy), timeout)
  }


  def getAndLock(id: String,
          lockFor: FiniteDuration = 30.seconds,
          parentSpan: Option[Span] = None,
          timeout: FiniteDuration = kvTimeout,
          retryStrategy: RetryStrategy = async.environment.retryStrategy()
         ): Try[GetResult] = {
    block(async.getAndLock(id, lockFor, parentSpan, timeout, retryStrategy), timeout)
  }

  def getAndTouch(id: String,
                  expiration: FiniteDuration,
                 parentSpan: Option[Span] = None,
                  durability: Durability = Disabled,
                 timeout: FiniteDuration = kvTimeout,
                 retryStrategy: RetryStrategy = async.environment.retryStrategy()
                ): Try[GetResult] = {
    block(async.getAndTouch(id, expiration, parentSpan, durability, timeout, retryStrategy), timeout)
  }

  def get(id: String,
          withExpiration: Boolean = false,
          parentSpan: Option[Span] = None,
          timeout: FiniteDuration = kvTimeout,
          retryStrategy: RetryStrategy = async.environment.retryStrategy()
         ): Try[GetResult] = {
    block(async.get(id, withExpiration, parentSpan, timeout, retryStrategy), timeout)
  }


  def lookupIn(id: String,
               spec: LookupInSpec,
               parentSpan: Option[Span] = None,
               timeout: FiniteDuration = kvTimeout,
               retryStrategy: RetryStrategy = async.environment.retryStrategy()
         ): Try[LookupInResult] = {
    block(async.lookupIn(id, spec, parentSpan, timeout, retryStrategy), timeout)
  }

}
