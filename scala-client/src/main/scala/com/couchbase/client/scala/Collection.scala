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
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv._
import io.opentracing.Span
import reactor.core.scala.publisher.Mono
//import com.couchbase.client.scala.query.N1qlQueryResult

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.reflect.runtime.universe._

object Collection {
  private[scala] val SafetyTimeout = 1.second

  private[scala] def block[T](in: Future[T], timeout: Duration): Try[T] = {
    try {
      Try(Await.result(in, timeout + SafetyTimeout))
    }
    catch {
      case NonFatal(err) => Failure(err)
    }
  }

}

class Collection(val async: AsyncCollection,
                 bucketName: String)
                (implicit ec: ExecutionContext) {
  val reactive = new ReactiveCollection(async)
  val binary = new BinaryCollection(async.binary)
  private[scala] val kvTimeout = Duration(2500, TimeUnit.MILLISECONDS)

  private def block[T](in: Future[T], timeout: Duration): Try[T] = {
    Collection.block(in, timeout)
  }

  def exists[T](id: String,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = async.environment.retryStrategy()
               )
  : Try[ExistsResult] = {
    block(async.exists(id, parentSpan, timeout, retryStrategy), timeout)
  }

  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
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
                 expiration: Duration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = async.environment.retryStrategy()
                )
                (implicit ev: Conversions.Encodable[T]): Try[MutationResult] = {
    block(async.replace(id, content, cas, durability, expiration, parentSpan, timeout, retryStrategy), timeout)
  }

  def upsert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = async.environment.retryStrategy()
               )
               (implicit ev: Conversions.Encodable[T]): Try[MutationResult] = {
    block(async.upsert(id, content, durability, expiration, parentSpan, timeout, retryStrategy), timeout)
  }

  def remove(id: String,
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = async.environment.retryStrategy()
            ): Try[MutationResult] = {
    block(async.remove(id, cas, durability, parentSpan, timeout, retryStrategy), timeout)
  }

  def mutateIn(id: String,
               spec: Seq[MutateInSpec],
               cas: Long = 0,
               document: Document = Document.DoNothing,
               durability: Durability = Disabled,
               parentSpan: Option[Span] = None,
               expiration: Duration = 0.seconds,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = async.environment.retryStrategy()
              ): Try[MutateInResult] = {
    block(async.mutateIn(id, spec, cas, document, durability, parentSpan, expiration, timeout, retryStrategy), timeout)
  }


  def getAndLock(id: String,
                 lockFor: Duration = 30.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = async.environment.retryStrategy()
                ): Try[GetResult] = {
    block(async.getAndLock(id, lockFor, parentSpan, timeout, retryStrategy), timeout)
  }

  def unlock(id: String,
             cas: Long,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = async.environment.retryStrategy()
            ): Try[Unit] = {
    block(async.unlock(id, cas, parentSpan, timeout, retryStrategy), timeout)
  }

  def getAndTouch(id: String,
                  expiration: Duration,
                  durability: Durability = Disabled,
                  parentSpan: Option[Span] = None,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = async.environment.retryStrategy()
                 ): Try[GetResult] = {
    block(async.getAndTouch(id, expiration, durability, parentSpan, timeout, retryStrategy), timeout)
  }

  def get(id: String,
          withExpiration: Boolean = false,
          project: Seq[String] = Seq.empty,
          parentSpan: Option[Span] = None,
          timeout: Duration = kvTimeout,
          retryStrategy: RetryStrategy = async.environment.retryStrategy()
         ): Try[GetResult] = {
    block(async.get(id, withExpiration, project, parentSpan, timeout, retryStrategy), timeout)
  }


  def lookupIn(id: String,
               spec: Seq[LookupInSpec],
               parentSpan: Option[Span] = None,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = async.environment.retryStrategy()
              ): Try[LookupInResult] = {
    block(async.lookupIn(id, spec, parentSpan, timeout, retryStrategy), timeout)
  }

  def getAnyReplica(id: String,
                     parentSpan: Option[Span] = None,
                     timeout: Duration = kvTimeout,
                     retryStrategy: RetryStrategy = async.environment.retryStrategy()
                    ): GetResult = {
    reactive.getAnyReplica(id,parentSpan, timeout, retryStrategy).block(Collection.SafetyTimeout + timeout)
  }

  def getAllReplicas(id: String,
                     parentSpan: Option[Span] = None,
                     timeout: Duration = kvTimeout,
                     retryStrategy: RetryStrategy = async.environment.retryStrategy()
                    ): Iterable[GetResult] = {
    reactive.getAllReplicas(id,parentSpan, timeout, retryStrategy).toIterable()
  }
}
