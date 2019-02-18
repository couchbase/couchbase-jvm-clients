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

import java.nio.charset.Charset
import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

import com.couchbase.client.core.Core
import com.couchbase.client.core.error._
import com.couchbase.client.core.error.subdoc.SubDocumentException
import com.couchbase.client.core.msg.{Request, Response, ResponseStatus}
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.{BestEffortRetryStrategy, RetryStrategy}
import com.couchbase.client.core.service.kv.{Observe, ObserveContext}
import com.couchbase.client.core.util.{UnsignedLEB128, Validators}
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import scala.compat.java8.FunctionConverters._
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.document.{LookupInResult, _}
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.FutureConversions
import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.buffer.ByteBuf
import io.netty.util.CharsetUtil
import io.opentracing.Span
import reactor.core.scala.publisher.Mono
import rx.RxReactiveStreams

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future, JavaConversions}
import scala.concurrent.duration.{Duration, _}
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._
import ujson._
import com.couchbase.client.scala.durability.Durability._

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.compat.java8.OptionConverters._
import collection.JavaConverters._

case class HandlerParams(core: Core, bucketName: String, collectionIdEncoded: Array[Byte])


sealed trait ReplicaMode

object ReplicaMode {

  case object All extends ReplicaMode

  case object Any extends ReplicaMode

  case object First extends ReplicaMode

  case object Second extends ReplicaMode

  case object Third extends ReplicaMode

}

object AsyncCollection {
  private[scala] val getFullDoc = Array(LookupInSpec.getDoc)

  private def wrap[Resp <: Response, Res](in: Try[Request[Resp]],
                                          id: String,
                                          handler: RequestHandler[Resp, Res],
                                          core: Core)
                                         (implicit ec: ExecutionContext): Future[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        val out = FutureConverters.toScala(request.response())
          .map(response => handler.response(id, response))

        out

      case Failure(err) => Future.failed(err)
    }
  }

  private def wrapWithDurability[Resp <: Response, Res <: HasDurabilityTokens](in: Try[Request[Resp]],
                                                                               id: String,
                                                                               handler: RequestHandler[Resp, Res],
                                                                               durability: Durability,
                                                                               remove: Boolean,
                                                                               timeout: java.time.Duration,
                                                                               core: Core,
                                                                               bucketName: String,
                                                                               collectionIdEncoded: Array[Byte])
                                                                              (implicit ec: ExecutionContext): Future[Res] = {
    val initial: Future[Res] = wrap(in, id, handler, core)

    durability match {
      case ClientVerified(replicateTo, persistTo) =>
        initial.flatMap(response => {

          val observeCtx = new ObserveContext(core.context(),
            PersistTo.asCore(persistTo),
            ReplicateTo.asCore(replicateTo),
            response.mutationToken.asJava,
            response.cas,
            bucketName,
            id,
            collectionIdEncoded,
            remove,
            timeout
          )

          FutureConversions.javaMonoToScalaFuture(Observe.poll(observeCtx))

            // After the observe return the original response
            .map(_ => response)
        })

      case _ => initial
    }
  }
}

class AsyncCollection(name: String,
                      collectionId: Long,
                      bucketName: String,
                      val core: Core,
                      val environment: ClusterEnvironment)
                     (implicit ec: ExecutionContext) {

  import DurationConversions._

  private[scala] val kvTimeout = javaDurationToScala(environment.timeoutConfig().kvTimeout())
  private[scala] val collectionIdEncoded = UnsignedLEB128.encode(collectionId)
  private[scala] val hp = HandlerParams(core, bucketName, collectionIdEncoded)
  private[scala] val existsHandler = new ExistsHandler(hp)
  private[scala] val insertHandler = new InsertHandler(hp)
  private[scala] val replaceHandler = new ReplaceHandler(hp)
  private[scala] val upsertHandler = new UpsertHandler(hp)
  private[scala] val removeHandler = new RemoveHandler(hp)
  private[scala] val getFullDocHandler = new GetFullDocHandler(hp)
  private[scala] val getSubDocHandler = new GetSubDocHandler(hp)
  private[scala] val getAndTouchHandler = new GetAndTouchHandler(hp)
  private[scala] val getAndLockHandler = new GetAndLockHandler(hp)
  private[scala] val mutateInHandler = new MutateInHandler(hp)
  private[scala] val unlockHandler = new UnlockHandler(hp)
  private[scala] val getFromReplicaHandler = new GetFromReplicaHandler(hp)

  val binary = new AsyncBinaryCollection(this)

  private[scala] def wrap[Resp <: Response, Res](in: Try[Request[Resp]],
                                          id: String,
                                          handler: RequestHandler[Resp, Res]): Future[Res] = {
    AsyncCollection.wrap(in, id, handler, core)
  }

  private[scala] def wrapWithDurability[Resp <: Response, Res <: HasDurabilityTokens](in: Try[Request[Resp]],
                                                                               id: String,
                                                                               handler: RequestHandler[Resp, Res],
                                                                               durability: Durability,
                                                                               remove: Boolean,
                                                                               timeout: java.time.Duration): Future[Res] = {
    AsyncCollection.wrapWithDurability(in, id, handler, durability, remove, timeout, core, bucketName, collectionIdEncoded)
  }

  def exists(id: String,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()): Future[ExistsResult] = {
    val req = existsHandler.request(id, parentSpan, timeout, retryStrategy)
    wrap(req, id, existsHandler)
  }

  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy())
               (implicit ev: Conversions.Encodable[T])
  : Future[MutationResult] = {
    val req = insertHandler.request(id, content, durability, expiration, parentSpan, timeout, retryStrategy)
    wrapWithDurability(req, id, insertHandler, durability, false, timeout)
  }

  def replace[T](id: String,
                 content: T,
                 cas: Long = 0,
                 durability: Durability = Disabled,
                 expiration: Duration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy())
                (implicit ev: Conversions.Encodable[T]): Future[MutationResult] = {
    val req = replaceHandler.request(id, content, cas, durability, expiration, parentSpan, timeout, retryStrategy)
    wrapWithDurability(req, id, replaceHandler, durability, false, timeout)
  }

  def upsert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy())
               (implicit ev: Conversions.Encodable[T]): Future[MutationResult] = {
    val req = upsertHandler.request(id, content, durability, expiration, parentSpan, timeout, retryStrategy)
    wrapWithDurability(req, id, upsertHandler, durability, false, timeout)
  }


  def remove(id: String,
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()): Future[MutationResult] = {
    val req = removeHandler.request(id, cas, durability, parentSpan, timeout, retryStrategy)
    wrapWithDurability(req, id, removeHandler, durability, true, timeout)
  }

  def get(id: String,
          withExpiration: Boolean = false,
          parentSpan: Option[Span] = None,
          timeout: Duration = kvTimeout,
          retryStrategy: RetryStrategy = environment.retryStrategy())
  : Future[GetResult] = {
    if (withExpiration) {
      getSubDoc(id, AsyncCollection.getFullDoc, withExpiration, parentSpan, timeout, retryStrategy).map(lookupInResult =>
        GetResult(id, lookupInResult.contentAsBytes(0).get, lookupInResult.flags, lookupInResult.cas, lookupInResult.expiration))
    }
    else {
      getFullDoc(id, parentSpan, timeout, retryStrategy)
    }
  }

  private def getFullDoc(id: String,
                         parentSpan: Option[Span] = None,
                         timeout: Duration = kvTimeout,
                         retryStrategy: RetryStrategy = environment.retryStrategy()): Future[GetResult] = {
    val req = getFullDocHandler.request(id, parentSpan, timeout, retryStrategy)
    wrap(req, id, getFullDocHandler)
  }


  private def getSubDoc(id: String,
                        spec: Seq[LookupInSpec],
                        withExpiration: Boolean,
                        parentSpan: Option[Span] = None,
                        timeout: Duration = kvTimeout,
                        retryStrategy: RetryStrategy = environment.retryStrategy()): Future[LookupInResult] = {
    val req = getSubDocHandler.request(id, spec, withExpiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, getSubDocHandler)
  }

  def mutateIn(id: String,
               spec: Seq[MutateInSpec],
               cas: Long = 0,
               insertDocument: Boolean = false,
               durability: Durability = Disabled,
               parentSpan: Option[Span] = None,
               expiration: Duration = 0.seconds,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()): Future[MutateInResult] = {
    val req = mutateInHandler.request(id, spec, cas, insertDocument, durability, expiration, parentSpan, timeout, retryStrategy)
    wrapWithDurability(req, id, mutateInHandler, durability, false, timeout)
  }

  def getAndLock(id: String,
                 expiration: Duration = 30.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy()
                ): Future[GetResult] = {
    val req = getAndLockHandler.request(id, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, getAndLockHandler)
  }

  def unlock(id: String,
             cas: Long,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()
            ): Future[Unit] = {
    val req = unlockHandler.request(id, cas, parentSpan, timeout, retryStrategy)
    wrap(req, id, unlockHandler)
  }

  def getAndTouch(id: String,
                  expiration: Duration,
                  durability: Durability = Disabled,
                  parentSpan: Option[Span] = None,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = environment.retryStrategy()
                 ): Future[GetResult] = {
    val req = getAndTouchHandler.request(id, expiration, durability, parentSpan, timeout, retryStrategy)
    wrap(req, id, getAndTouchHandler)
  }

  def lookupIn(id: String,
               spec: Seq[LookupInSpec],
               parentSpan: Option[Span] = None,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()
              ): Future[LookupInResult] = {
    // Set withExpiration to false as it makes all subdoc lookups multi operations, which changes semantics - app
    // may expect error to be raised and it won't
    getSubDoc(id, spec, withExpiration = false, parentSpan, timeout, retryStrategy)
  }

  def getFromReplica(id: String,
                     replicaMode: ReplicaMode,
                     parentSpan: Option[Span] = None,
                     timeout: Duration = kvTimeout,
                     retryStrategy: RetryStrategy = environment.retryStrategy()
                    ): Seq[Future[GetResult]] = {
    val reqsTry: Try[Seq[GetRequest]] = getFromReplicaHandler.request(id, replicaMode, parentSpan, timeout, retryStrategy)

    reqsTry match {
      case Failure(err) => Seq(Future.failed(err))

      case Success(reqs: Seq[GetRequest]) =>
        val out = reqs.map(request => {
          core.send(request)

          FutureConverters.toScala(request.response())
            .map(response => {
              getFullDocHandler.response(id, response)
            })
        })

        replicaMode match {
            // The main benefit of Any is to avoid blocking for all results when one will do
          case ReplicaMode.Any => out.take(1)
          case _ => out
        }

    }
  }
}

