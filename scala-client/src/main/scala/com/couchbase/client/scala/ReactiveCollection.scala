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

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.msg.kv.{GetRequest, KeyValueRequest}
import com.couchbase.client.core.msg.{Request, Response}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.{Conversions, JsonSerializer, JsonTranscoder, Transcoder}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.kv.handlers.{
  KeyValueRequestHandler,
  KeyValueRequestHandlerWithTranscoder
}
import com.couchbase.client.scala.util.{FutureConversions, TimeoutUtil}
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** Provides asynchronous access to all collection APIs, based around reactive programming using the
  * [[https://projectreactor.io/ Project Reactor]] library.  This is the main entry-point
  * for key-value (KV) operations.
  *
  * <p>If synchronous, blocking access is needed, we recommend looking at the [[Collection]].  If a simpler
  * async API based around Scala `Future`s is desired, then check out the [[AsyncCollection]].
  *
  * @author Graham Pople
  * @since 1.0.0
  * @define Same             This reactive programming version performs the same functionality and takes the same
  *                          parameters,
  *                          but returns the same result object asynchronously in a Project Reactor `SMono`.
  * */
class ReactiveCollection(async: AsyncCollection) {
  private[scala] val kvTimeout: Durability => Duration = TimeoutUtil.kvTimeout(async.environment)
  private[scala] val kvReadTimeout: Duration           = async.kvReadTimeout
  private val environment                              = async.environment
  private val core                                     = async.core

  import com.couchbase.client.scala.util.DurationConversions._

  def name: String = async.name

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * See [[com.couchbase.client.scala.Collection.insert]] for details.  $Same */
  def insert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      expiry: Duration = 0.seconds,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  )(implicit serializer: JsonSerializer[T]): SMono[MutationResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req = async.insertHandler.request(
      id,
      content,
      durability,
      expiry,
      timeoutActual,
      retryStrategy,
      transcoder,
      serializer
    )
    wrap(req, id, async.insertHandler)
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * See [[com.couchbase.client.scala.Collection.replace]] for details.  $Same */
  def replace[T](
      id: String,
      content: T,
      cas: Long = 0,
      durability: Durability = Disabled,
      expiry: Duration = 0.seconds,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  )(implicit serializer: JsonSerializer[T]): SMono[MutationResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req = async.replaceHandler.request(
      id,
      content,
      cas,
      durability,
      expiry,
      timeoutActual,
      retryStrategy,
      transcoder,
      serializer
    )
    wrap(req, id, async.replaceHandler)
  }

  /** Upserts the contents of a full document in this collection.
    *
    * See [[com.couchbase.client.scala.Collection.upsert]] for details.  $Same */
  def upsert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      expiry: Duration = 0.seconds,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance,
      parentSpan: Option[RequestSpan] = None
  )(implicit serializer: JsonSerializer[T]): SMono[MutationResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req = async.upsertHandler.request(
      id,
      content,
      durability,
      expiry,
      timeoutActual,
      retryStrategy,
      transcoder,
      serializer,
      parentSpan
    )
    wrap(req, id, async.upsertHandler)
  }

  /** Removes a document from this collection, if it exists.
    *
    * See [[com.couchbase.client.scala.Collection.remove]] for details.  $Same */
  def remove(
      id: String,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy
  ): SMono[MutationResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req           = async.removeHandler.request(id, cas, durability, timeoutActual, retryStrategy)
    wrap(req, id, async.removeHandler)
  }

  /** Fetches a full document from this collection.
    *
    * See [[com.couchbase.client.scala.Collection.get]] for details.  $Same */
  def get(
      id: String,
      withExpiry: Boolean = false,
      project: Seq[String] = Seq.empty,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  ): SMono[GetResult] = {

    // Implementation note: Option is returned because SMono.empty is hard to work with.  See JCBC-1310.

    if (project.nonEmpty) {
      async.getSubDocHandler.requestProject(id, project, timeout, retryStrategy) match {
        case Success(request) =>
          core.send(request)

          FutureConversions
            .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
            .map(
              r =>
                async.getSubDocHandler.responseProject(request, id, r, transcoder) match {
                  case Success(v)   => v
                  case Failure(err) => throw err
                }
            )

        case Failure(err) => SMono.raiseError(err)
      }

    } else if (withExpiry) {
      getSubDoc(id, AsyncCollection.getFullDoc, withExpiry, timeout, retryStrategy, transcoder)
        .map(
          v =>
            GetResult(
              id,
              Left(v.contentAs[Array[Byte]](0).get),
              v.flags,
              v.cas,
              v.expiry,
              transcoder
            )
        )
    } else {
      getFullDoc(id, timeout, retryStrategy, transcoder)
    }
  }

  private def getFullDoc(
      id: String,
      timeout: Duration,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder
  ): SMono[GetResult] = {
    val req = async.getFullDocHandler.request(id, timeout, retryStrategy)
    wrap(req, id, async.getFullDocHandler, transcoder)
  }

  private def getSubDoc(
      id: String,
      spec: Seq[LookupInSpec],
      withExpiry: Boolean,
      timeout: Duration,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder
  ): SMono[LookupInResult] = {
    async.getSubDocHandler.request(id, spec, withExpiry, timeout, retryStrategy) match {
      case Success(request) =>
        core.send(request)

        FutureConversions
          .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => async.getSubDocHandler.response(request, id, r, withExpiry, transcoder))

      case Failure(err) => SMono.raiseError(err)
    }
  }

  /** SubDocument mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * See [[com.couchbase.client.scala.Collection.mutateIn]] for details.  $Same */
  def mutateIn(
      id: String,
      spec: Seq[MutateInSpec],
      cas: Long = 0,
      document: StoreSemantics = StoreSemantics.Replace,
      durability: Durability = Disabled,
      expiry: Duration,
      timeout: Duration = Duration.MinusInf,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance,
      @Stability.Internal accessDeleted: Boolean = false
  ): SMono[MutateInResult] = {
    val timeoutActual = if (timeout == Duration.MinusInf) kvTimeout(durability) else timeout
    val req = async.mutateInHandler.request(
      id,
      spec,
      cas,
      document,
      durability,
      expiry,
      timeoutActual,
      retryStrategy,
      accessDeleted,
      transcoder
    )
    req match {
      case Success(request) =>
        core.send(request)

        FutureConversions
          .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => async.mutateInHandler.response(request, id, document, r))

      case Failure(err) => SMono.raiseError(err)
    }
  }

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * See [[com.couchbase.client.scala.Collection.getAndLock]] for details.  $Same */
  def getAndLock(
      id: String,
      lockTime: Duration,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  ): SMono[GetResult] = {
    val req = async.getAndLockHandler.request(id, lockTime, timeout, retryStrategy)
    wrap(req, id, async.getAndLockHandler, transcoder)
  }

  /** Unlock a locked document.
    *
    * See [[com.couchbase.client.scala.Collection.unlock]] for details.  $Same */
  def unlock(
      id: String,
      cas: Long,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = async.environment.retryStrategy
  ): SMono[Unit] = {
    val req = async.unlockHandler.request(id, cas, timeout, retryStrategy)
    wrap(req, id, async.unlockHandler)
  }

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAndTouch]] for details.  $Same */
  def getAndTouch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  ): SMono[GetResult] = {
    val req = async.getAndTouchHandler.request(id, expiry, timeout, retryStrategy)
    wrap(req, id, async.getAndTouchHandler, transcoder)
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * See [[com.couchbase.client.scala.Collection.lookupIn]] for details.  $Same */
  def lookupIn(
      id: String,
      spec: Seq[LookupInSpec],
      withExpiry: Boolean = false,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  ): SMono[LookupInResult] = {
    getSubDoc(id, spec, withExpiry, timeout, retryStrategy, transcoder)
  }

  /** Retrieves any available version of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAnyReplica]] for details.  $Same */
  def getAnyReplica(
      id: String,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  ): SMono[GetReplicaResult] = {
    getAllReplicas(id, timeout, retryStrategy, transcoder)
      .timeout(timeout)
      .next()
  }

  /** Retrieves all available versions of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAllReplicas]] for details.  $Same */
  def getAllReplicas(
      id: String,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy,
      transcoder: Transcoder = JsonTranscoder.Instance
  ): SFlux[GetReplicaResult] = {
    val reqsTry: Try[Seq[GetRequest]] =
      async.getFromReplicaHandler.requestAll(id, timeout, retryStrategy)

    reqsTry match {
      case Failure(err) => SFlux.raiseError(err)

      case Success(reqs: Seq[GetRequest]) =>
        val monos: Seq[SMono[GetReplicaResult]] = reqs.map(request => {
          core.send(request)

          FutureConversions
            .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
            .flatMap(r => {
              val isReplica = request match {
                case _: GetRequest => false
                case _             => true
              }
              async.getFromReplicaHandler.response(request, id, r, isReplica, transcoder) match {
                case Some(getResult) => SMono.just(getResult)
                case _               => SMono.empty[GetReplicaResult]
              }
            })
        })

        SFlux.mergeSequential(monos).timeout(timeout)
    }

  }

  /** Updates the expiry of the document with the given id.
    *
    * See [[com.couchbase.client.scala.Collection.touch]] for details.  $Same */
  def touch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy
  ): SMono[MutationResult] = {
    val req = async.touchHandler.request(id, expiry, timeout, retryStrategy)
    wrap(req, id, async.touchHandler)
  }

  private def wrap[Resp <: Response, Res](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandler[Resp, Res]
  ): SMono[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        FutureConversions
          .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => handler.response(request, id, r))

      case Failure(err) => SMono.raiseError(err)
    }
  }

  private def wrap[Resp <: Response, Res](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandlerWithTranscoder[Resp, Res],
      transcoder: Transcoder
  ): SMono[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        FutureConversions
          .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => handler.response(request, id, r, transcoder))

      case Failure(err) => SMono.raiseError(err)
    }
  }

  /** Checks if a document exists.
    *
    * See [[com.couchbase.client.scala.Collection.exists]] for details.  $Same */
  def exists(
      id: String,
      timeout: Duration = kvReadTimeout,
      retryStrategy: RetryStrategy = environment.retryStrategy
  ): SMono[ExistsResult] = {
    val req = async.existsHandler.request(id, timeout, retryStrategy)
    wrap(req, id, async.existsHandler)
  }

}
