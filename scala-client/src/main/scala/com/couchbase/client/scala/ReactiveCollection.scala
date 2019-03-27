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

import java.util.Optional
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.Reactor
import com.couchbase.client.core.msg.kv.GetRequest
import com.couchbase.client.core.msg.{Request, Response}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.Validators
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.FutureConversions
import io.opentracing.Span
import reactor.core.scala.publisher.{Flux, Mono}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv.handlers.RequestHandler

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
  *                          but returns the same result object asynchronously in a Project Reactor `Mono`.
  * */
class ReactiveCollection(async: AsyncCollection) {
  private val kvTimeout = async.kvTimeout
  private val environment = async.environment
  private val core = async.core

  import com.couchbase.client.scala.util.DurationConversions._

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * See [[com.couchbase.client.scala.Collection.insert]] for details.  $Same */
  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy())
               (implicit ev: Conversions.Encodable[T]): Mono[MutationResult] = {
    val req = async.insertHandler.request(id, content, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.insertHandler)
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * See [[com.couchbase.client.scala.Collection.replace]] for details.  $Same */
  def replace[T](id: String,
                 content: T,
                 cas: Long = 0,
                 durability: Durability = Disabled,
                 expiration: Duration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy())
                (implicit ev: Conversions.Encodable[T]): Mono[MutationResult] = {
    val req = async.replaceHandler.request(id, content, cas, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.replaceHandler)
  }

  /** Upserts the contents of a full document in this collection.
    *
    * See [[com.couchbase.client.scala.Collection.upsert]] for details.  $Same */
  def upsert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiration: Duration = 0.seconds,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy())
               (implicit ev: Conversions.Encodable[T]): Mono[MutationResult] = {
    val req = async.upsertHandler.request(id, content, durability, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.upsertHandler)
  }

  /** Removes a document from this collection, if it exists.
    *
    * See [[com.couchbase.client.scala.Collection.remove]] for details.  $Same */
  def remove(id: String,
             cas: Long = 0,
             durability: Durability = Disabled,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[MutationResult] = {
    val req = async.removeHandler.request(id, cas, durability, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.removeHandler)
  }

  /** Fetches a full document from this collection.
    *
    * See [[com.couchbase.client.scala.Collection.get]] for details.  $Same */
  def get(id: String,
          withExpiration: Boolean = false,
          project: Seq[String] = Seq.empty,
          parentSpan: Option[Span] = None,
          timeout: Duration = kvTimeout,
          retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[Option[GetResult]] = {

    // Implementation note: Option is returned because Mono.empty is hard to work with.  See JCBC-1310.

    if (project.nonEmpty) {
      async.getSubDocHandler.requestProject(id, project, parentSpan, timeout, retryStrategy) match {
        case Success(request) =>
          core.send(request)

          FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
            .map(r => async.getSubDocHandler.responseProject(id, r) match {
              case Success(v) => v
              case Failure(err) => throw err
            })

        case Failure(err) => Mono.error(err)
      }

    }
    else if (withExpiration) {
      getSubDoc(id, AsyncCollection.getFullDoc, withExpiration, parentSpan, timeout, retryStrategy)
        .map(lookupInResult => lookupInResult.map(v => {
          GetResult(id, Left(v.contentAsBytes(0).get), v.flags, v.cas, v.expiration)
        })
        )
    }
    else {
      getFullDoc(id, parentSpan, timeout, retryStrategy)
    }
  }

  private def getFullDoc(id: String,
                         parentSpan: Option[Span] = None,
                         timeout: Duration = kvTimeout,
                         retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[Option[GetResult]] = {
    val req = async.getFullDocHandler.request(id, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.getFullDocHandler)
  }


  private def getSubDoc(id: String,
                        spec: Seq[LookupInSpec],
                        withExpiration: Boolean,
                        parentSpan: Option[Span] = None,
                        timeout: Duration = kvTimeout,
                        retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[Option[LookupInResult]] = {
    async.getSubDocHandler.request(id, spec, withExpiration, parentSpan, timeout, retryStrategy) match {
      case Success(request) =>
        core.send(request)

        FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => async.getSubDocHandler.response(id, r))

      case Failure(err) => Mono.error(err)
    }
  }

  /** SubDocument mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * See [[com.couchbase.client.scala.Collection.mutateIn]] for details.  $Same */
  def mutateIn(id: String,
               spec: Seq[MutateInSpec],
               cas: Long = 0,
               document: DocumentCreation = DocumentCreation.DoNothing,
               durability: Durability = Disabled,
               parentSpan: Option[Span] = None,
               expiration: Duration,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[MutateInResult] = {
    val req = async.mutateInHandler.request(id, spec, cas, document, durability, expiration, parentSpan, timeout,
      retryStrategy)
    req match {
      case Success(request) =>
        core.send(request)

        FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => async.mutateInHandler.response(id, document, r))

      case Failure(err) => Mono.error(err)
    }
  }

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * See [[com.couchbase.client.scala.Collection.getAndLock]] for details.  $Same */
  def getAndLock(id: String,
                 expiration: Duration = 30.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy()
                ): Mono[Option[GetResult]] = {
    val req = async.getAndLockHandler.request(id, expiration, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.getAndLockHandler)
  }

  /** Unlock a locked document.
    *
    * See [[com.couchbase.client.scala.Collection.unlock]] for details.  $Same */
  def unlock(id: String,
             cas: Long,
             parentSpan: Option[Span] = None,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = async.environment.retryStrategy()
            ): Mono[Unit] = {
    val req = async.unlockHandler.request(id, cas, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.unlockHandler)
  }

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAndTouch]] for details.  $Same */
  def getAndTouch(id: String,
                  expiration: Duration,
                  durability: Durability = Disabled,
                  parentSpan: Option[Span] = None,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = environment.retryStrategy()
                 ): Mono[Option[GetResult]] = {
    val req = async.getAndTouchHandler.request(id, expiration, durability, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.getAndTouchHandler)
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * See [[com.couchbase.client.scala.Collection.lookupIn]] for details.  $Same */
  def lookupIn(id: String,
               spec: Seq[LookupInSpec],
               parentSpan: Option[Span] = None,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy()
              ): Mono[Option[LookupInResult]] = {
    // Set withExpiration to false as it makes all subdoc lookups multi operations, which changes semantics - app
    // may expect error to be raised and it won't
    getSubDoc(id, spec, withExpiration = false, parentSpan, timeout, retryStrategy)
  }

  /** Retrieves any available version of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAnyReplica]] for details.  $Same */
  def getAnyReplica(id: String,
                    parentSpan: Option[Span] = None,
                    timeout: Duration = kvTimeout,
                    retryStrategy: RetryStrategy = environment.retryStrategy()
                   ): Mono[GetResult] = {
    getAllReplicas(id, parentSpan, timeout, retryStrategy).next()
  }

  /** Retrieves all available versions of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAllReplicas]] for details.  $Same */
  def getAllReplicas(id: String,
                     parentSpan: Option[Span] = None,
                     timeout: Duration = kvTimeout,
                     retryStrategy: RetryStrategy = environment.retryStrategy()
                    ): Flux[GetResult] = {
    val reqsTry: Try[Seq[GetRequest]] = async.getFromReplicaHandler.requestAll(id, parentSpan, timeout, retryStrategy)

    reqsTry match {
      case Failure(err) => Flux.error(err)

      case Success(reqs: Seq[GetRequest]) =>
        val monos: Seq[Mono[GetResult]] = reqs.map(request => {
          core.send(request)

          FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
            .flatMap(r => {
              async.getFullDocHandler.response(id, r) match {
                case Some(getResult) => Mono.just(getResult)
                case _ => Mono.empty[GetResult]
              }
            })
        })

        Flux.merge(monos)
    }

  }

  private def wrap[Resp <: Response, Res](in: Try[Request[Resp]], id: String, handler: RequestHandler[Resp, Res])
  : Mono[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .map(r => handler.response(id, r))

      case Failure(err) => Mono.error(err)
    }
  }

  /** Checks if a document exists.
    *
    * See [[com.couchbase.client.scala.Collection.exists]] for details.  $Same */
  def exists[T](id: String,
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy()): Mono[ExistsResult] = {
    val req = async.existsHandler.request(id, parentSpan, timeout, retryStrategy)
    wrap(req, id, async.existsHandler)
  }

}
