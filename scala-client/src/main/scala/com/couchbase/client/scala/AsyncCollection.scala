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

import com.couchbase.client.core.Core
import com.couchbase.client.core.error._
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.msg.{Request, Response}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.kv.{Observe, ObserveContext}
import com.couchbase.client.scala.AsyncCollection.wrap
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.kv.handlers._
import com.couchbase.client.scala.util.FutureConversions

import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

case class HandlerParams(core: Core, bucketName: String, collectionIdentifier: CollectionIdentifier)


/** Provides asynchronous access to all collection APIs, based around Scala `Future`s.  This is the main entry-point
  * for key-value (KV) operations.
  *
  * <p>If synchronous, blocking access is needed, we recommend looking at the [[Collection]].  If a more advanced
  * async API based around reactive programming is desired, then check out the [[ReactiveCollection]].
  *
  * @author Graham Pople
  * @since 1.0.0
  * @define Same             This asynchronous version performs the same functionality and takes the same parameters,
  *                          but returns the same result object asynchronously in a `Future`.
  * */
class AsyncCollection(name: String,
                      bucketName: String,
                      scopeName: String,
                      val core: Core,
                      val environment: ClusterEnvironment) {
  private[scala] implicit val ec: ExecutionContext = environment.ec

  import com.couchbase.client.scala.util.DurationConversions._

  private[scala] val kvTimeout = javaDurationToScala(environment.timeoutConfig.kvTimeout())
  private[scala] val retryStrategy = environment.retryStrategy
  private[scala] val collectionIdentifier = new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(name))
  private[scala] val hp = HandlerParams(core, bucketName, collectionIdentifier)
  private[scala] val existsHandler = new ExistsHandler(hp)
  private[scala] val insertHandler = new InsertHandler(hp)
  private[scala] val replaceHandler = new ReplaceHandler(hp)
  private[scala] val upsertHandler = new UpsertHandler(hp)
  private[scala] val removeHandler = new RemoveHandler(hp)
  private[scala] val getFullDocHandler = new GetFullDocHandler(hp)
  private[scala] val getSubDocHandler = new GetSubDocumentHandler(hp)
  private[scala] val getAndTouchHandler = new GetAndTouchHandler(hp)
  private[scala] val getAndLockHandler = new GetAndLockHandler(hp)
  private[scala] val mutateInHandler = new MutateInHandler(hp)
  private[scala] val unlockHandler = new UnlockHandler(hp)
  private[scala] val getFromReplicaHandler = new GetFromReplicaHandler(hp)
  private[scala] val touchHandler = new TouchHandler(hp)

  val binary = new AsyncBinaryCollection(this)

  private[scala] def wrap[Resp <: Response, Res](in: Try[Request[Resp]],
                                                 id: String,
                                                 handler: RequestHandler[Resp, Res]): Future[Res] = {
    AsyncCollection.wrap(in, id, handler, core)
  }

  private[scala] def wrapWithDurability[Resp <: Response, Res <: HasDurabilityTokens](in: Try[Request[Resp]],
                                                                                      id: String,
                                                                                      handler: RequestHandler[Resp,
                                                                                        Res],
                                                                                      durability: Durability,
                                                                                      remove: Boolean,
                                                                                      timeout: java.time.Duration)
  : Future[Res] = {
    AsyncCollection.wrapWithDurability(in, id, handler, durability, remove, timeout, core, bucketName,
      collectionIdentifier)
  }

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * See [[com.couchbase.client.scala.Collection.insert]] for details.  $Same */
  def insert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiry: Duration = 0.seconds,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy)
               (implicit ev: Conversions.Encodable[T])
  : Future[MutationResult] = {
    val req = insertHandler.request(id, content, durability, expiry, timeout, retryStrategy)
    wrapWithDurability(req, id, insertHandler, durability, false, timeout)
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * See [[com.couchbase.client.scala.Collection.replace]] for details.  $Same */
  def replace[T](id: String,
                 content: T,
                 cas: Long = 0,
                 durability: Durability = Disabled,
                 expiry: Duration = 0.seconds,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy)
                (implicit ev: Conversions.Encodable[T]): Future[MutationResult] = {
    val req = replaceHandler.request(id, content, cas, durability, expiry, timeout, retryStrategy)
    wrapWithDurability(req, id, replaceHandler, durability, false, timeout)
  }

  /** Upserts the contents of a full document in this collection.
    *
    * See [[com.couchbase.client.scala.Collection.upsert]] for details.  $Same */
  def upsert[T](id: String,
                content: T,
                durability: Durability = Disabled,
                expiry: Duration = 0.seconds,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = environment.retryStrategy)
               (implicit ev: Conversions.Encodable[T]): Future[MutationResult] = {
    val req = upsertHandler.request(id, content, durability, expiry, timeout, retryStrategy)
    wrapWithDurability(req, id, upsertHandler, durability, false, timeout)
  }

  /** Removes a document from this collection, if it exists.
    *
    * See [[com.couchbase.client.scala.Collection.remove]] for details.  $Same */
  def remove(id: String,
             cas: Long = 0,
             durability: Durability = Disabled,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy): Future[MutationResult] = {
    val req = removeHandler.request(id, cas, durability, timeout, retryStrategy)
    wrapWithDurability(req, id, removeHandler, durability, true, timeout)
  }

  /** Fetches a full document from this collection.
    *
    * See [[com.couchbase.client.scala.Collection.get]] for details.  $Same */
  def get(id: String,
          withExpiry: Boolean = false,
          project: Seq[String] = Seq.empty,
          timeout: Duration = kvTimeout,
          retryStrategy: RetryStrategy = environment.retryStrategy)
  : Future[GetResult] = {

    if (project.nonEmpty) {
      getSubDocHandler.requestProject(id, project, timeout, retryStrategy) match {
        case Success(request) =>
          core.send(request)

          val out: Future[GetResult] = FutureConverters.toScala(request.response())
            .flatMap(response => {
              val ret = getSubDocHandler.responseProject(id, response) match {
                case Success(v: Option[GetResult]) =>
                  v match {
                    case Some(x) => Future.successful(x)
                    case _ => Future.failed(KeyNotFoundException.forKey(id))
                  }

                case Failure(err) => Future.failed(err)
              }

              ret
            })

          out

        case Failure(err) => Future.failed(err)
      }

    }
    else if (withExpiry) {
      getSubDoc(id, AsyncCollection.getFullDoc, withExpiry, timeout, retryStrategy)
        .map(lookupInResult =>
          GetResult(id, Left(lookupInResult.contentAsBytes(0).get),
            lookupInResult.flags, lookupInResult.cas, lookupInResult.expiry))
    }
    else {
      getFullDoc(id, timeout, retryStrategy)
    }
  }

  private def getFullDoc(id: String,
                         timeout: Duration = kvTimeout,
                         retryStrategy: RetryStrategy = environment.retryStrategy): Future[GetResult] = {
    val req = getFullDocHandler.request(id, timeout, retryStrategy)
    wrap(req, id, getFullDocHandler)
      .map {
        case Some(x) => x
        case _ => throw KeyNotFoundException.forKey(id)
      }
  }


  private def getSubDoc(id: String,
                        spec: Seq[LookupInSpec],
                        withExpiry: Boolean,
                        timeout: Duration = kvTimeout,
                        retryStrategy: RetryStrategy = environment.retryStrategy): Future[LookupInResult] = {
    val req = getSubDocHandler.request(id, spec, withExpiry, timeout, retryStrategy)
    req match {
      case Success(request) =>
        core.send(request)

        FutureConverters.toScala(request.response())
          .map(response => getSubDocHandler.response(id, response, withExpiry))
          .map {
            case Some(x) => x
            case _ => throw KeyNotFoundException.forKey(id)
          }

      case Failure(err) => Future.failed(err)
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
               expiry: Duration = 0.seconds,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy): Future[MutateInResult] = {
    val req = mutateInHandler.request(id, spec, cas, document, durability, expiry, timeout,
      retryStrategy)

    req match {
      case Success(request) =>
        core.send(request)

        val out = FutureConverters.toScala(request.response())
          .map(response => mutateInHandler.response(id, document, response))

        durability match {
          case ClientVerified(replicateTo, persistTo) =>
            out.flatMap(response => {

              val observeCtx = new ObserveContext(core.context(),
                PersistTo.asCore(persistTo),
                ReplicateTo.asCore(replicateTo),
                response.mutationToken.asJava,
                response.cas,
                collectionIdentifier,
                id,
                false,
                timeout
              )

              FutureConversions.javaMonoToScalaFuture(Observe.poll(observeCtx))

                // After the observe return the original response
                .map(_ => response)
            })

          case _ => out
        }

        out

      case Failure(err) => Future.failed(err)
    }

  }

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * See [[com.couchbase.client.scala.Collection.getAndLock]] for details.  $Same */
  def getAndLock(id: String,
                 expiry: Duration = 30.seconds,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = environment.retryStrategy
                ): Future[GetResult] = {
    val req = getAndLockHandler.request(id, expiry, timeout, retryStrategy)
    wrap(req, id, getAndLockHandler)
      .map {
        case Some(x) => x
        case _ => throw KeyNotFoundException.forKey(id)
      }
  }

  /** Unlock a locked document.
    *
    * See [[com.couchbase.client.scala.Collection.unlock]] for details.  $Same */
  def unlock(id: String,
             cas: Long,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy
            ): Future[Unit] = {
    val req = unlockHandler.request(id, cas, timeout, retryStrategy)
    wrap(req, id, unlockHandler)
  }

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAndTouch]] for details.  $Same */
  def getAndTouch(id: String,
                  expiry: Duration,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = environment.retryStrategy
                 ): Future[GetResult] = {
    val req = getAndTouchHandler.request(id, expiry, timeout, retryStrategy)
    wrap(req, id, getAndTouchHandler)
      .map {
        case Some(x) => x
        case _ => throw KeyNotFoundException.forKey(id)
      }
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * See [[com.couchbase.client.scala.Collection.lookupIn]] for details.  $Same */
  def lookupIn(id: String,
               spec: Seq[LookupInSpec],
               withExpiry: Boolean = false,
               timeout: Duration = kvTimeout,
               retryStrategy: RetryStrategy = environment.retryStrategy
              ): Future[LookupInResult] = {
    getSubDoc(id, spec, withExpiry, timeout, retryStrategy)
  }

  /** Retrieves any available version of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAnyReplica]] for details.  $Same */
  def getAnyReplica(id: String,
                    timeout: Duration = kvTimeout,
                    retryStrategy: RetryStrategy = environment.retryStrategy
                   ): Future[GetReplicaResult] = {
    getAllReplicas(id, timeout, retryStrategy).take(1).head
  }

  /** Retrieves all available versions of the document.
    *
    * See [[com.couchbase.client.scala.Collection.getAllReplicas]] for details.  $Same */
  def getAllReplicas(id: String,
                     timeout: Duration = kvTimeout,
                     retryStrategy: RetryStrategy = environment.retryStrategy
                    ): Seq[Future[GetReplicaResult]] = {
    val reqsTry: Try[Seq[GetRequest]] = getFromReplicaHandler.requestAll(id, timeout, retryStrategy)

    reqsTry match {
      case Failure(err) => Seq(Future.failed(err))

      case Success(reqs: Seq[GetRequest]) =>
        val out = reqs.map(request => {
          core.send(request)

          FutureConverters.toScala(request.response())
            .map(response => {
              val isMaster = request match {
                case _: GetRequest => true
                case _ => false
              }
              getFromReplicaHandler.response(id, response, isMaster)
            })
            .map {
              case Some(x) => x
              case _ => throw KeyNotFoundException.forKey(id)
            }
        })

        out
    }
  }

  /** Checks if a document exists.
    *
    * See [[com.couchbase.client.scala.Collection.exists]] for details.  $Same */
  def exists(id: String,
             timeout: Duration = kvTimeout,
             retryStrategy: RetryStrategy = environment.retryStrategy): Future[ExistsResult] = {
    val req = existsHandler.request(id, timeout, retryStrategy)
    wrap(req, id, existsHandler)
  }

  /** Updates the expiry of the document with the given id.
    *
    * See [[com.couchbase.client.scala.Collection.touch]] for details.  $Same */
  def touch(id: String,
            expiry: Duration,
            timeout: Duration = kvTimeout,
            retryStrategy: RetryStrategy = retryStrategy
           ): Future[MutationResult] = {
    val req = touchHandler.request(id, expiry, timeout, retryStrategy)
    wrap(req, id, touchHandler)
  }
}

object AsyncCollection {
  private[scala] val getFullDoc = Array(LookupInSpec.get(""))

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
                                                                               collectionidentifier: CollectionIdentifier)
                                                                              (implicit ec: ExecutionContext)
  : Future[Res] = {
    val initial: Future[Res] = wrap(in, id, handler, core)

    durability match {
      case ClientVerified(replicateTo, persistTo) =>
        initial.flatMap(response => {

          val observeCtx = new ObserveContext(core.context(),
            PersistTo.asCore(persistTo),
            ReplicateTo.asCore(replicateTo),
            response.mutationToken.asJava,
            response.cas,
            collectionidentifier,
            id,
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
