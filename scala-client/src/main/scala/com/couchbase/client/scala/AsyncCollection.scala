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
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.error._
import com.couchbase.client.core.error.context.KeyValueErrorContext
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.kv.{Observe, ObserveContext}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.kv.handlers._
import com.couchbase.client.scala.util.{FutureConversions, TimeoutUtil}

import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

private[scala] case class HandlerParams(
    core: Core,
    bucketName: String,
    collectionIdentifier: CollectionIdentifier,
    env: ClusterEnvironment
) {
  def tracer = env.coreEnv.requestTracer()
}

private[scala] case class HandlerBasicParams(core: Core, env: ClusterEnvironment) {
  def tracer = env.coreEnv.requestTracer()
}

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
class AsyncCollection(
    val name: String,
    bucketName: String,
    scopeName: String,
    val core: Core,
    val environment: ClusterEnvironment
) {
  private[scala] implicit val ec: ExecutionContext = environment.ec

  import com.couchbase.client.scala.util.DurationConversions._

  private[scala] val kvTimeout: Durability => Duration = TimeoutUtil.kvTimeout(environment)
  private[scala] val kvReadTimeout: Duration           = environment.timeoutConfig.kvTimeout()
  private[scala] val collectionIdentifier =
    new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(name))
  private[scala] val hp                    = HandlerParams(core, bucketName, collectionIdentifier, environment)
  private[scala] val existsHandler         = new ExistsHandler(hp)
  private[scala] val insertHandler         = new InsertHandler(hp)
  private[scala] val replaceHandler        = new ReplaceHandler(hp)
  private[scala] val upsertHandler         = new UpsertHandler(hp)
  private[scala] val removeHandler         = new RemoveHandler(hp)
  private[scala] val getFullDocHandler     = new GetFullDocHandler(hp)
  private[scala] val getSubDocHandler      = new GetSubDocumentHandler(hp)
  private[scala] val getAndTouchHandler    = new GetAndTouchHandler(hp)
  private[scala] val getAndLockHandler     = new GetAndLockHandler(hp)
  private[scala] val mutateInHandler       = new MutateInHandler(hp)
  private[scala] val unlockHandler         = new UnlockHandler(hp)
  private[scala] val getFromReplicaHandler = new GetFromReplicaHandler(hp)
  private[scala] val touchHandler          = new TouchHandler(hp)

  val binary = new AsyncBinaryCollection(this)

  private[scala] def wrap[Resp <: Response, Res](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandler[Resp, Res]
  ): Future[Res] = {
    AsyncCollection.wrap(in, id, handler, core)
  }

  private[scala] def wrapWithDurability[Resp <: Response, Res <: HasDurabilityTokens](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandler[Resp, Res],
      durability: Durability,
      remove: Boolean,
      timeout: java.time.Duration
  ): Future[Res] = {
    AsyncCollection.wrapWithDurability(
      in,
      id,
      handler,
      durability,
      remove,
      timeout,
      core,
      bucketName,
      collectionIdentifier
    )
  }

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * $Same */
  def insert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    val opts = InsertOptions().durability(durability).timeout(timeout)
    insert(id, content, opts)
  }

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * $Same */
  def insert[T](
      id: String,
      content: T,
      options: InsertOptions
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req = insertHandler.request(
      id,
      content,
      options.durability,
      options.expiry,
      timeoutActual,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.transcoder.getOrElse(environment.transcoder),
      serializer,
      options.parentSpan
    )
    wrapWithDurability(req, id, insertHandler, options.durability, false, timeoutActual)
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * $Same */
  def replace[T](
      id: String,
      content: T,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    val opts = ReplaceOptions().cas(cas).durability(durability).timeout(timeout)
    replace(id, content, opts)
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * $Same */
  def replace[T](
      id: String,
      content: T,
      options: ReplaceOptions
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req = replaceHandler.request(
      id,
      content,
      options.cas,
      options.durability,
      options.expiry,
      timeoutActual,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.transcoder.getOrElse(environment.transcoder),
      serializer,
      options.parentSpan
    )
    wrapWithDurability(req, id, replaceHandler, options.durability, false, timeoutActual)
  }

  /** Upserts the contents of a full document in this collection.
    *
    * $Same */
  def upsert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    val opts = UpsertOptions().durability(durability).timeout(timeout)
    upsert(id, content, opts)
  }

  /** Upserts the contents of a full document in this collection.
    *
    * $Same */
  def upsert[T](
      id: String,
      content: T,
      options: UpsertOptions
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req = upsertHandler.request(
      id,
      content,
      options.durability,
      options.expiry,
      timeoutActual,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.transcoder.getOrElse(environment.transcoder),
      serializer,
      options.parentSpan
    )
    wrapWithDurability(req, id, upsertHandler, options.durability, false, timeoutActual)
  }

  /** Removes a document from this collection, if it exists.
    *
    * $Same */
  def remove(
      id: String,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Future[MutationResult] = {
    val opts = RemoveOptions().cas(cas).durability(durability).timeout(timeout)
    remove(id, opts)
  }

  /** Removes a document from this collection, if it exists.
    *
    * $Same */
  def remove(
      id: String,
      options: RemoveOptions
  ): Future[MutationResult] = {
    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req = removeHandler.request(
      id,
      options.cas,
      options.durability,
      timeoutActual,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    wrapWithDurability(req, id, removeHandler, options.durability, true, timeoutActual)
  }

  /** Fetches a full document from this collection.
    *
    * $Same */
  def get(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Future[GetResult] = {
    val opts = GetOptions().timeout(timeout)
    get(id, opts)
  }

  /** Fetches a full document from this collection.
    *
    * $Same */
  def get(
      id: String,
      options: GetOptions
  ): Future[GetResult] = {
    val project       = options.project
    val timeout       = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy)
    val parentSpan    = options.parentSpan
    val transcoder    = options.transcoder.getOrElse(environment.transcoder)
    val withExpiry    = options.withExpiry

    if (project.nonEmpty) {
      getSubDocHandler.requestProject(id, project, timeout, retryStrategy, parentSpan) match {
        case Success(request) =>
          core.send(request)

          val out = FutureConverters
            .toScala(request.response())
            .flatMap(response => {
              getSubDocHandler.responseProject(request, id, response, transcoder) match {
                case Success(v: GetResult) => Future.successful(v)
                case Failure(err)          => Future.failed(err)
              }
            })

          out.onComplete(_ => request.context.logicallyComplete())

          out

        case Failure(err) => Future.failed(err)
      }

    } else if (withExpiry) {
      getSubDoc(
        id,
        AsyncCollection.getFullDoc,
        withExpiry,
        timeout,
        retryStrategy,
        transcoder,
        parentSpan
      ).map(
        lookupInResult =>
          GetResult(
            id,
            Left(lookupInResult.contentAs[Array[Byte]](0).get),
            lookupInResult.flags,
            lookupInResult.cas,
            lookupInResult.expiry,
            transcoder
          )
      )
    } else {
      getFullDoc(id, timeout, retryStrategy, transcoder, parentSpan)
    }
  }

  private def getFullDoc(
      id: String,
      timeout: Duration,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder,
      parentSpan: Option[RequestSpan]
  ): Future[GetResult] = {
    val req = getFullDocHandler.request(id, timeout, retryStrategy, parentSpan)
    AsyncCollection.wrapGet(req, id, getFullDocHandler, transcoder, core)
  }

  private def getSubDoc(
      id: String,
      spec: collection.Seq[LookupInSpec],
      withExpiry: Boolean,
      timeout: Duration,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder,
      parentSpan: Option[RequestSpan]
  ): Future[LookupInResult] = {
    val req = getSubDocHandler.request(id, spec, withExpiry, timeout, retryStrategy, parentSpan)
    req match {
      case Success(request) =>
        core.send(request)

        val out = FutureConverters
          .toScala(request.response())
          .map(response => {
            getSubDocHandler.response(request, id, response, withExpiry, transcoder)
          })

        out.onComplete(_ => request.context.logicallyComplete())

        out

      case Failure(err) => Future.failed(err)
    }
  }

  /** Sub-Document mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * $Same */
  def mutateIn(
      id: String,
      spec: collection.Seq[MutateInSpec],
      cas: Long = 0,
      document: StoreSemantics = StoreSemantics.Replace,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Future[MutateInResult] = {
    val opts = MutateInOptions().cas(cas).document(document).durability(durability).timeout(timeout)
    mutateIn(id, spec, opts)
  }

  /** Sub-Document mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * $Same */
  def mutateIn(
      id: String,
      spec: collection.Seq[MutateInSpec],
      options: MutateInOptions
  ): Future[MutateInResult] = {

    val timeoutActual =
      if (options.timeout == Duration.MinusInf) kvTimeout(options.durability) else options.timeout
    val req = mutateInHandler.request(
      id,
      spec,
      options.cas,
      options.document,
      options.durability,
      options.expiry,
      timeoutActual,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.accessDeleted,
      options.createAsDeleted,
      options.transcoder.getOrElse(environment.transcoder),
      options.parentSpan
    )

    req match {
      case Success(request) =>
        core.send(request)

        val out = FutureConverters
          .toScala(request.response())
          .map(response => mutateInHandler.response(request, id, options.document, response))

        out.onComplete(_ => request.context.logicallyComplete())

        options.durability match {
          case ClientVerified(replicateTo, persistTo) =>
            out.flatMap(response => {

              val observeCtx = new ObserveContext(
                core.context(),
                PersistTo.asCore(persistTo),
                ReplicateTo.asCore(replicateTo),
                response.mutationToken.asJava,
                response.cas,
                collectionIdentifier,
                id,
                false,
                timeoutActual,
                request.internalSpan().toRequestSpan()
              )

              FutureConversions
                .javaMonoToScalaFuture(Observe.poll(observeCtx))
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
    * $Same */
  def getAndLock(
      id: String,
      lockTime: Duration,
      timeout: Duration = kvReadTimeout
  ): Future[GetResult] = {
    val opts = GetAndLockOptions().timeout(timeout)
    getAndLock(id, lockTime, opts)
  }

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * $Same */
  def getAndLock(
      id: String,
      lockTime: Duration,
      options: GetAndLockOptions
  ): Future[GetResult] = {
    val timeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val req = getAndLockHandler.request(
      id,
      lockTime,
      timeout,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    AsyncCollection.wrapGet(
      req,
      id,
      getAndLockHandler,
      options.transcoder.getOrElse(environment.transcoder),
      core
    )
  }

  /** Unlock a locked document.
    *
    * $Same */
  def unlock(
      id: String,
      cas: Long,
      timeout: Duration = kvReadTimeout
  ): Future[Unit] = {
    val opts = UnlockOptions().timeout(timeout)
    unlock(id, cas, opts)
  }

  /** Unlock a locked document.
    *
    * $Same */
  def unlock(
      id: String,
      cas: Long,
      options: UnlockOptions
  ): Future[Unit] = {
    val timeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val req = unlockHandler.request(
      id,
      cas,
      timeout,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    wrap(req, id, unlockHandler)
  }

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * $Same */
  def getAndTouch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout
  ): Future[GetResult] = {
    val opts = GetAndTouchOptions().timeout(timeout)
    getAndTouch(id, expiry, opts)
  }

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * $Same */
  def getAndTouch(
      id: String,
      expiry: Duration,
      options: GetAndTouchOptions
  ): Future[GetResult] = {
    val timeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val in = getAndTouchHandler.request(
      id,
      expiry,
      timeout,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    AsyncCollection.wrapGet(
      in,
      id,
      getAndTouchHandler,
      options.transcoder.getOrElse(environment.transcoder),
      core
    )
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * $Same */
  def lookupIn(
      id: String,
      spec: collection.Seq[LookupInSpec],
      timeout: Duration = kvReadTimeout
  ): Future[LookupInResult] = {
    val opts = LookupInOptions().timeout(timeout)
    lookupIn(id, spec, opts)
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * $Same */
  def lookupIn(
      id: String,
      spec: collection.Seq[LookupInSpec],
      options: LookupInOptions
  ): Future[LookupInResult] = {
    val timeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    getSubDoc(
      id,
      spec,
      options.withExpiry,
      timeout,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.transcoder.getOrElse(environment.transcoder),
      options.parentSpan
    )
  }

  /** Retrieves any available version of the document.
    *
    * $Same */
  def getAnyReplica(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Future[GetReplicaResult] = {
    val opts = GetAnyReplicaOptions().timeout(timeout)
    getAnyReplica(id, opts)
  }

  /** Retrieves any available version of the document.
    *
    * $Same */
  def getAnyReplica(
      id: String,
      options: GetAnyReplicaOptions
  ): Future[GetReplicaResult] = {
    getAllReplicas(id, options.convert).take(1).head
  }

  /** Retrieves all available versions of the document.
    *
    * $Same */
  def getAllReplicas(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Seq[Future[GetReplicaResult]] = {
    val opts = GetAllReplicasOptions().timeout(timeout)
    getAllReplicas(id, opts)
  }

  /** Retrieves all available versions of the document.
    *
    * $Same */
  def getAllReplicas(
      id: String,
      options: GetAllReplicasOptions
  ): Seq[Future[GetReplicaResult]] = {
    val timeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val reqsTry: Try[Seq[GetRequest]] =
      getFromReplicaHandler.requestAll(
        id,
        timeout,
        options.retryStrategy.getOrElse(environment.retryStrategy),
        options.parentSpan
      )

    reqsTry match {
      case Failure(err) => Seq(Future.failed(err))

      case Success(reqs: Seq[GetRequest]) =>
        reqs.map(request => {
          core.send(request)

          val out = FutureConverters
            .toScala(request.response())
            .flatMap(response => {
              val isReplica = request match {
                case _: GetRequest => false
                case _             => true
              }
              getFromReplicaHandler.response(
                request,
                id,
                response,
                isReplica,
                options.transcoder.getOrElse(environment.transcoder)
              ) match {
                case Some(x) => Future.successful(x)
                case _ =>
                  val ctx = KeyValueErrorContext.completedRequest(request, response.status())
                  Future.failed(new DocumentNotFoundException(ctx))
              }
            })

          out.onComplete(_ => request.context.logicallyComplete())
          out
        })
    }
  }

  /** Checks if a document exists.
    *
    * $Same */
  def exists(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Future[ExistsResult] = {
    val opts = ExistsOptions().timeout(timeout)
    exists(id, opts)
  }

  /** Checks if a document exists.
    *
    * $Same */
  def exists(
      id: String,
      options: ExistsOptions
  ): Future[ExistsResult] = {
    val timeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val req = existsHandler.request(
      id,
      timeout,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    wrap(req, id, existsHandler)
  }

  /** Updates the expiry of the document with the given id.
    *
    * $Same */
  def touch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout
  ): Future[MutationResult] = {
    val opts = TouchOptions().timeout(timeout)
    touch(id, expiry, opts)
  }

  /** Updates the expiry of the document with the given id.
    *
    * $Same */
  def touch(
      id: String,
      expiry: Duration,
      options: TouchOptions
  ): Future[MutationResult] = {
    val timeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val req = touchHandler.request(
      id,
      expiry,
      timeout,
      options.retryStrategy.getOrElse(environment.retryStrategy),
      options.parentSpan
    )
    wrap(req, id, touchHandler)
  }
}

object AsyncCollection {
  private[scala] val getFullDoc = Array(LookupInSpec.get(""))

  private def wrap[Resp <: Response, Res](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandler[Resp, Res],
      core: Core
  )(implicit ec: ExecutionContext): Future[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        val out = FutureConverters
          .toScala(request.response())
          .map(response => handler.response(request, id, response))

        out.onComplete(_ => request.context.logicallyComplete())

        out

      case Failure(err) => Future.failed(err)
    }
  }

  private def wrapGet[Resp <: Response, Res](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandlerWithTranscoder[Resp, Res],
      transcoder: Transcoder,
      core: Core
  )(implicit ec: ExecutionContext): Future[Res] = {
    in match {
      case Success(request) =>
        core.send(request)

        val out = FutureConverters
          .toScala(request.response())
          .map(response => {
            handler.response(request, id, response, transcoder)
          })

        out.onComplete(_ => request.context.logicallyComplete())

        out

      case Failure(err) => Future.failed(err)
    }
  }

  private def wrap[Resp <: Response, Res](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandlerWithTranscoder[Resp, Res],
      transcoder: Transcoder,
      core: Core
  )(implicit ec: ExecutionContext): Future[Res] = {
    in match {
      case Success(request) =>
        core.send[Resp](request)

        val out = FutureConverters
          .toScala(request.response())
          .map(response => handler.response(request, id, response, transcoder))

        out.onComplete(_ => request.context.logicallyComplete())

        out

      case Failure(err) => Future.failed(err)
    }
  }

  private def wrapWithDurability[Resp <: Response, Res <: HasDurabilityTokens](
      in: Try[KeyValueRequest[Resp]],
      id: String,
      handler: KeyValueRequestHandler[Resp, Res],
      durability: Durability,
      remove: Boolean,
      timeout: java.time.Duration,
      core: Core,
      bucketName: String,
      collectionidentifier: CollectionIdentifier
  )(implicit ec: ExecutionContext): Future[Res] = {
    val initial: Future[Res] = wrap(in, id, handler, core)

    durability match {
      case ClientVerified(replicateTo, persistTo) =>
        initial.flatMap(response => {

          val observeCtx = new ObserveContext(
            core.context(),
            PersistTo.asCore(persistTo),
            ReplicateTo.asCore(replicateTo),
            response.mutationToken.asJava,
            response.cas,
            collectionidentifier,
            id,
            remove,
            timeout,
            in.get.internalSpan().toRequestSpan()
          )

          FutureConversions
            .javaMonoToScalaFuture(Observe.poll(observeCtx))
            // After the observe return the original response
            .map(_ => response)
        })

      case _ => initial
    }
  }
}
