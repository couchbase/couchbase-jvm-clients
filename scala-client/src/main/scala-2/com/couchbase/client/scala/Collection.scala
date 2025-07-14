/*
 * Copyright (c) 2025 Couchbase, Inc.
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

import com.couchbase.client.scala.codec.{JsonDeserializer, JsonSerializer}
import com.couchbase.client.scala.datastructures.{
  CouchbaseBuffer,
  CouchbaseCollectionOptions,
  CouchbaseMap,
  CouchbaseQueue,
  CouchbaseSet
}
import com.couchbase.client.scala.kv.GetReplicaResult
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.api.kv.{
  CoreExpiry,
  CoreReadPreference,
  CoreSubdocGetCommand,
  CoreSubdocGetResult
}
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.kv._
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.kv._
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.kv.{Observe, ObserveContext}
import com.couchbase.client.core.{Core, CoreKeyspace}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.durability._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.manager.query.{
  AsyncCollectionQueryIndexManager,
  CollectionQueryIndexManager
}
import com.couchbase.client.scala.util.CoreCommonConverters._
import com.couchbase.client.scala.util.{ExpiryUtil, FutureConversions, TimeoutUtil}
import reactor.core.publisher.Flux

import java.lang
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Optional
import scala.compat.java8.FutureConverters
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scala.concurrent.Await
import scala.reflect.ClassTag
import scala.util.Try

class Collection(
    /** Provides access to an async version of this API. */
    val async: AsyncCollection,
    val bucketName: String
) extends CollectionBase {

  /** Provides access to a reactive-programming version of this API. */
  val reactive = new ReactiveCollection(async)

  /** Manage query indexes for this collection */
  lazy val queryIndexes = new CollectionQueryIndexManager(async.queryIndexes)

  /** Fetches a full document from this collection.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.GetOptions]] instead, which supports all available options.
    *
    * @param id             $Id
    * @param timeout        $Timeout
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def get(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Try[GetResult] =
    Try(kvOps.getBlocking(makeCommonOptions(timeout), id, AsyncCollectionBase.EmptyList, false))
      .map(result => convert(result, async.environment, None))

  /** Fetches a full document from this collection.
    *
    * @param id             $Id
    * @param options        $Options
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def get(
      id: String,
      options: GetOptions
  ): Try[GetResult] =
    Try(kvOps.getBlocking(convert(options), id, options.project.asJava, options.withExpiry))
      .map(result => convert(result, async.environment, options.transcoder))

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.InsertOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentAlreadyExistsException`, indicating the document already exists.
    *         $ErrorHandling
    */
  def insert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Try[MutationResult] = {
    Try(
      kvOps.insertBlocking(
        makeCommonOptions(timeout),
        id,
        encoder(async.environment.transcoder, serializer, content),
        convert(durability),
        CoreExpiry.NONE
      )
    ).map(result => convert(result))
  }

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentAlreadyExistsException`, indicating the document already exists.
    *         $ErrorHandling
    */
  def insert[T](
      id: String,
      content: T,
      options: InsertOptions
  )(implicit serializer: JsonSerializer[T]): Try[MutationResult] = {
    Try(
      kvOps.insertBlocking(
        convert(options),
        id,
        encoder(options.transcoder.getOrElse(async.environment.transcoder), serializer, content),
        convert(options.durability),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime)
      )
    ).map(result => convert(result))
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.ReplaceOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param cas           $CAS
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def replace[T](
      id: String,
      content: T,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Try[MutationResult] = {
    Try(
      kvOps.replaceBlocking(
        makeCommonOptions(timeout),
        id,
        encoder(async.environment.transcoder, serializer, content),
        cas,
        convert(durability),
        CoreExpiry.NONE,
        false
      )
    ).map(result => convert(result))
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def replace[T](
      id: String,
      content: T,
      options: ReplaceOptions
  )(implicit serializer: JsonSerializer[T]): Try[MutationResult] = {
    Try(
      kvOps.replaceBlocking(
        convert(options),
        id,
        encoder(options.transcoder.getOrElse(async.environment.transcoder), serializer, content),
        options.cas,
        convert(options.durability),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime),
        options.preserveExpiry
      )
    ).map(result => convert(result))
  }

  /** Upserts the contents of a full document in this collection.
    *
    * Upsert here means to insert the document if it does not exist, or replace the content if it does.
    *
    * This overloads provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.UpsertOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  $ErrorHandling
    */
  def upsert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Try[MutationResult] = {
    Try(
      kvOps.upsertBlocking(
        makeCommonOptions(timeout),
        id,
        encoder(async.environment.transcoder, serializer, content),
        convert(durability),
        CoreExpiry.NONE,
        false
      )
    ).map(result => convert(result))
  }

  /** Upserts the contents of a full document in this collection.
    *
    * Upsert here means to insert the document if it does not exist, or replace the content if it does.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  $ErrorHandling
    */
  def upsert[T](
      id: String,
      content: T,
      options: UpsertOptions
  )(implicit serializer: JsonSerializer[T]): Try[MutationResult] = {
    Try(
      kvOps.upsertBlocking(
        convert(options),
        id,
        encoder(options.transcoder.getOrElse(async.environment.transcoder), serializer, content),
        convert(options.durability),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime),
        options.preserveExpiry
      )
    ).map(result => convert(result))
  }

  /** Removes a document from this collection, if it exists.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.RemoveOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param cas           $CAS
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def remove(
      id: String,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Try[MutationResult] = {
    Try(kvOps.removeBlocking(makeCommonOptions(timeout), id, cas, convert(durability)))
      .map(result => convert(result))
  }

  /** Removes a document from this collection, if it exists.
    *
    * @param id            $Id
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def remove(
      id: String,
      options: RemoveOptions
  ): Try[MutationResult] = {
    Try(kvOps.removeBlocking(convert(options), id, options.cas, convert(options.durability)))
      .map(result => convert(result))
  }

  /** Sub-Document mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * Mutations are all-or-nothing: if one fails, then no mutation will be performed.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.MutateInOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param spec          a sequence of `MutateInSpec` specifying what mutations to apply to the document.  See
    *                      [[com.couchbase.client.scala.kv.MutateInSpec]] for more details.
    * @param cas           $CAS
    * @param document      controls whether the document should be inserted, upserted, or not touched.  See
    *                      [[com.couchbase.client.scala.kv.StoreSemantics]] for details.
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(MutateInResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def mutateIn(
      id: String,
      spec: collection.Seq[MutateInSpec],
      cas: Long = 0,
      document: StoreSemantics = StoreSemantics.Replace,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Try[MutateInResult] = {
    Try(
      kvOps.subdocMutateBlocking(
        makeCommonOptions(timeout),
        id,
        () => spec.map(v => v.convert).asJava,
        convert(document),
        cas,
        convert(durability),
        CoreExpiry.NONE,
        false,
        false,
        false
      )
    ).map(result => convert(result))
  }

  /** Sub-Document mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * Mutations are all-or-nothing: if one fails, then no mutation will be performed.
    *
    * @param id            $Id
    * @param spec          a sequence of `MutateInSpec` specifying what mutations to apply to the document.  See
    *                      [[com.couchbase.client.scala.kv.MutateInSpec]] for more details.
    * @param options       $Options
    *
    * @return on success, a `Success(MutateInResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def mutateIn(
      id: String,
      spec: collection.Seq[MutateInSpec],
      options: MutateInOptions
  ): Try[MutateInResult] = {
    Try(
      kvOps.subdocMutateBlocking(
        convert(options),
        id,
        () => spec.map(v => v.convert).asJava,
        convert(options.document),
        options.cas,
        convert(options.durability),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime),
        options.preserveExpiry,
        options.accessDeleted,
        options.createAsDeleted
      )
    ).map(result => convert(result))
  }

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * The CAS value returned in the [[kv.GetResult]] is the document's 'key': during the locked period, the document
    * may only be modified by providing this CAS.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.GetAndLockOptions]] instead, which supports all available options.
    *
    * @param id             $Id
    * @param lockTime        how long to lock the document for
    * @param timeout        $Timeout
    *
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def getAndLock(
      id: String,
      lockTime: Duration,
      timeout: Duration = kvReadTimeout
  ): Try[GetResult] =
    Try(kvOps.getAndLockBlocking(makeCommonOptions(timeout), id, convert(lockTime)))
      .map(result => convert(result, async.environment, None))

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * The CAS value returned in the [[kv.GetResult]] is the document's 'key': during the locked period, the document
    * may only be modified by providing this CAS.
    *
    * @param id             $Id
    * @param lockTime       how long to lock the document for
    * @param options        $Options
    *
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def getAndLock(
      id: String,
      lockTime: Duration,
      options: GetAndLockOptions
  ): Try[GetResult] =
    Try(kvOps.getAndLockBlocking(convert(options), id, convert(lockTime)))
      .map(result => convert(result, async.environment, options.transcoder))

  /** Unlock a locked document.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.UnlockOptions]] instead, which supports all available options.
    *
    * @param id             $Id
    * @param cas            must match the CAS value return from a previous `.getAndLock()` to successfully
    *                       unlock the document
    * @param timeout        $Timeout
    *
    * @return on success, a `Success(Unit)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def unlock(
      id: String,
      cas: Long,
      timeout: Duration = kvReadTimeout
  ): Try[Unit] =
    Try(kvOps.unlockBlocking(makeCommonOptions(timeout), id, cas)).map(_ => ())

  /** Unlock a locked document.
    *
    * @param id             $Id
    * @param cas            must match the CAS value return from a previous `.getAndLock()` to successfully
    *                       unlock the document
    * @param options        $Options
    *
    * @return on success, a `Success(Unit)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def unlock(
      id: String,
      cas: Long,
      options: UnlockOptions
  ): Try[Unit] =
    Try(kvOps.unlockBlocking(convert(options), id, cas)).map(_ => ())

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * @param id             $Id
    * @param expiry         $Expiry
    * @param options        $Options
    *
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def getAndTouch(
      id: String,
      expiry: Duration,
      options: GetAndTouchOptions
  ): Try[GetResult] =
    Try(kvOps.getAndTouchBlocking(convert(options), id, convertExpiry(expiry)))
      .map(result => convert(result, async.environment, options.transcoder))

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.GetAndTouchOptions]] instead, which supports all available options.
    *
    * @param id             $Id
    * @param expiry         $Expiry
    * @param timeout        $Timeout
    *
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def getAndTouch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout
  ): Try[GetResult] =
    Try(kvOps.getAndTouchBlocking(makeCommonOptions(timeout), id, convertExpiry(expiry)))
      .map(result => convert(result, async.environment, None))

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[kv.LookupInResult]] for details on
    * how to process the results.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.LookupInOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param spec          a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                      [[kv.LookupInSpec]] for more details.
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def lookupIn(
      id: String,
      spec: collection.Seq[LookupInSpec],
      timeout: Duration = kvReadTimeout
  ): Try[LookupInResult] =
    block(async.lookupIn(id, spec, timeout))

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[kv.LookupInResult]] for details on
    * how to process the results.
    *
    * @param id            $Id
    * @param spec          a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                      [[kv.LookupInSpec]] for more details.
    * @param options       $Options
    *
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def lookupIn(
      id: String,
      spec: collection.Seq[LookupInSpec],
      options: LookupInOptions
  ): Try[LookupInResult] =
    block(async.lookupIn(id, spec, options))

  /** Retrieves any available version of the document.
    *
    * The application should default to using `.get()` instead.  This method is intended for high-availability
    * situations where, say, a `.get()` operation has failed, and the
    * application wants to return any - even possibly stale - data as soon as possible.
    *
    * Under the hood this sends a request to all configured replicas for the document, including the active, and
    * whichever returns first is returned.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.GetAnyReplicaOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def getAnyReplica(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Try[GetReplicaResult] =
    block(async.getAnyReplica(id, timeout))

  /** Retrieves any available version of the document.
    *
    * The application should default to using `.get()` instead.  This method is intended for high-availability
    * situations where, say, a `.get()` operation has failed, and the
    * application wants to return any - even possibly stale - data as soon as possible.
    *
    * Under the hood this sends a request to all configured replicas for the document, including the active, and
    * whichever returns first is returned.
    *
    * @param id            $Id
    * @param options       $Options
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def getAnyReplica(
      id: String,
      options: GetAnyReplicaOptions
  ): Try[GetReplicaResult] = {
    block(async.getAnyReplica(id, options))
  }

  /** Retrieves all available versions of the document.
    *
    * The application should default to using `.get()` instead.  This method is intended for advanced scenarios,
    * including where a particular write has ambiguously failed (e.g. it may or may not have succeeded), and the
    * application wants to attempt manual verification and resolution.
    *
    * The returned `Iterable` will block on each call to `next` until the next replica has responded.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.GetAllReplicasOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def getAllReplicas(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Iterable[GetReplicaResult] = {
    val futures = async.getAllReplicas(id, timeout)
    futures.map(f => Await.result(f, timeout))
  }

  /** Retrieves all available versions of the document.
    *
    * The application should default to using `.get()` instead.  This method is intended for advanced scenarios,
    * including where a particular write has ambiguously failed (e.g. it may or may not have succeeded), and the
    * application wants to attempt manual verification and resolution.
    *
    * The returned Iterable` will block on each call to `next` until the next replica has responded.
    *
    * @param id            $Id
    * @param options       $Options
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def getAllReplicas(
      id: String,
      options: GetAllReplicasOptions
  ): Iterable[GetReplicaResult] = {
    val futures = async.getAllReplicas(id, options)
    val to      = if (options.timeout.isFinite) options.timeout else kvReadTimeout
    futures.map(f => Await.result(f, to))
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[kv.LookupInReplicaResult]] for details on
    * how to process the results.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.LookupInAllReplicasOptions]] instead, which supports all available options.
    *
    * This variant will read and return all replicas of the document.
    *
    * @param id      $Id
    * @param spec    a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                [[kv.LookupInSpec]] for more details.
    * @param timeout $Timeout
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.
    */
  @SinceCouchbase("7.6")
  def lookupInAllReplicas(
      id: String,
      spec: collection.Seq[LookupInSpec],
      timeout: Duration = kvReadTimeout
  ): Try[Iterable[LookupInReplicaResult]] = {
    val futures = async.lookupInAllReplicas(id, spec, timeout)
    block(Future.sequence(futures)).map(_.toIterable)
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[kv.LookupInReplicaResult]] for details on
    * how to process the results.
    *
    * This variant will read and return all replicas of the document.
    *
    * @param id      $Id
    * @param spec    a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                [[kv.LookupInSpec]] for more details.
    * @param options $Options
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.
    */
  @SinceCouchbase("7.6")
  def lookupInAllReplicas(
      id: String,
      spec: collection.Seq[LookupInSpec],
      options: LookupInAllReplicasOptions
  ): Try[Iterable[LookupInReplicaResult]] = {
    val futures = async.lookupInAllReplicas(id, spec, options)
    block(Future.sequence(futures)).map(_.toIterable)
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[kv.LookupInReplicaResult]] for details on
    * how to process the results.
    *
    * This variant will read all replicas of the document, and return the first one found.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.LookupInAnyReplicaOptions]] instead, which supports all available options.
    *
    * @param id      $Id
    * @param spec    a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                [[kv.LookupInSpec]] for more details.
    * @param timeout $Timeout
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.
    */
  @SinceCouchbase("7.6")
  def lookupInAnyReplica(
      id: String,
      spec: collection.Seq[LookupInSpec],
      timeout: Duration = kvReadTimeout
  ): Try[LookupInReplicaResult] = {
    block(async.lookupInAnyReplica(id, spec, timeout))
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[kv.LookupInReplicaResult]] for details on
    * how to process the results.
    *
    * This variant will read all replicas of the document, and return the first one found.
    *
    * @param id      $Id
    * @param spec    a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                [[kv.LookupInSpec]] for more details.
    * @param options $Options
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.
    */
  @SinceCouchbase("7.6")
  def lookupInAnyReplica(
      id: String,
      spec: collection.Seq[LookupInSpec],
      options: LookupInAnyReplicaOptions
  ): Try[LookupInReplicaResult] =
    block(async.lookupInAnyReplica(id, spec, options))

  /** Checks if a document exists.
    *
    * This doesn't fetch the document so if the application simply needs to know if the document exists, this is the
    * most efficient method.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.ExistsOptions]] instead, which supports all available options.
    *
    * @param id             $Id
    * @param timeout        $Timeout
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def exists(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Try[ExistsResult] =
    Try(kvOps.existsBlocking(makeCommonOptions(timeout), id))
      .map(result => convert(result))

  /** Checks if a document exists.
    *
    * This doesn't fetch the document so if the application simply needs to know if the document exists, this is the
    * most efficient method.
    *
    * @param id             $Id
    * @param options        $Options
    *
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def exists(
      id: String,
      options: ExistsOptions
  ): Try[ExistsResult] =
    Try(kvOps.existsBlocking(convert(options), id))
      .map(result => convert(result))

  /** Updates the expiry of the document with the given id.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[com.couchbase.client.scala.kv.TouchOptions]] instead, which supports all available options.
    *
    * @param id             $Id
    * @param timeout        $Timeout
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def touch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout
  ): Try[MutationResult] = {
    Try(kvOps.touchBlocking(makeCommonOptions(timeout), id, convertExpiry(expiry)))
      .map(result => convert(result))
  }

  /** Updates the expiry of the document with the given id.
    *
    * @param id             $Id
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def touch(
      id: String,
      expiry: Duration,
      options: TouchOptions
  ): Try[MutationResult] = {
    Try(kvOps.touchBlocking(convert(options), id, convertExpiry(expiry)))
      .map(result => convert(result))
  }

  /** Initiates a KV range scan, which will return a non-blocking stream of KV documents.
    *
    * Uses default options.
    *
    * '''CAVEAT:'''This method is suitable for use cases that require relatively
    * low concurrency and tolerate relatively high latency.
    * If your application does many scans at once, or requires low latency results,
    * we recommend using SQL++ (with a primary index on the collection) instead.
    */
  @SinceCouchbase("7.6")
  def scan(scanType: ScanType): Try[Iterator[ScanResult]] = {
    scan(scanType, ScanOptions())
  }

  /** Initiates a KV range scan, which will return a non-blocking stream of KV documents.
    *
    * '''CAVEAT:'''This method is suitable for use cases that require relatively
    * low concurrency and tolerate relatively high latency.
    * If your application does many scans at once, or requires low latency results,
    * we recommend using SQL++ (with a primary index on the collection) instead.
    */
  @SinceCouchbase("7.6")
  def scan(scanType: ScanType, opts: ScanOptions): Try[Iterator[ScanResult]] = {
    block(async.scan(scanType, opts))
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseBuffer]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def buffer[T](id: String, options: Option[CouchbaseCollectionOptions] = None)(
      implicit decode: JsonDeserializer[T],
      encode: JsonSerializer[T],
      tag: ClassTag[T]
  ): CouchbaseBuffer[T] = {
    new CouchbaseBuffer[T](id, this)
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseSet]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def set[T](id: String, options: Option[CouchbaseCollectionOptions] = None)(
      implicit decode: JsonDeserializer[T],
      encode: JsonSerializer[T]
  ): CouchbaseSet[T] = {
    new CouchbaseSet[T](id, this)
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseMap]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def map[T](id: String, options: Option[CouchbaseCollectionOptions] = None)(
      implicit decode: JsonDeserializer[T],
      encode: JsonSerializer[T],
      tag: ClassTag[T]
  ): CouchbaseMap[T] = {
    new CouchbaseMap[T](id, this)
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseQueue]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def queue[T](id: String, options: Option[CouchbaseCollectionOptions] = None)(
      implicit decode: JsonDeserializer[T],
      encode: JsonSerializer[T],
      tag: ClassTag[T]
  ): CouchbaseQueue[T] = {
    new CouchbaseQueue[T](id, this)
  }

}
