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
import com.couchbase.client.scala.manager.query.AsyncCollectionQueryIndexManager
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

class AsyncCollection(
    val name: String,
    val bucketName: String,
    val scopeName: String,
    val couchbaseOps: CoreCouchbaseOps,
    val environment: ClusterEnvironment
) extends AsyncCollectionBase {

  /** Manage query indexes for this collection */
  lazy val queryIndexes = new AsyncCollectionQueryIndexManager(this, keyspace)

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * $Same
    */
  def insert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    convert(
      kvOps.insertAsync(
        makeCommonOptions(timeout),
        id,
        encoder(environment.transcoder, serializer, content),
        convert(durability),
        CoreExpiry.NONE
      )
    ).map(result => convert(result))
  }

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * $Same
    */
  def insert[T](
      id: String,
      content: T,
      options: InsertOptions
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    convert(
      kvOps.insertAsync(
        convert(options),
        id,
        encoder(options.transcoder.getOrElse(environment.transcoder), serializer, content),
        convert(options.durability),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime)
      )
    ).map(result => convert(result))
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * $Same
    */
  def replace[T](
      id: String,
      content: T,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    convert(
      kvOps.replaceAsync(
        makeCommonOptions(timeout),
        id,
        encoder(environment.transcoder, serializer, content),
        cas,
        convert(durability),
        CoreExpiry.NONE,
        false
      )
    ).map(result => convert(result))
  }

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * $Same
    */
  def replace[T](
      id: String,
      content: T,
      options: ReplaceOptions
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    convert(
      kvOps.replaceAsync(
        convert(options),
        id,
        encoder(options.transcoder.getOrElse(environment.transcoder), serializer, content),
        options.cas,
        convert(options.durability),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime),
        options.preserveExpiry
      )
    ).map(result => convert(result))
  }

  /** Upserts the contents of a full document in this collection.
    *
    * $Same
    */
  def upsert[T](
      id: String,
      content: T,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    convert(
      kvOps.upsertAsync(
        makeCommonOptions(timeout),
        id,
        encoder(environment.transcoder, serializer, content),
        convert(durability),
        CoreExpiry.NONE,
        false
      )
    ).map(result => convert(result))
  }

  /** Upserts the contents of a full document in this collection.
    *
    * $Same
    */
  def upsert[T](
      id: String,
      content: T,
      options: UpsertOptions
  )(implicit serializer: JsonSerializer[T]): Future[MutationResult] = {
    convert(
      kvOps.upsertAsync(
        convert(options),
        id,
        encoder(options.transcoder.getOrElse(environment.transcoder), serializer, content),
        convert(options.durability),
        ExpiryUtil.expiryActual(options.expiry, options.expiryTime),
        options.preserveExpiry
      )
    ).map(result => convert(result))
  }

  /** Removes a document from this collection, if it exists.
    *
    * $Same
    */
  def remove(
      id: String,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Future[MutationResult] = {
    convert(kvOps.removeAsync(makeCommonOptions(timeout), id, cas, convert(durability)))
      .map(result => convert(result))
  }

  /** Removes a document from this collection, if it exists.
    *
    * $Same
    */
  def remove(
      id: String,
      options: RemoveOptions
  ): Future[MutationResult] = {
    convert(kvOps.removeAsync(convert(options), id, options.cas, convert(options.durability)))
      .map(result => convert(result))
  }

  /** Fetches a full document from this collection.
    *
    * $Same
    */
  def get(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Future[GetResult] = {
    convert(kvOps.getAsync(makeCommonOptions(timeout), id, AsyncCollectionBase.EmptyList, false))
      .map(result => convert(result, environment, None))
  }

  /** Fetches a full document from this collection.
    *
    * $Same
    */
  def get(
      id: String,
      options: GetOptions
  ): Future[GetResult] = {
    convert(kvOps.getAsync(convert(options), id, options.project.asJava, options.withExpiry))
      .map(result => convert(result, environment, options.transcoder))
  }

  /** Sub-Document mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * $Same
    */
  def mutateIn(
      id: String,
      spec: collection.Seq[MutateInSpec],
      cas: Long = 0,
      document: StoreSemantics = StoreSemantics.Replace,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Future[MutateInResult] = {
    convert(
      kvOps.subdocMutateAsync(
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
    * $Same
    */
  def mutateIn(
      id: String,
      spec: collection.Seq[MutateInSpec],
      options: MutateInOptions
  ): Future[MutateInResult] = {
    convert(
      kvOps.subdocMutateAsync(
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
    * $Same
    */
  def getAndLock(
      id: String,
      lockTime: Duration,
      timeout: Duration = kvReadTimeout
  ): Future[GetResult] = {
    convert(kvOps.getAndLockAsync(makeCommonOptions(timeout), id, convert(lockTime)))
      .map(result => convert(result, environment, None))
  }

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * $Same
    */
  def getAndLock(
      id: String,
      lockTime: Duration,
      options: GetAndLockOptions
  ): Future[GetResult] = {
    convert(kvOps.getAndLockAsync(convert(options), id, convert(lockTime)))
      .map(result => convert(result, environment, options.transcoder))
  }

  /** Unlock a locked document.
    *
    * $Same
    */
  def unlock(
      id: String,
      cas: Long,
      timeout: Duration = kvReadTimeout
  ): Future[Unit] = {
    convert(kvOps.unlockAsync(makeCommonOptions(timeout), id, cas)).map(_ => ())
  }

  /** Unlock a locked document.
    *
    * $Same
    */
  def unlock(
      id: String,
      cas: Long,
      options: UnlockOptions
  ): Future[Unit] = {
    convert(kvOps.unlockAsync(convert(options), id, cas)).map(_ => ())
  }

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * $Same
    */
  def getAndTouch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout
  ): Future[GetResult] = {
    convert(kvOps.getAndTouchAsync(makeCommonOptions(timeout), id, convertExpiry(expiry)))
      .map(result => convert(result, environment, None))
  }

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * $Same
    */
  def getAndTouch(
      id: String,
      expiry: Duration,
      options: GetAndTouchOptions
  ): Future[GetResult] = {
    convert(kvOps.getAndTouchAsync(convert(options), id, convertExpiry(expiry)))
      .map(result => convert(result, environment, options.transcoder))
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * $Same
    */
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
    * $Same
    */
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
      options.parentSpan,
      options.accessDeleted
    )
  }

  /** Retrieves any available version of the document.
    *
    * $Same
    */
  def getAnyReplica(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Future[GetReplicaResult] = {
    FutureConversions
      .javaMonoToScalaFuture(
        kvOps.getAnyReplicaReactive(
          makeCommonOptions(timeout),
          id,
          CoreReadPreference.NO_PREFERENCE
        )
      )
      .map(result => convertReplica(result, environment, None))
  }

  /** Retrieves any available version of the document.
    *
    * $Same
    */
  def getAnyReplica(
      id: String,
      options: GetAnyReplicaOptions
  ): Future[GetReplicaResult] = {
    FutureConversions
      .javaMonoToScalaFuture(
        kvOps.getAnyReplicaReactive(
          convert(options),
          id,
          options.readPreference.map(_.toCore).getOrElse(CoreReadPreference.NO_PREFERENCE)
        )
      )
      .map(result => convertReplica(result, environment, options.transcoder))
  }

  /** Retrieves all available versions of the document.
    *
    * Note that this will block the user's thread until all versions have been returned (or failed).
    *
    * Users needing a true non-blocking streaming version should use the reactive version.
    *
    * $Same
    */
  def getAllReplicas(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Seq[Future[GetReplicaResult]] = {
    val opts = GetAllReplicasOptions().timeout(timeout)
    getAllReplicas(id, opts)
  }

  /** Retrieves all available versions of the document.
    *
    * Note that this will block the user's thread until all versions have been returned (or failed).
    *
    * Users needing a true non-blocking streaming version should use the reactive version.
    *
    * $Same
    */
  def getAllReplicas(
      id: String,
      options: GetAllReplicasOptions
  ): Seq[Future[GetReplicaResult]] = {
    // With the move to kvOps (and Protostellar support), we don't know how many replicas we're
    // getting until we've got them all.  Previously we would check the config for this information.
    // Since the API here returns a Seq and not a Future, there is unfortunately
    // no option but to block & buffer the stream and return already completed/failed Futures.
    // Users that require a true streaming solution should use the reactive version.
    val futureList =
      FutureConversions.javaMonoToScalaFuture(
        kvOps
          .getAllReplicasReactive(
            convert(options),
            id,
            options.readPreference.map(_.toCore).getOrElse(CoreReadPreference.NO_PREFERENCE)
          )
          .collectList()
      )

    import scala.jdk.CollectionConverters._

    val awaitTimeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val results      = scala.concurrent.Await.result(futureList, awaitTimeout)
    results.asScala.toSeq.map(res =>
      Future.successful(convertReplica(res, environment, options.transcoder))
    )
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * This variant will read all replicas of the document, and return the first one found.
    *
    * $Same
    */
  @SinceCouchbase("7.6")
  def lookupInAnyReplica(
      id: String,
      spec: collection.Seq[LookupInSpec],
      timeout: Duration = kvReadTimeout
  ): Future[LookupInReplicaResult] = {
    val opts = LookupInAnyReplicaOptions().timeout(timeout)
    lookupInAnyReplica(id, spec, opts)
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * This variant will read all replicas of the document, and return the first one found.
    *
    * $Same
    */
  @SinceCouchbase("7.6")
  def lookupInAnyReplica(
      id: String,
      spec: collection.Seq[LookupInSpec],
      options: LookupInAnyReplicaOptions
  ): Future[LookupInReplicaResult] = {
    FutureConversions
      .javaMonoToScalaFuture(
        kvOps.subdocGetAnyReplicaReactive(
          convert(options),
          id,
          LookupInSpec.map(spec).asJava,
          options.readPreference.map(_.toCore).getOrElse(CoreReadPreference.NO_PREFERENCE)
        )
      )
      .map(result => convertLookupInReplica(result, environment))
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * This variant will read and return all replicas of the document.
    *
    * Note that this will block the user's thread until all versions have been returned (or failed).
    *
    * Users needing a true non-blocking streaming version should use the reactive version.
    *
    * $Same
    */
  @SinceCouchbase("7.6")
  def lookupInAllReplicas(
      id: String,
      spec: collection.Seq[LookupInSpec],
      timeout: Duration = kvReadTimeout
  ): Seq[Future[LookupInReplicaResult]] = {
    val opts = LookupInAllReplicasOptions().timeout(timeout)
    lookupInAllReplicas(id, spec, opts)
  }

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * This variant will read and return all replicas of the document.
    *
    * Note that this will block the user's thread until all versions have been returned (or failed).
    *
    * Users needing a true non-blocking streaming version should use the reactive version.
    *
    * $Same
    */
  @SinceCouchbase("7.6")
  def lookupInAllReplicas(
      id: String,
      spec: collection.Seq[LookupInSpec],
      options: LookupInAllReplicasOptions
  ): Seq[Future[LookupInReplicaResult]] = {
    val futureList =
      FutureConversions.javaMonoToScalaFuture(
        kvOps
          .subdocGetAllReplicasReactive(
            convert(options),
            id,
            LookupInSpec.map(spec).asJava,
            options.readPreference.map(_.toCore).getOrElse(CoreReadPreference.NO_PREFERENCE)
          )
          .collectList()
      )

    import scala.jdk.CollectionConverters._

    val awaitTimeout = if (options.timeout == Duration.MinusInf) kvReadTimeout else options.timeout
    val results      = scala.concurrent.Await.result(futureList, awaitTimeout)
    results.asScala.toSeq.map(res => Future.successful(convertLookupInReplica(res, environment)))
  }

  /** Checks if a document exists.
    *
    * $Same
    */
  def exists(
      id: String,
      timeout: Duration = kvReadTimeout
  ): Future[ExistsResult] = {
    convert(kvOps.existsAsync(makeCommonOptions(timeout), id))
      .map(result => convert(result))
  }

  /** Checks if a document exists.
    *
    * $Same
    */
  def exists(
      id: String,
      options: ExistsOptions
  ): Future[ExistsResult] = {
    convert(kvOps.existsAsync(convert(options), id))
      .map(result => convert(result))
  }

  /** Updates the expiry of the document with the given id.
    *
    * $Same
    */
  def touch(
      id: String,
      expiry: Duration,
      timeout: Duration = kvReadTimeout
  ): Future[MutationResult] = {
    convert(kvOps.touchAsync(makeCommonOptions(timeout), id, convertExpiry(expiry)))
      .map(result => convert(result))
  }

  /** Updates the expiry of the document with the given id.
    *
    * $Same
    */
  def touch(
      id: String,
      expiry: Duration,
      options: TouchOptions
  ): Future[MutationResult] = {
    convert(kvOps.touchAsync(convert(options), id, ExpiryUtil.expiryActual(expiry, None)))
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
  def scan(scanType: ScanType): Future[Iterator[ScanResult]] = {
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
  def scan(scanType: ScanType, opts: ScanOptions): Future[Iterator[ScanResult]] = {
    FutureConversions.javaCFToScalaFuture(
      scanRequest(scanType, opts)
        .collectList()
        .map[Iterator[ScanResult]](v => v.asScala.iterator)
        .toFuture
    )
  }
}
