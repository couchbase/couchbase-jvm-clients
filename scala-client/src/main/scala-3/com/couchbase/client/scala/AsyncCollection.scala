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

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * $Same
    */
  def insert[T](
      id: String,
      content: T,
      options: InsertOptions = InsertOptions.Default
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
      options: ReplaceOptions = ReplaceOptions.Default
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
      options: UpsertOptions = UpsertOptions.Default
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
      options: RemoveOptions = RemoveOptions.Default
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
      options: GetOptions = GetOptions.Default
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
      options: MutateInOptions = MutateInOptions.Default
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
      options: GetAndLockOptions = GetAndLockOptions.Default
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
      options: UnlockOptions = UnlockOptions.Default
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
      options: GetAndTouchOptions = GetAndTouchOptions.Default
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
      options: LookupInOptions = LookupInOptions.Default
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

  /** Checks if a document exists.
    *
    * $Same
    */
  def exists(
      id: String,
      options: ExistsOptions = ExistsOptions.Default
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
      options: TouchOptions = TouchOptions.Default
  ): Future[MutationResult] = {
    convert(kvOps.touchAsync(convert(options), id, ExpiryUtil.expiryActual(expiry, None)))
      .map(result => convert(result))
  }

  /** Initiates a KV range scan, which will return a non-blocking stream of KV documents.
    *
    * '''CAVEAT:'''This method is suitable for use cases that require relatively
    * low concurrency and tolerate relatively high latency.
    * If your application does many scans at once, or requires low latency results,
    * we recommend using SQL++ (with a primary index on the collection) instead.
    */
  @SinceCouchbase("7.6")
  def scan(
      scanType: ScanType,
      opts: ScanOptions = ScanOptions.Default
  ): Future[Iterator[ScanResult]] = {
    FutureConversions.javaCFToScalaFuture(
      scanRequest(scanType, opts)
        .collectList()
        .map[Iterator[ScanResult]](v => v.asScala.iterator)
        .toFuture
    )
  }
}
