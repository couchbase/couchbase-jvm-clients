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

import com.couchbase.client.scala.codec.{JsonSerializer, JsonDeserializer, _}
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.{ExpiryUtil}
import com.couchbase.client.scala.util.CoreCommonConverters._
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Try, Failure, Success}
import com.couchbase.client.core.annotation.SinceCouchbase

class Collection(
    /** Provides access to an async version of this API. */
    val async: AsyncCollection,
    val bucketName: String
) extends CollectionBase {

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
      options: GetOptions = GetOptions.Default
  ): Try[GetResult] =
    Try(kvOps.getBlocking(convert(options), id, options.project.asJava, options.withExpiry))
      .map(result => convert(result, async.environment, options.transcoder))

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
      options: InsertOptions = InsertOptions.Default
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
      options: ReplaceOptions = ReplaceOptions.Default
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
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  $ErrorHandling
    */
  def upsert[T](
      id: String,
      content: T,
      options: UpsertOptions = UpsertOptions.Default
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
    * @param id            $Id
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    */
  def remove(
      id: String,
      options: RemoveOptions = RemoveOptions.Default
  ): Try[MutationResult] = {
    Try(kvOps.removeBlocking(convert(options), id, options.cas, convert(options.durability)))
      .map(result => convert(result))
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
      options: MutateInOptions = MutateInOptions.Default
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
      options: GetAndLockOptions = GetAndLockOptions.Default
  ): Try[GetResult] =
    Try(kvOps.getAndLockBlocking(convert(options), id, convert(lockTime)))
      .map(result => convert(result, async.environment, options.transcoder))

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
      options: UnlockOptions = UnlockOptions.Default
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
      options: GetAndTouchOptions = GetAndTouchOptions.Default
  ): Try[GetResult] =
    Try(kvOps.getAndTouchBlocking(convert(options), id, convertExpiry(expiry)))
      .map(result => convert(result, async.environment, options.transcoder))

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
      options: LookupInOptions = LookupInOptions.Default
  ): Try[LookupInResult] =
    block(async.lookupIn(id, spec, options))

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
      options: ExistsOptions = ExistsOptions.Default
  ): Try[ExistsResult] =
    Try(kvOps.existsBlocking(convert(options), id))
      .map(result => convert(result))

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
      options: TouchOptions = TouchOptions.Default
  ): Try[MutationResult] = {
    Try(kvOps.touchBlocking(convert(options), id, convertExpiry(expiry)))
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
  ): Try[Iterator[ScanResult]] = {
    block(async.scan(scanType, opts))
  }
}
