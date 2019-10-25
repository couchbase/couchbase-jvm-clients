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
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.datastructures._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}
import scala.reflect.runtime.universe._

object Collection {
  private[scala] def block[T](in: Future[T]): Try[T] = {
    try {
      Try(Await.result(in, Duration.Inf))
    } catch {
      case NonFatal(err) => Failure(err)
    }
  }
}

/**
  * Provides blocking, synchronous access to all collection APIs.  This is the main entry-point for key-value (KV)
  * operations.
  *
  * <p>If asynchronous access is needed, we recommend looking at the [[AsyncCollection]] which is built around
  * returning `Future`s, or the [[ReactiveCollection]] which provides a reactive programming API.
  *
  * This blocking API itself is just a small layer on top of the [[AsyncCollection]] which blocks the current thread
  * until the request completes with a response.</p>
  *
  * @author Graham Pople
  * @since 1.0.0
  * @define Id             the unique identifier of the document
  * @define CAS            Couchbase documents all have a CAS (Compare-And-Set) field, a simple integer that allows
  *                        optimistic concurrency - e.g. it can detect if another agent has modified a document
  *                        in-between this agent getting and modifying the document.  See **CHANGEME** for a full
  *                        description.  The default is 0, which disables CAS checking.
  * @define WithExpiry     Couchbase documents optionally can have an expiration field set, e.g. when they will
  *                        automatically expire.  For efficiency reasons, by default the value of this expiration
  *                        field is not fetched upon getting a document.  If expiry is being used, then set this
  *                        field to true to ensure the expiration is fetched.  This will not only make it available
  *                        in the returned result, but also ensure that the expiry is available to use when mutating
  *                        the document, to avoid accidentally resetting the expiry to the default of 0.
  * @define Expiry         Couchbase documents optionally can have an expiration field set, e.g. when they will
  *                        automatically expire.  On mutations if this is left at the default (0), then any expiry
  *                        will be removed and the document will never expire.  If the application wants to preserve
  *                        expiration then they should use the `withExpiration` parameter on any gets, and provide
  *                        the returned expiration parameter to any mutations.
  * @define Timeout        when the operation will timeout.  This will default to `timeoutConfig().kvTimeout()` in the
  *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy  provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define ErrorHandling  any `scala.util.control.NonFatal` error returned will derive ultimately from
  *                        `com.couchbase.client.core.error.CouchbaseException`.  If the exception also derives from
  *                        `com.couchbase.client.core.error.RetryableOperationException`.
  *                        then the failure was most likely temporary and may succeed if the application tries it
  *                        again.  (Though note that, in some cases, the operation may have in fact succeeded, and
  *                        the server was unable to report this to the SDK.  So the application should consider
  *                        carefully the result of reapplying the operation, and perhaps consider some more complex
  *                        error handling logic, possibly including the use of
  *                        [[Collection.getAllReplicas]]).  If the exception
  *                        does not derive from
  *                        `com.couchbase.client.core.error.RetryableOperationException`
  *                        then this is indicative of a more
  *                        permanent error or an application bug, that probably needs human review.
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.Conversions.JsonSerializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at ***CHANGEME:TYPES***
  * @define Durability     writes in Couchbase are written to a single node, and from there the Couchbase Server will
  *                        take care of sending that mutation to any configured replicas.  This parameter provides
  *                        some control over ensuring the success of the mutation's replication.  See
  *                        [[com.couchbase.client.scala.durability.Durability]]
  *                        for a detailed discussion.
  **/
class Collection(
                  /** Provides access to an async version of this API. */
                  val async: AsyncCollection,
                  bucketName: String) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  def name: String = async.name

  /** Provides access to a reactive-programming version of this API. */
  val reactive = new ReactiveCollection(async)

  /** Provides access to less-commonly used methods. */
  val binary = new BinaryCollection(async.binary)

  private[scala] val kvTimeout = async.kvTimeout
  private[scala] val retryStrategy = async.retryStrategy

  private def block[T](in: Future[T]) =
    Collection.block(in)

  /** Fetches a full document from this collection.
    *
    * @param id             $Id
    * @param withExpiry     $WithExpiry
    * @param project        projection is an advanced feature allowing one or more fields to be fetched from a JSON
    *                       document, and the results
    *                       combined into a [[json.JsonObject]] result.  By default this
    *                       is set to None, meaning a normal full document fetch will be performed.
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def get(
           id: String,
           withExpiry: Boolean = false,
           project: Seq[String] = Seq.empty,
           timeout: Duration = kvTimeout,
           retryStrategy: RetryStrategy = retryStrategy,
           transcoder: Transcoder = JsonTranscoder.Instance
         ): Try[GetResult] =
    block(
      async
        .get(id, withExpiry, project, timeout, retryStrategy, transcoder)
    )

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param durability    $Durability
    * @param expiry        $Expiry
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentAlreadyExistsException`, indicating the document already exists.
    *         $ErrorHandling
    **/
  def insert[T](
                 id: String,
                 content: T,
                 durability: Durability = Disabled,
                 expiry: Duration = 0.seconds,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy,
                 transcoder: Transcoder = JsonTranscoder.Instance
               )(implicit serializer: JsonSerializer[T]): Try[MutationResult] =
    block(
      async.insert(
        id,
        content,
        durability,
        expiry,
        timeout,
        retryStrategy,
        transcoder
      )
    )


  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param cas           $CAS
    * @param durability    $Durability
    * @param expiry        $Expiry
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    **/
  def replace[T](
                  id: String,
                  content: T,
                  cas: Long = 0,
                  durability: Durability = Disabled,
                  expiry: Duration = 0.seconds,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = retryStrategy,
                  transcoder: Transcoder = JsonTranscoder.Instance
                )(implicit serializer: JsonSerializer[T]): Try[MutationResult] =
    block(
      async.replace(
        id,
        content,
        cas,
        durability,
        expiry,
        timeout,
        retryStrategy,
        transcoder
      )
    )

  /** Upserts the contents of a full document in this collection.
    *
    * Upsert here means to insert the document if it does not exist, or replace the content if it does.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param durability    $Durability
    * @param expiry        $Expiry
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  $ErrorHandling
    */
  def upsert[T](
                 id: String,
                 content: T,
                 durability: Durability = Disabled,
                 expiry: Duration = 0.seconds,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy,
                 transcoder: Transcoder = JsonTranscoder.Instance
               )(implicit serializer: JsonSerializer[T]): Try[MutationResult] =
    block(
      async.upsert(
        id,
        content,
        durability,
        expiry,
        timeout,
        retryStrategy,
        transcoder
      )
    )

  /** Removes a document from this collection, if it exists.
    *
    * @param id            $Id
    * @param cas           $CAS
    * @param durability    $Durability
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    **/
  def remove(
              id: String,
              cas: Long = 0,
              durability: Durability = Disabled,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = retryStrategy
            ): Try[MutationResult] =
    block(
      async.remove(id, cas, durability, timeout, retryStrategy)
    )

  /** SubDocument mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * Mutations are all-or-nothing: if one fails, then no mutation will be performed.
    *
    * @param id            $Id
    * @param spec          a sequence of `MutateInSpec` specifying what mutations to apply to the document.  See
    *                      [[kv.MutateInSpec]] for more details.
    * @param cas           $CAS
    * @param document      controls whether the document should be inserted, upserted, or not touched.  See
    *                      [[kv.StoreSemantics]] for details.
    * @param durability    $Durability
    * @param expiry        $Expiry
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    *
    * @return on success, a `Success(MutateInResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    **/
  def mutateIn(
                id: String,
                spec: Seq[MutateInSpec],
                cas: Long = 0,
                document: StoreSemantics = StoreSemantics.Replace,
                durability: Durability = Disabled,
                expiry: Duration = 0.seconds,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = retryStrategy,
                transcoder: Transcoder = JsonTranscoder.Instance,
                @Stability.Internal accessDeleted: Boolean = false
              ): Try[MutateInResult] =
    block(
      async.mutateIn(
        id,
        spec,
        cas,
        document,
        durability,
        expiry,
        timeout,
        retryStrategy,
        transcoder,
        accessDeleted
      )
    )

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * The CAS value returned in the [[kv.GetResult]] is the document's 'key': during the locked period, the document
    * may only be modified by providing this CAS.
    *
    * @param id             $Id
    * @param lockTime        how long to lock the document for
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def getAndLock(id: String,
                 lockTime: Duration,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy,
                 transcoder: Transcoder = JsonTranscoder.Instance): Try[GetResult] =
    block(
      async.getAndLock(id, lockTime, timeout, retryStrategy, transcoder)
    )

  /** Unlock a locked document.
    *
    * @param id             $Id
    * @param cas            must match the CAS value return from a previous [[Collection.getAndLock]] to successfully
    *                       unlock the document
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a `Success(Unit)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def unlock(
              id: String,
              cas: Long,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = retryStrategy
            ): Try[Unit] =
    block(async.unlock(id, cas, timeout, retryStrategy))

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * @param id             $Id
    * @param expiry         $Expiry
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def getAndTouch(id: String,
                  expiry: Duration,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = retryStrategy,
                  transcoder: Transcoder = JsonTranscoder.Instance): Try[GetResult] =
    block(
      async.getAndTouch(
        id,
        expiry,
        timeout,
        retryStrategy,
        transcoder
      )
    )

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[kv.LookupInResult]] for details on
    * how to process the results.
    *
    * @param id            $Id
    * @param spec          a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                      [[kv.LookupInSpec]] for more details.
    * @param withExpiry    $WithExpiry
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    **/
  def lookupIn(
                id: String,
                spec: Seq[LookupInSpec],
                withExpiry: Boolean = false,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = retryStrategy,
                transcoder: Transcoder = JsonTranscoder.Instance
              ): Try[LookupInResult] =
    block(async.lookupIn(id, spec, withExpiry, timeout, retryStrategy, transcoder))

  /** Retrieves any available version of the document.
    *
    * The application should default to using [[Collection.get]] instead.  This method is intended for high-availability
    * situations where, say, a [[Collection.get]] operation has failed, and the
    * application wants to return any - even possibly stale - data as soon as possible.
    *
    * Under the hood this sends a request to all configured replicas for the document, including the master, and
    * whichever returns first is returned.
    *
    * @param id            $Id
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    **/
  def getAnyReplica(id: String,
                    timeout: Duration = kvTimeout,
                    retryStrategy: RetryStrategy = retryStrategy,
                    transcoder: Transcoder = JsonTranscoder.Instance): Try[GetReplicaResult] =
    Try(reactive
      .getAnyReplica(id, timeout, retryStrategy, transcoder)
      .block(timeout))

  /** Retrieves all available versions of the document.
    *
    * The application should default to using [[Collection.get]] instead.  This method is intended for advanced scenarios,
    * including where a particular write has ambiguously failed (e.g. it may or may not have succeeded), and the
    * application wants to attempt manual verification and resolution.
    *
    * The returned `Iterable` will block on each call to `next` until the next replica has responded.
    *
    * @param id            $Id
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found. $ErrorHandling
    **/
  def getAllReplicas(id: String,
                     timeout: Duration = kvTimeout,
                     retryStrategy: RetryStrategy = retryStrategy,
                     transcoder: Transcoder = JsonTranscoder.Instance): Iterable[GetReplicaResult] =
    reactive.getAllReplicas(id, timeout, retryStrategy, transcoder).toIterable()

  /** Checks if a document exists.
    *
    * This doesn't fetch the document so if the appplication simply needs to know if the document exists, this is the
    * most efficient method.
    *
    * @param id             $Id
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be `com
    *         .couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def exists[T](
                 id: String,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy
               ): Try[ExistsResult] =
    block(async.exists(id, timeout, retryStrategy))

  /** Updates the expiry of the document with the given id.
    *
    * @param id             $Id
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be
    *         `com.couchbase.client.core.error.DocumentDoesNotExistException`, indicating the document could not be
    *         found.  $ErrorHandling
    */
  def touch(id: String,
            expiry: Duration,
            timeout: Duration = kvTimeout,
            retryStrategy: RetryStrategy = retryStrategy
           ): Try[MutationResult] = {
    block(async.touch(id, expiry, timeout, retryStrategy))
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseBuffer]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def buffer[T](id: String,
                options: Option[CouchbaseCollectionOptions] = None)
               (implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: TypeTag[T]): CouchbaseBuffer[T] = {
    new CouchbaseBuffer[T](id, this)
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseSet]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def set[T](id: String,
                options: Option[CouchbaseCollectionOptions] = None)
               (implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: TypeTag[T]): CouchbaseSet[T] = {
    new CouchbaseSet[T](id, this)
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseMap]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def map[T](id: String,
                options: Option[CouchbaseCollectionOptions] = None)
               (implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: TypeTag[T]): CouchbaseMap[T] = {
    new CouchbaseMap[T](id, this)
  }

  /** Returns a [[com.couchbase.client.scala.datastructures.CouchbaseQueue]] backed by this collection.
    *
    * @param id id of the document underyling the datastructure
    * @param options options for controlling the behaviour of the datastructure
    */
  def queue[T](id: String,
                options: Option[CouchbaseCollectionOptions] = None)
               (implicit decode: JsonDeserializer[T], encode: JsonSerializer[T], tag: TypeTag[T]): CouchbaseQueue[T] = {
    new CouchbaseQueue[T](id, this)
  }
}


