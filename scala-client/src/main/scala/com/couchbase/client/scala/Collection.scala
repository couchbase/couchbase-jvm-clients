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

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.api._
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv._
import io.opentracing.Span
import reactor.core.scala.publisher.Mono
//import com.couchbase.client.scala.query.N1qlQueryResult

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.reflect.runtime.universe._

object Collection {
  private[scala] val SafetyTimeout = 1.second

  private[scala] def block[T](in: Future[T], timeout: Duration): Try[T] = {
    try {
      Try(Await.result(in, timeout + SafetyTimeout))
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
  * @define WithExpiration Couchbase documents optionally can have an expiration field set, e.g. when they will
  *                        automatically expire.  For efficiency reasons, by default the value of this expiration
  *                        field is not fetched upon getting a document.  If expiry is being used, then set this
  *                        field to true to ensure the expiration is fetched.  This will not only make it available
  *                        in the returned result, but also ensure that the expiry is available to use when mutating
  *                        the document, to avoid accidentally resetting the expiry to the default of 0.
  * @define Expiration     Couchbase documents optionally can have an expiration field set, e.g. when they will
  *                        automatically expire.  On mutations if this is left at the default (0), then any expiry
  *                        will be removed and the document will never expire.  If the application wants to preserve
  *                        expiration then they should use the `withExpiration` parameter on any gets, and provide
  *                        the returned expiration parameter to any mutations.
  * @define ParentSpan     this SDK supports the [[https://opentracing.io/ Open Tracing]] initiative, which is a way of
  *                        tracing complex distributed systems.  This field allows an OpenTracing parent span to be
  *                        provided, which will become the parent of any spans created by the SDK as a result of this
  *                        operation.  Note that if a span is not provided then the SDK will try to access any
  *                        thread-local parent span setup by a Scope.  Much of time this will `just work`, but it's
  *                        recommended to provide the parentSpan explicitly if possible, as thread-local is not a
  *                        100% reliable way of passing parameters.
  * @define Timeout        when the operation will timeout.  This will default to `timeoutConfig().kvTimeout()` in the
  *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy  provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define ErrorHandling  any [[scala.util.control.NonFatal]] error returned will derive ultimately from
  *                        [[com.couchbase.client.core.error.CouchbaseException]].  If the exception also derives from
  *                        [[com.couchbase.client.core.error.RetryableOperationException]]
  *                        then the failure was most likely temporary and may succeed if the application tries it
  *                        again.  (Though note that, in some cases, the operation may have in fact succeeded, and
  *                        the server was unable to report this to the SDK.  So the application should consider
  *                        carefully the result of reapplying the operation, and perhaps consider some more complex
  *                        error handling logic, possibly including the use of
  *                        [[Collection.getAllReplicas]]).  If the exception
  *                        does not derive from
  *                        [[com.couchbase.client.core.error.RetryableOperationException]]
  *                        then this is indicative of a more
  *                        permanent error or an application bug, that probably needs human review.
  * @define SupportedTypes this can be of any type for which an implicit
  *                        [[com.couchbase.client.scala.codec.Conversions.Encodable]] can be found: a list
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
                  bucketName: String)(

                  /** The implicit execution context upon which all `Future`s will be exexcuted.  As long as
                    * Couchbase resources are opened in the usual way through the methods in [[Cluster]], this will
                    * be provided automatically for you. */
                  implicit ec: ExecutionContext
                ) {
  /** Provides access to a reactive-programming version of this API. */
  val reactive = new ReactiveCollection(async)

  /** Provides access to less-commonly used methods. */
  val binary = new BinaryCollection(async.binary)

  private[scala] val kvTimeout = async.kvTimeout
  private[scala] val retryStrategy = async.retryStrategy

  private def block[T](in: Future[T], timeout: Duration) =
    Collection.block(in, timeout)

  /** Fetches a full document from this collection.
    *
    * @param id             $Id
    * @param withExpiration $WithExpiration
    * @param project        projection is an advanced feature allowing one or more fields to be fetched from a JSON
    *                       document, and the results
    *                       combined into a [[com.couchbase.client.scala.json.JsonObject]] result.  By default this
    *                       is set to None, meaning a normal full document fetch will be performed. For a full
    *                       description
    *                       of this feature see **CHANGEME**.
    * @param parentSpan     $ParentSpan
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def get(
           id: String,
           withExpiration: Boolean = false,
           project: Seq[String] = Seq.empty,
           parentSpan: Option[Span] = None,
           timeout: Duration = kvTimeout,
           retryStrategy: RetryStrategy = retryStrategy
         ): Try[GetResult] =
    block(
      async
        .get(id, withExpiration, project, parentSpan, timeout, retryStrategy),
      timeout
    )

  /** Inserts a full document into this collection, if it does not exist already.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param durability    $Durability
    * @param expiration    $Expiration
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentAlreadyExistsException]], indicating the document already exists.
    *         $ErrorHandling
    **/
  def insert[T](
                 id: String,
                 content: T,
                 durability: Durability = Disabled,
                 expiration: Duration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy
               )(implicit ev: Conversions.Encodable[T]): Try[MutationResult] =
    block(
      async.insert(
        id,
        content,
        durability,
        expiration,
        parentSpan,
        timeout,
        retryStrategy
      ),
      timeout
    )

  /** Replaces the contents of a full document in this collection, if it already exists.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param cas           $CAS
    * @param durability    $Durability
    * @param expiration    $Expiration
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found. $ErrorHandling
    **/
  def replace[T](
                  id: String,
                  content: T,
                  cas: Long = 0,
                  durability: Durability = Disabled,
                  expiration: Duration = 0.seconds,
                  parentSpan: Option[Span] = None,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = retryStrategy
                )(implicit ev: Conversions.Encodable[T]): Try[MutationResult] =
    block(
      async.replace(
        id,
        content,
        cas,
        durability,
        expiration,
        parentSpan,
        timeout,
        retryStrategy
      ),
      timeout
    )

  /** Upserts the contents of a full document in this collection.
    *
    * Upsert here means to insert the document if it does not exist, or replace the content if it does.
    *
    * @param id            $Id
    * @param content       $SupportedTypes
    * @param durability    $Durability
    * @param expiration    $Expiration
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  $ErrorHandling
    */
  def upsert[T](
                 id: String,
                 content: T,
                 durability: Durability = Disabled,
                 expiration: Duration = 0.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy
               )(implicit ev: Conversions.Encodable[T]): Try[MutationResult] =
    block(
      async.upsert(
        id,
        content,
        durability,
        expiration,
        parentSpan,
        timeout,
        retryStrategy
      ),
      timeout
    )

  /** Removes a document from this collection, if it exists.
    *
    * @param id            $Id
    * @param cas           $CAS
    * @param durability    $Durability
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found. $ErrorHandling
    **/
  def remove(
              id: String,
              cas: Long = 0,
              durability: Durability = Disabled,
              parentSpan: Option[Span] = None,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = retryStrategy
            ): Try[MutationResult] =
    block(
      async.remove(id, cas, durability, parentSpan, timeout, retryStrategy),
      timeout
    )

  /** SubDocument mutations allow modifying parts of a JSON document directly, which can be more efficiently than
    * fetching and modifying the full document.
    *
    * Mutations are all-or-nothing: if one fails, then no mutation will be performed.
    *
    * @param id            $Id
    * @param spec          a sequence of `MutateInSpec` specifying what mutations to apply to the document.  See
    *                      [[MutateInSpec]] for more details.
    * @param cas           $CAS
    * @param document      controls whether the document should be inserted, upserted, or not touched.  See
    *                      [[DocumentCreation]] for details.
    * @param durability    $Durability
    * @param expiration    $Expiration
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(MutateInResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found. $ErrorHandling
    **/
  def mutateIn(
                id: String,
                spec: Seq[MutateInSpec],
                cas: Long = 0,
                document: DocumentCreation = DocumentCreation.DoNothing,
                durability: Durability = Disabled,
                parentSpan: Option[Span] = None,
                expiration: Duration = 0.seconds,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = retryStrategy
              ): Try[MutateInResult] =
    block(
      async.mutateIn(
        id,
        spec,
        cas,
        document,
        durability,
        parentSpan,
        expiration,
        timeout,
        retryStrategy
      ),
      timeout
    )

  /** Fetches a full document from this collection, and simultaneously lock the document from writes.
    *
    * The CAS value returned in the [[GetResult]] is the document's 'key': during the locked period, the document
    * may only be modified by providing this CAS.
    *
    * @param id             $Id
    * @param lockFor        how long to lock the document for
    * @param parentSpan     $ParentSpan
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def getAndLock(id: String,
                 lockFor: Duration = 30.seconds,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy): Try[GetResult] =
    block(
      async.getAndLock(id, lockFor, parentSpan, timeout, retryStrategy),
      timeout
    )

  /** Unlock a locked document.
    *
    * @param id             $Id
    * @param cas            must match the CAS value return from a previous [[Collection.getAndLock]] to successfully
    *                       unlock the document
    * @param parentSpan     $ParentSpan
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a `Success(Unit)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def unlock(
              id: String,
              cas: Long,
              parentSpan: Option[Span] = None,
              timeout: Duration = kvTimeout,
              retryStrategy: RetryStrategy = retryStrategy
            ): Try[Unit] =
    block(async.unlock(id, cas, parentSpan, timeout, retryStrategy), timeout)

  /** Fetches a full document from this collection, and simultaneously update the expiry value of the document.
    *
    * @param id             $Id
    * @param expiration     $Expiration
    * @param durability     $Durability
    * @param parentSpan     $ParentSpan
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def getAndTouch(id: String,
                  expiration: Duration,
                  durability: Durability = Disabled,
                  parentSpan: Option[Span] = None,
                  timeout: Duration = kvTimeout,
                  retryStrategy: RetryStrategy = retryStrategy): Try[GetResult] =
    block(
      async.getAndTouch(
        id,
        expiration,
        durability,
        parentSpan,
        timeout,
        retryStrategy
      ),
      timeout
    )

  /** SubDocument lookups allow retrieving parts of a JSON document directly, which may be more efficient than
    * retrieving the entire document.
    *
    * Individual operations can succeed or fail without affecting the others.  See [[LookupInResult]] for details on
    * how to process the results.
    *
    * @param id            $Id
    * @param spec          a sequence of `LookupInSpec` specifying what fields to fetch.  See
    *                      [[LookupInSpec]] for more details.
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(LookupInResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found. $ErrorHandling
    **/
  def lookupIn(
                id: String,
                spec: Seq[LookupInSpec],
                parentSpan: Option[Span] = None,
                timeout: Duration = kvTimeout,
                retryStrategy: RetryStrategy = retryStrategy
              ): Try[LookupInResult] =
    block(async.lookupIn(id, spec, parentSpan, timeout, retryStrategy), timeout)

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
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found. $ErrorHandling
    **/
  def getAnyReplica(id: String,
                    parentSpan: Option[Span] = None,
                    timeout: Duration = kvTimeout,
                    retryStrategy: RetryStrategy = retryStrategy): Try[GetResult] =
    Try(reactive
      .getAnyReplica(id, parentSpan, timeout, retryStrategy)
      .block(Collection.SafetyTimeout + timeout))

  /** Retrieves all available versions of the document.
    *
    * The application should default to using [[Collection.get]] instead.  This method is intended for advanced scenarios,
    * including where a particular write has ambiguously failed (e.g. it may or may not have succeeded), and the
    * application wants to attempt manual verification and resolution.
    *
    * The returned `Iterable` will block on each call to `next` until the next replica has responded.
    *
    * @param id            $Id
    * @param parentSpan    $ParentSpan
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found. $ErrorHandling
    **/
  def getAllReplicas(id: String,
                     parentSpan: Option[Span] = None,
                     timeout: Duration = kvTimeout,
                     retryStrategy: RetryStrategy = retryStrategy): Iterable[GetResult] =
  // TODO make this skip failed replicas
    reactive.getAllReplicas(id, parentSpan, timeout, retryStrategy).toIterable()

  /** Checks if a document exists.
    *
    * This doesn't fetch the document so if the appplication simply needs to know if the document exists, this is the
    * most efficient method.
    *
    * @param id             $Id
    * @param parentSpan     $ParentSpan
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @return on success, a `Success(GetResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    **/
  def exists[T](
                 id: String,
                 parentSpan: Option[Span] = None,
                 timeout: Duration = kvTimeout,
                 retryStrategy: RetryStrategy = retryStrategy
               ): Try[ExistsResult] =
    block(async.exists(id, parentSpan, timeout, retryStrategy), timeout)
}


