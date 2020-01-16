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

import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.TimeoutUtil

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

/** Operations on non-JSON Couchbase documents.
  *
  * @param async an asynchronous version of this API
  * @param ec    an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *              opened in the normal way, starting from functions in [[Cluster]]
  *
  * @define CounterDoc      though it is common to use Couchbase to store exclusively JSON, Couchbase is actually
  *                         agnostic to what is stored.  It is possible to use a document as a 'counter' - e.g. it
  *                         stores an integer.  This is useful for use-cases such as implementing
  *                         AUTO_INCREMENT-style functionality, where each new document can be given a unique
  *                         monotonically increasing id.
  * @define OnlyBinary      this method should not be used with JSON documents.  This operates
  *                         at the byte level and is unsuitable for dealing with JSON documents. Use this method only
  *                         when explicitly dealing with binary or UTF-8 documents. It may invalidate an existing JSON
  *                         document.
  * @define OnlyCounter     this method should not be used with JSON documents.  Use this method only
  *                         when explicitly dealing with counter documents. It may invalidate an existing JSON
  *                         document.
  * @define Id              the unique identifier of the document
  * @define CAS             Couchbase documents all have a CAS (Compare-And-Set) field, a simple integer that allows
  *                         optimistic concurrency - e.g. it can detect if another agent has modified a document
  *                         in-between this agent getting and modifying the document.  See **CHANGEME** for a full
  *                        description.  The default is 0, which disables CAS checking.
  * @define Timeout         when the operation will timeout.  This will default to `timeoutConfig().kvTimeout()` in the
  *                         provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy   provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                         in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define ErrorHandling   any [[scala.util.control.NonFatal]] error returned will derive ultimately from
  *                         [[com.couchbase.client.core.error.CouchbaseException]].  If the exception also derives from
  *                         [[com.couchbase.client.core.error.RetryableOperationException]]
  *                         then the failure was most likely temporary and may succeed if the application tries it
  *                        again.  (Though note that, in some cases, the operation may have in fact succeeded, and
  *                         the server was unable to report this to the SDK.  So the application should consider
  *                         carefully the result of reapplying the operation, and perhaps consider some more complex
  *                         error handling logic, possibly including the use of
  *                         [[Collection.getAllReplicas]]).  If the exception
  *                         does not derive from
  *                         [[com.couchbase.client.core.error.RetryableOperationException]]
  *                         then this is indicative of a more
  *                         permanent error or an application bug, that probably needs human review.
  * @define Durability      writes in Couchbase are written to a single node, and from there the Couchbase Server will
  *                         take care of sending that mutation to any configured replicas.  This parameter provides
  *                         some control over ensuring the success of the mutation's replication.  See
  *                         [[com.couchbase.client.scala.durability.Durability]]
  *                         for a detailed discussion.
  * @author Graham Pople
  * @since 1.0.0
  */
class BinaryCollection(val async: AsyncBinaryCollection) {
  private[scala] implicit val ec: ExecutionContext     = async.ec
  private[scala] val kvTimeout: Durability => Duration = TimeoutUtil.kvTimeout(async.environment)
  private val environment                              = async.environment

  /** A reactive version of this API. */
  lazy val reactive = new ReactiveBinaryCollection(async)

  /** Add bytes to the end of a Couchbase binary document.
    *
    * $OnlyBinary
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[AppendOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param content       the bytes to append
    * @param cas           $CAS
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def append(
      id: String,
      content: Array[Byte],
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Try[MutationResult] = {
    Collection.block(
      async.append(id, content, cas, durability, timeout)
    )

  }

  /** Add bytes to the end of a Couchbase binary document.
    *
    * $OnlyBinary
    *
    * @param id            $Id
    * @param content         the bytes to append
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def append(
      id: String,
      content: Array[Byte],
      options: AppendOptions
  ): Try[MutationResult] = {
    Collection.block(
      async.append(id, content, options)
    )

  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * $OnlyBinary
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[PrependOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param content       the bytes to append
    * @param cas           $CAS
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def prepend(
      id: String,
      content: Array[Byte],
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Try[MutationResult] = {
    Collection.block(
      async.prepend(id, content, cas, durability, timeout)
    )
  }

  /** Add bytes to the beginning of a Couchbase binary document.
    *
    * $OnlyBinary
    *
    * @param id            $Id
    * @param content       the bytes to append
    * @param options       $Options
    *
    * @return on success, a `Success(MutationResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def prepend(
      id: String,
      content: Array[Byte],
      options: PrependOptions
  ): Try[MutationResult] = {
    Collection.block(
      async.prepend(id, content, options)
    )
  }

  /** Increment a Couchbase 'counter' document.  $CounterDoc
    *
    * $OnlyCounter
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[IncrementOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param delta         the amount to increment by
    * @param initial       if not-None, the amount to initialise the document too, if it does not exist.  If this is
    *                      not set, and the document does not exist, Failure(DocumentDoesNotExistException) will be
    *                      returned
    * @param cas           $CAS
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(CounterResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def increment(
      id: String,
      delta: Long,
      initial: Option[Long] = None,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Try[CounterResult] = {
    Collection.block(
      async.increment(
        id,
        delta,
        initial,
        cas,
        durability,
        timeout
      )
    )
  }

  /** Increment a Couchbase 'counter' document.  $CounterDoc
    *
    * $OnlyCounter
    *
    * @param id            $Id
    * @param delta         the amount to increment by
    * @param options       $Options
    *
    * @return on success, a `Success(CounterResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def increment(
      id: String,
      delta: Long,
      options: IncrementOptions
  ): Try[CounterResult] = {
    Collection.block(
      async.increment(
        id,
        delta,
        options
      )
    )
  }

  /** Decrement a Couchbase 'counter' document.  $CounterDoc
    *
    * $OnlyCounter
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes an [[DecrementOptions]] instead, which supports all available options.
    *
    * @param id            $Id
    * @param delta         the amount to decrement by, which should be a positive amount
    * @param initial       if not-None, the amount to initialise the document too, if it does not exist.  If this is
    *                      not set, and the document does not exist, Failure(DocumentDoesNotExistException) will be
    *                      returned
    * @param cas           $CAS
    * @param durability    $Durability
    * @param timeout       $Timeout
    *
    * @return on success, a `Success(CounterResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def decrement(
      id: String,
      delta: Long,
      initial: Option[Long] = None,
      cas: Long = 0,
      durability: Durability = Disabled,
      timeout: Duration = Duration.MinusInf
  ): Try[CounterResult] = {
    Collection.block(
      async.decrement(
        id,
        delta,
        initial,
        cas,
        durability,
        timeout
      )
    )
  }

  /** Decrement a Couchbase 'counter' document.  $CounterDoc
    *
    * $OnlyCounter
    *
    * @param id            $Id
    * @param delta         the amount to decrement by, which should be a positive amount
    * @param options       $Options
    *
    * @return on success, a `Success(CounterResult)`, else a `Failure(CouchbaseException)`.  This could be [[com
    *         .couchbase.client.core.error.DocumentDoesNotExistException]], indicating the document could not be
    *         found.  $ErrorHandling
    * */
  def decrement(
      id: String,
      delta: Long,
      options: DecrementOptions
  ): Try[CounterResult] = {
    Collection.block(
      async.decrement(
        id,
        delta,
        options
      )
    )
  }

}
