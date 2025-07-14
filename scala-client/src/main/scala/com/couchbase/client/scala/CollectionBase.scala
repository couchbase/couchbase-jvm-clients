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
import com.couchbase.client.core.api.kv.CoreExpiry
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.util.CoreCommonConverters.{
  convert,
  convertExpiry,
  encoder,
  makeCommonOptions
}
import com.couchbase.client.scala.util.{AsyncUtils, ExpiryUtil, TimeoutUtil}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/** Provides blocking, synchronous access to all collection APIs.  This is the main entry-point for key-value (KV)
  * operations.
  *
  * If asynchronous access is needed, we recommend looking at the [[AsyncCollection]] which is built around
  * returning `Future`s, or the [[ReactiveCollection]] which provides a reactive programming API.
  *
  * This blocking API itself is just a small layer on top of the [[AsyncCollection]] which blocks the current thread
  * until the request completes with a response.
  *
  * @author Graham Pople
  * @since 1.0.0
  * @define Id             the unique identifier of the document
  * @define CAS            Couchbase documents all have a CAS (Compare-And-Set) field, a simple integer that allows
  *                        optimistic concurrency - e.g. it can detect if another agent has modified a document
  *                        in-between this agent getting and modifying the document.  The default is 0, which disables
  *                        CAS checking.
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
  *                        `com.couchbase.client.core.error.CouchbaseException`.  See
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/error-handling.html the error handling docs]]
  *                        for more detail.
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.Conversions.JsonSerializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
  * @define Durability     writes in Couchbase are written to a single node, and from there the Couchbase Server will
  *                        take care of sending that mutation to any configured replicas.  This parameter provides
  *                        some control over ensuring the success of the mutation's replication.  See
  *                        [[com.couchbase.client.scala.durability.Durability]]
  *                        for a detailed discussion.
  * @define ParentSpan     an optional parent 'span' for the request, allowing tracing requests through the full
  *                        distributed system
  * @define Options        configure options that affect this operation
  */
trait CollectionBase { this: Collection =>
  private[scala] implicit val ec: ExecutionContext = async.ec

  def name: String      = async.name
  def scopeName: String = async.scopeName

  /** Provides access to less-commonly used methods. */
  val binary = new BinaryCollection(async.binary)

  private[scala] val kvTimeout: Durability => Duration = TimeoutUtil.kvTimeout(async.environment)
  private[scala] val kvReadTimeout: Duration           = async.kvReadTimeout
  private[scala] val kvOps                             = async.kvOps
  private[scala] lazy val collectionIdentifier         =
    new CollectionIdentifier(bucketName, Some(scopeName).asJava, Some(name).asJava)

  private[scala] def block[T](in: Future[T]) =
    AsyncUtils.block(in)
}
