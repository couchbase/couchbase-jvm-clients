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

import java.util.UUID

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.diagnostics.{ClusterState, HealthPinger, PingResult}
import com.couchbase.client.core.retry.{FailFastRetryStrategy, RetryStrategy}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.diagnostics.{PingOptions, WaitUntilReadyOptions}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions, TimeoutUtil}
import com.couchbase.client.scala.util.DurationConversions.{
  javaDurationToScala,
  scalaDurationToJava
}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

/** Represents a Couchbase bucket resource.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param async provides an asynchronous version of this interface
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  * @author Graham Pople
  * @since 1.0.0
  */
trait BucketBase { this: Bucket =>
  private[scala] implicit val ec: ExecutionContext = async.ec

  /** Returns the name of this bucket. */
  def name: String = async.name

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collectionName the name of the collection
    * @return a created collection resource
    */
  def collection(collectionName: String): Collection = {
    scope(DefaultResources.DefaultScope).collection(collectionName)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: Collection = {
    scope(DefaultResources.DefaultScope).defaultCollection
  }

  /** Opens and returns a Couchbase scope resource.
    *
    * @param scopeName the name of the scope
    */
  def scope(scopeName: String): Scope = {
    new Scope(async.scope(scopeName), async.name)
  }

  /** Opens and returns the default Couchbase scope. */
  def defaultScope: Scope = {
    scope(DefaultResources.DefaultScope)
  }
}
