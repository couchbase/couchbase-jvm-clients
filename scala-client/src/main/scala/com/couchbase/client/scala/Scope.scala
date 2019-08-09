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

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext

/** Represents a Couchbase scope resource.
  *
  * Applications should not create these manually, but instead first open a [[Cluster]] and through that a
  * [[Bucket]].
  *
  * @param async an asynchronous version of this API
  * @param bucketName the name of the bucket this scope is on
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  * @author Graham Pople
  * @since 1.0.0
  */
@Volatile
class Scope private[scala] (val async: AsyncScope,
            bucketName: String) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  /** Access a Reactive version of this API. */
  lazy val reactive: ReactiveScope = new ReactiveScope(async, bucketName)

  /** The name of this scope. */
  def name = async.name

  /** Opens and returns a Couchbase collection resource, that exists on this scope. */
  def collection(name: String): Collection = {
    AsyncUtils.block(async.collection(name))
      .map(asyncCollection => new Collection(asyncCollection, bucketName))
      .get
  }

  /** Opens and returns the default collection on this scope. */
  private[scala] def defaultCollection = {
    collection(DefaultResources.DefaultCollection)
  }
}

