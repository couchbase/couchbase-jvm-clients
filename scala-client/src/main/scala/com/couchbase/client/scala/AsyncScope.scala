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

import com.couchbase.client.core.Core
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.scala.env.ClusterEnvironment

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

/** Represents a Couchbase scope resource.
  *
  * This is an asynchronous version of the [[Scope]] interface.
  *
  * Applications should not create these manually, but instead first open a [[Cluster]] and through that a
  * [[Bucket]].
  *
  * @param bucketName the name of the bucket this scope is on
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  * @author Graham Pople
  * @since 1.0.0
  */
class AsyncScope private[scala] (scopeName: String,
                 bucketName: String,
                 private val core: Core,
                 private[scala] val environment: ClusterEnvironment) {
  private[scala] implicit val ec: ExecutionContext = environment.ec

  /** The name of this scope. */
  def name = scopeName

  /** Opens and returns the default collection on this scope. */
  private[scala] def defaultCollection: Future[AsyncCollection] = collection(DefaultResources.DefaultCollection)

  /** Opens and returns a Couchbase collection resource, that exists on this scope. */
  def collection(name: String): Future[AsyncCollection] = {
    if (name == CollectionIdentifier.DEFAULT_COLLECTION && scopeName == CollectionIdentifier.DEFAULT_SCOPE) {
      Future {
        new AsyncCollection(name, bucketName, scopeName, core, environment)
      }
    }
    else {
      FutureConverters
        .toScala(core.configurationProvider().refreshCollectionMap(bucketName, false).toFuture)
        .map(_ => new AsyncCollection(name, bucketName, scopeName, core, environment))
    }
  }
}
