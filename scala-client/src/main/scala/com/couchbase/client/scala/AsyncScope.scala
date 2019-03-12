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

import java.time.Duration

import com.couchbase.client.core.Core
import com.couchbase.client.core.msg.kv.GetCollectionIdRequest
import com.couchbase.client.scala.env.ClusterEnvironment

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

// TODO get ScalaDocs using JavaDoc's CSS style

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
                 private val environment: ClusterEnvironment)
                (implicit ec: ExecutionContext) {

  /** The name of this scope. */
  def name = scopeName

  /** Opens and returns the default collection on this scope. */
  def defaultCollection: Future[AsyncCollection] = collection(DefaultResources.DefaultCollection)

  /** Opens and returns a Couchbase collection resource, that exists on this scope. */
  def collection(name: String): Future[AsyncCollection] = {
    if (name == DefaultResources.DefaultCollection && scopeName == DefaultResources.DefaultScope) {
      Future {
        new AsyncCollection(name, DefaultResources.DefaultCollectionId, bucketName, core, environment)
      }
    }
    else {
      val request = new GetCollectionIdRequest(Duration.ofSeconds(1),
        core.context(), bucketName, environment.retryStrategy(), scopeName, name)
      core.send(request)
      FutureConverters.toScala(request.response())
        .map(res => {
          if (res.status().success()) {
            new AsyncCollection(name, res.collectionId().get(), bucketName, core, environment)
          } else {
            // TODO BLOCKED collection opening is going to be redone later
            throw new IllegalStateException("Do not raise me.. propagate into collection.. " + "collection error")
          }
        })

    }
  }
}
