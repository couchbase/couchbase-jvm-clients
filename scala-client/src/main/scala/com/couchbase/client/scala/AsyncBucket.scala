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
import com.couchbase.client.scala.env.ClusterEnvironment

import scala.concurrent.{ExecutionContext, Future}

/** Represents a Couchbase bucket resource.
  *
  * This is the asynchronous version of the [[Bucket]] API.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param name the name of this bucket
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  */
class AsyncBucket(val name: String,
                  private[scala] val core: Core,
                  private[scala] val environment: ClusterEnvironment)
                 (implicit ec: ExecutionContext) {
  /** Opens and returns a Couchbase scope resource.
    *
    * @param name the name of the scope
    */
  def scope(scope: String): Future[AsyncScope] = {
    Future {
      new AsyncScope(scope, name, core, environment)
    }
  }

  /** Opens and returns the default Couchbase scope. */
  def defaultScope: Future[AsyncScope] = {
    scope(DefaultResources.DefaultScope)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: Future[AsyncCollection] = {
    scope(DefaultResources.DefaultScope).flatMap(_.defaultCollection)
  }

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collection the name of the collection
    * @return a created collection resource
    */
  def collection(collection: String): Future[AsyncCollection] = {
    scope(DefaultResources.DefaultScope).flatMap(_.collection(collection))
  }
}



