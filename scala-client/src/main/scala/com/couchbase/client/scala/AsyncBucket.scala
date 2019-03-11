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

class AsyncBucket(val name: String,
                  private[scala] val core: Core,
                  private[scala] val environment: ClusterEnvironment)
                 (implicit ec: ExecutionContext) {
  def scope(scope: String): Future[AsyncScope] = {
    Future {
      new AsyncScope(scope, name, core, environment)
    }
  }

  def defaultCollection: Future[AsyncCollection] = {
    scope(Defaults.DefaultScope).flatMap(_.defaultCollection)
  }

  def collection(collection: String): Future[AsyncCollection] = {
    scope(Defaults.DefaultScope).flatMap(_.collection(collection))
  }
}



