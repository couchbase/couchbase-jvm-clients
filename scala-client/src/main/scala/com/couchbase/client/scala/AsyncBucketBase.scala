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

import com.couchbase.client.core.Core
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.diagnostics.{HealthPinger, PingResult}
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.scala.diagnostics.{PingOptions, WaitUntilReadyOptions}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions

import java.util.Optional
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

trait AsyncBucketBase { this: AsyncBucket =>

  private[scala] implicit val ec: ExecutionContext = environment.ec
  
  /** Opens and returns a Couchbase scope resource.
    *
    * @param scopeName the name of the scope
    */
  def scope(scopeName: String): AsyncScope = {
    new AsyncScope(scopeName, name, couchbaseOps, environment)
  }

  /** Opens and returns the default Couchbase scope. */
  def defaultScope: AsyncScope = {
    scope(DefaultResources.DefaultScope)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: AsyncCollection = {
    scope(DefaultResources.DefaultScope).defaultCollection
  }

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collectionName the name of the collection
    *
    * @return a created collection resource
    */
  def collection(collectionName: String): AsyncCollection = {
    scope(DefaultResources.DefaultScope).collection(collectionName)
  }
} 
