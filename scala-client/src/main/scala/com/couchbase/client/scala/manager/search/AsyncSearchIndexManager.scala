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
package com.couchbase.client.scala.manager.search

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

@Stability.Volatile
class AsyncSearchIndexManager(private[scala] val couchbaseOps: CoreCouchbaseOps)(
    implicit val ec: ExecutionContext
) {
  private val internal = couchbaseOps.clusterSearchIndexManager()
  private[scala] val DefaultTimeout: Duration =
    couchbaseOps.asCore().context().environment().timeoutConfig().managementTimeout()
  private[scala] val DefaultRetryStrategy: RetryStrategy =
    couchbaseOps.asCore().context().environment().retryStrategy()

  def getIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[SearchIndex] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.getIndex(indexName, CoreCommonOptions.of(timeout, retryStrategy, null))
      )
      .map(result => convert(result))
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Seq[SearchIndex]] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.getAllIndexes(CoreCommonOptions.of(timeout, retryStrategy, null))
      )
      .map(result => result.asScala.map(v => convert(v)))
  }

  def upsertIndex(
      indexDefinition: SearchIndex,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal
          .upsertIndex(convert(indexDefinition), CoreCommonOptions.of(timeout, retryStrategy, null))
      )
      .map(_ => ())
  }

  def dropIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.dropIndex(indexName, CoreCommonOptions.of(timeout, retryStrategy, null))
      )
      .map(_ => ())
  }
}
