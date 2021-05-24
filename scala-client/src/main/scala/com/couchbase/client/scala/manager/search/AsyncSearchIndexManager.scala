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

import java.nio.charset.StandardCharsets
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.deps.io.netty.handler.codec.http._
import com.couchbase.client.core.endpoint.http.{CoreCommonOptions, CoreHttpResponse}
import com.couchbase.client.core.endpoint.http.CoreHttpPath.path
import com.couchbase.client.core.msg.RequestTarget
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.AsyncCluster
import com.couchbase.client.scala.util.CouchbasePickler
import com.couchbase.client.scala.util.DurationConversions._

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

@Stability.Volatile
class AsyncSearchIndexManager(private[scala] val cluster: AsyncCluster)(
    implicit val ec: ExecutionContext
) {
  private val core = cluster.core
  private val DefaultTimeout: Duration =
    core.context().environment().timeoutConfig().managementTimeout()
  private val DefaultRetryStrategy: RetryStrategy = core.context().environment().retryStrategy()
  private val httpClient                          = core.httpClient(RequestTarget.search())

  def getIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[SearchIndex] = {
    val options = CoreCommonOptions.of(timeout, retryStrategy, null)
    val request = httpClient.get(path(indexPath(indexName)), options).build()
    core.send(request)
    val out = request.response.toScala
      .map((response: CoreHttpResponse) => {
        val read = CouchbasePickler.read[SearchIndexWrapper](response.content())
        read.indexDef.copy(numPlanPIndexes = read.numPlanPIndexes)
      })
    out.onComplete(_ => request.context.logicallyComplete())
    out
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Seq[SearchIndex]] = {
    val options = CoreCommonOptions.of(timeout, retryStrategy, null)
    val request = httpClient.get(path(indexesPath), options).build()

    core.send(request)
    val out = request.response.toScala
      .map((response: CoreHttpResponse) => {
        AsyncSearchIndexManager.parseIndexes(response.content())
      })
    out.onComplete(_ => request.context.logicallyComplete())
    out
  }

  def upsertIndex(
      indexDefinition: SearchIndex,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    val options = CoreCommonOptions.of(timeout, retryStrategy, null)
    val request = httpClient
      .put(path(indexPath(indexDefinition.name)), options)
      .header(HttpHeaderNames.CACHE_CONTROL, "no-cache")
      .json(indexDefinition.toJson.getBytes(StandardCharsets.UTF_8))
      .build()

    core.send(request)
    val out = request.response.toScala
    out.onComplete(_ => request.context.logicallyComplete())
    out.map(_ => ())
  }

  def dropIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    val options = CoreCommonOptions.of(timeout, retryStrategy, null)
    val request = httpClient.delete(path(indexPath(indexName)), options).build()

    core.send(request)
    val out = request.response.toScala
    out.onComplete(_ => request.context.logicallyComplete())
    out
      .map(_ => ())
  }

  private def indexesPath = "/api/index"

  private def indexPath(indexName: String) = indexesPath + "/" + urlEncode(indexName)

  private def indexCountPath(indexName: String) = indexPath(indexName) + "/count"

}

object AsyncSearchIndexManager {
  // This can throw, so should be called inside a Future operator
  private[scala] def parseIndexes(in: Array[Byte]): Seq[SearchIndex] = {
    val json                             = CouchbasePickler.read[ujson.Obj](in)
    val indexDefs                        = json.obj("indexDefs")
    val allIndexes: SearchIndexesWrapper = CouchbasePickler.read[SearchIndexesWrapper](indexDefs)
    allIndexes.indexDefs.values.toSeq
  }
}
