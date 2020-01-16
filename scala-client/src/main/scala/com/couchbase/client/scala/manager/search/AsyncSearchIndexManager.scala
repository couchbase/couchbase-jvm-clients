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
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled
import com.couchbase.client.core.deps.io.netty.handler.codec.http._
import com.couchbase.client.core.error.IndexNotFoundException
import com.couchbase.client.core.msg.search.{GenericSearchRequest, GenericSearchResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.AsyncCluster
import com.couchbase.client.scala.util.CouchbasePickler
import com.couchbase.client.scala.util.DurationConversions._

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@Stability.Volatile
class AsyncSearchIndexManager(private[scala] val cluster: AsyncCluster)(
    implicit val ec: ExecutionContext
) {
  private val core = cluster.core
  private val DefaultTimeout: Duration =
    core.context().environment().timeoutConfig().managementTimeout()
  private val DefaultRetryStrategy: RetryStrategy = core.context().environment().retryStrategy()

  /** Maps any raw errors into more useful ones. */
  private def transformer(
      indexName: String
  ): Throwable => Throwable =
    err =>
      if (err.getMessage.contains("index not found")) {
        new IndexNotFoundException(indexName)
      } else err

  def getIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[SearchIndex] = {
    val request = getIndexRequest(indexName, timeout, retryStrategy)
    core.send(request)
    val out = request.response.toScala
      .map((response: GenericSearchResponse) => {
        val read = CouchbasePickler.read[SearchIndexWrapper](response.content())
        read.indexDef.copy(numPlanPIndexes = read.numPlanPIndexes)
      })
      .transform(identity, transformer(indexName))
    out.onComplete(_ => request.context.logicallyComplete())
    out
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Seq[SearchIndex]] = {
    val request = searchRequest(HttpMethod.GET, indexesPath, timeout, retryStrategy)

    core.send(request)
    val out = request.response.toScala
      .map((response: GenericSearchResponse) => {
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
    val req: DefaultFullHttpRequest = {
      val payload = Unpooled.wrappedBuffer(indexDefinition.toJson.getBytes(StandardCharsets.UTF_8))
      val request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.PUT,
        indexPath(indexDefinition.name),
        payload
      )
      request.headers.set(HttpHeaderNames.CACHE_CONTROL, "no-cache")
      request.headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
      request.headers.set(HttpHeaderNames.CONTENT_LENGTH, payload.readableBytes)
      request
    }
    val request = searchRequest(req, idempotent = false, timeout, retryStrategy)

    core.send(request)
    val out = request.response.toScala
      .transform(identity, transformer(indexDefinition.name))
    out.onComplete(_ => request.context.logicallyComplete())
    out.map(_ => ())
  }

  def dropIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    val request = searchRequest(HttpMethod.DELETE, indexPath(indexName), timeout, retryStrategy)

    core.send(request)
    val out = request.response.toScala
    out.onComplete(_ => request.context.logicallyComplete())
    out
      .transform(identity, transformer(indexName))
      .map(_ => ())
  }

  private def indexesPath = "/api/index"

  private def indexPath(indexName: String) = indexesPath + "/" + urlEncode(indexName)

  private def indexCountPath(indexName: String) = indexPath(indexName) + "/count"

  private def getIndexRequest(
      name: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): GenericSearchRequest = {
    searchRequest(HttpMethod.GET, indexPath(name), timeout, retryStrategy)
  }

  private def searchRequest(
      method: HttpMethod,
      path: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): GenericSearchRequest = {
    searchRequest(
      new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path),
      method == HttpMethod.GET,
      timeout,
      retryStrategy
    )
  }

  private def searchRequest(
      httpRequest: => FullHttpRequest,
      idempotent: Boolean,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): GenericSearchRequest = {
    new GenericSearchRequest(timeout, core.context, retryStrategy, () => httpRequest, idempotent)
  }

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
