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

package com.couchbase.client.scala.query.handlers

import com.couchbase.client.core.Core
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.msg.analytics.AnalyticsRequest
import com.couchbase.client.core.msg.search.{SearchChunkTrailer, SearchRequest, SearchResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.{JsonArray, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.{FacetResult, SearchMetaData, SearchMetrics, SearchStatus}
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.{DurationConversions, Validate}

import scala.collection.GenSeq
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * Handles requests and responses for search operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class SearchHandler() {

  import DurationConversions._

  def request[T](indexName: String,
                 query: SearchQuery,
                 options: SearchOptions,
                 core: Core,
                 environment: ClusterEnvironment)
  : Try[SearchRequest] = {

    val validations: Try[SearchRequest] = for {
      _ <- Validate.notNull(query, "query")
      _ <- Validate.notNull(options.limit, "limit")
      _ <- Validate.notNull(options.skip, "skip")
      _ <- Validate.notNull(options.explain, "explain")
      _ <- Validate.notNull(options.highlightStyle, "highlightStyle")
      _ <- Validate.notNull(options.highlightFields, "highlightFields")
      _ <- Validate.notNull(options.fields, "fields")
      _ <- Validate.notNull(options.sort, "sort")
      _ <- Validate.notNull(options.facets, "facets")
      _ <- Validate.notNull(options.serverSideTimeout, "serverSideTimeout")
      _ <- Validate.notNull(options.deferredError, "deferredError")
      _ <- Validate.notNull(options.scanConsistency, "scanConsistency")
      _ <- Validate.notNull(options.timeout, "timeout")
      _ <- Validate.notNull(options.retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else if (options.deferredError.isDefined) {
      Failure(options.deferredError.get)
    }
    else {
      val params = options.export(indexName, query)

      val queryBytes = params.toString.getBytes(CharsetUtil.UTF_8)

      val timeout: Duration = options.timeout.getOrElse(environment.timeoutConfig.searchTimeout)
      val retryStrategy = options.retryStrategy.getOrElse(environment.retryStrategy)

      Try(new SearchRequest(timeout,
        core.context(),
        retryStrategy,
        core.context().authenticator(),
        indexName,
        queryBytes))
    }
  }
}

object SearchHandler {
  private[scala] def parseSearchMeta(response: SearchResponse, trailer: SearchChunkTrailer): SearchMetaData = {
    val rawStatus = response.header.getStatus
    val status = SearchStatus.fromBytes(rawStatus)
    val metrics = SearchMetrics(Duration.fromNanos(trailer.took()), trailer.totalRows(), trailer.maxScore())
    val meta = SearchMetaData(status, metrics)
    meta
  }

  private[scala] def parseSearchErrors(status: Array[Byte]): Seq[RuntimeException] = {
    val jsonStatus = JacksonTransformers.MAPPER.readValue(status, classOf[JsonObject])
    val errorsRaw = jsonStatus.safe.get("errors")
    errorsRaw match {
      case Success(errorsJson: JsonArray) => errorsJson.toSeq.map(v => new RuntimeException(v.toString))
      case Success(errorsJson: JsonObject) =>
        errorsJson.names.toSeq.seq.map(key => new RuntimeException(key + ": " + errorsJson.get(key)))
      case _ => Seq(new RuntimeException("Server error: errors field returned, but contained no errors"))
    }
  }
}