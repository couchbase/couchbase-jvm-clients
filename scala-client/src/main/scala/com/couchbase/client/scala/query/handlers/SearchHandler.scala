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
import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.result.{SearchMeta, SearchMetrics, SearchStatus}
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

  def request[T](query: SearchQuery,
                 timeout: Duration,
                 retryStrategy: RetryStrategy,
                 core: Core, environment: ClusterEnvironment)
  : Try[SearchRequest] = {

    val validations: Try[SearchRequest] = for {
      _ <- Validate.notNull(query, "query")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
    } yield null

    if (validations.isFailure) {
      validations
    }
    else if (query.deferredError.isDefined) {
      Failure(query.deferredError.get)
    }
    else {
      val params = query.export()

      val queryBytes = params.toString.getBytes(CharsetUtil.UTF_8)

      Try(new SearchRequest(timeout,
        core.context(),
        retryStrategy,
        core.context().authenticator(),
        query.indexName,
        queryBytes))
    }
  }
}

object SearchHandler {
  private[scala] def parseSearchMeta(response: SearchResponse, trailer: SearchChunkTrailer) = {
    val rawStatus = response.header.getStatus
    val status = SearchStatus.fromBytes(rawStatus)
    val metrics = SearchMetrics(Duration.fromNanos(trailer.took()), trailer.totalRows(), trailer.maxScore())
    val meta = SearchMeta(status, metrics)
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