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
import com.couchbase.client.core.msg.search.{SearchChunkTrailer, SearchRequest, SearchResponse}
import com.couchbase.client.scala.HandlerBasicParams
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.json.JsonObjectSafe
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchFacetResult._
import com.couchbase.client.scala.search.result.{SearchFacetResult, SearchMetaData, SearchMetrics}
import com.couchbase.client.scala.util.{CouchbasePickler, DurationConversions, Validate}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * Handles requests and responses for search operations.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] class SearchHandler(hp: HandlerBasicParams) {

  import DurationConversions._

  def request[T](
      indexName: String,
      query: SearchQuery,
      options: SearchOptions,
      core: Core,
      environment: ClusterEnvironment
  ): Try[SearchRequest] = {

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
      _ <- Validate.optNotNull(options.parentSpan, "parentSpan")
    } yield null

    if (validations.isFailure) {
      validations
    } else if (options.deferredError.isDefined) {
      Failure(options.deferredError.get)
    } else {
      val params = options.export(indexName, query)

      val queryBytes = params.toString.getBytes(CharsetUtil.UTF_8)

      val timeout: Duration = options.timeout.getOrElse(environment.timeoutConfig.searchTimeout)
      val retryStrategy     = options.retryStrategy.getOrElse(environment.retryStrategy)

      Try(
        new SearchRequest(
          timeout,
          core.context(),
          retryStrategy,
          core.context().authenticator(),
          indexName,
          queryBytes,
          hp.tracer.internalSpan(SearchRequest.OPERATION_NAME, options.parentSpan.orNull)
        )
      )
    }
  }
}

object SearchHandler {
  private[scala] def parseSearchMeta(
      response: SearchResponse,
      trailer: SearchChunkTrailer
  ): SearchMetaData = {
    val meta: Try[SearchMetaData] = for {
      status                <- JsonObjectSafe.fromJson(new String(response.header.getStatus, CharsetUtil.UTF_8))
      totalPartitionCount   <- status.numLong("total")
      successPartitionCount <- status.numLong("successful")
      errorPartitionCount   <- status.numLong("failed")
      errors                <- status.obj("errors")

      metrics <- Try(
        SearchMetrics(
          Duration.fromNanos(trailer.took),
          trailer.totalRows,
          trailer.maxScore,
          totalPartitionCount,
          successPartitionCount,
          errorPartitionCount
        )
      )

      meta <- Try(
        SearchMetaData(
          metrics,
          errors.toMap.asInstanceOf[collection.Map[String, String]]
        )
      )
    } yield meta

    meta match {
      case Success(m) => m
      case _ =>
        SearchMetaData(
          SearchMetrics(Duration.Zero, 0, 0, 0, 0, 0),
          Map.empty[String, String]
        )
    }
  }

  private[scala] def parseSearchFacets(
      trailer: SearchChunkTrailer
  ): Map[String, SearchFacetResult] = {
    val rawFacets = trailer.facets()
    if (rawFacets == null || rawFacets.isEmpty) {
      Map.empty
    } else {

      val tree: ujson.Value = ujson.read(rawFacets)
      val facets            = collection.mutable.Map.empty[String, SearchFacetResult]

      tree match {
        case x: ujson.Obj =>
          x.value.foreach(kv => {
            val key        = kv._1
            val entry      = kv._2
            val facetEntry = entry.asInstanceOf[ujson.Obj]

            val field   = facetEntry.value("field").str
            val total   = facetEntry.value("total").num.longValue()
            val missing = facetEntry.value("missing").num.longValue()
            val other   = facetEntry.value("other").num.longValue()

            facetEntry.value.get("numeric_ranges") match {
              case Some(tr) =>
                val ranges = CouchbasePickler.read[Seq[NumericRange]](tr)
                val result =
                  NumericRangeSearchFacetResult(key, field, total, missing, other, ranges)
                facets += key -> result
              case _ =>
                facetEntry.value.get("date_ranges") match {
                  case Some(tr) =>
                    val ranges = CouchbasePickler.read[Seq[DateRange]](tr)
                    val result =
                      DateRangeSearchFacetResult(key, field, total, missing, other, ranges)
                    facets += key -> result
                  case _ =>
                    val terms = facetEntry.value.get("terms") match {
                      case Some(tr) => CouchbasePickler.read[Seq[TermRange]](tr)
                      case _        => Seq.empty
                    }
                    val result = TermSearchFacetResult(key, field, total, missing, other, terms)
                    facets += key -> result
                }
            }
          })
        case _ =>
      }
      facets.toMap
    }
  }
}
