/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.performer.scala.search

// [skip:<1.2.4]

import com.couchbase.client.core.retry.BestEffortRetryStrategy
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.{
  convertMutationState,
  setSuccess
}
import com.couchbase.client.performer.scala.util.ContentAsUtil
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.search.MatchOperator.{
  SEARCH_MATCH_OPERATOR_AND,
  SEARCH_MATCH_OPERATOR_OR
}
import com.couchbase.client.protocol.sdk.search.SearchGeoDistanceUnits
import com.couchbase.client.protocol.sdk.search.SearchScanConsistency.SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED
import com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndexes
import com.couchbase.client.protocol.shared.ContentAs
import com.couchbase.client.scala.{Cluster, Scope}
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.manager.search.SearchIndex
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.facet.SearchFacet.{
  DateRange,
  DateRangeFacet,
  NumericRange,
  NumericRangeFacet,
  TermFacet
}
import com.couchbase.client.scala.search.queries.{MatchOperator, SearchQuery}
import com.couchbase.client.scala.search.result.{
  SearchFacetResult,
  SearchMetaData,
  SearchResult,
  SearchRow
}
import com.couchbase.client.scala.search.sort.{
  FieldSortMissing,
  FieldSortMode,
  FieldSortType,
  SearchSort
}
import com.couchbase.client.scala.search.{HighlightStyle, SearchOptions, SearchScanConsistency}
import com.google.protobuf.{ByteString, Timestamp}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object SearchHelper {
  // Have to hardcode these.  The earliest versions of the Scala SDK do not give access to the environment.
  private val DefaultManagementTimeout = Duration(75, TimeUnit.SECONDS)
  private val DefaultRetryStrategy     = BestEffortRetryStrategy.INSTANCE

  private[search] def convertSearchOptions(
      hasOptions: Boolean,
      o: com.couchbase.client.protocol.sdk.search.SearchOptions
  ): Option[SearchOptions] = {
    if (!hasOptions) return None
    var opts = SearchOptions()
    if (o.hasLimit) opts = opts.limit(o.getLimit)
    if (o.hasSkip) opts = opts.skip(o.getSkip)
    if (o.hasExplain) opts = opts.explain(o.getExplain)
    if (o.hasHighlight) {
      val style: Option[HighlightStyle] = if (o.getHighlight.hasStyle) {
        Some(o.getHighlight.getStyle match {
          case com.couchbase.client.protocol.sdk.search.HighlightStyle.HIGHLIGHT_STYLE_ANSI =>
            HighlightStyle.ANSI
          case com.couchbase.client.protocol.sdk.search.HighlightStyle.HIGHLIGHT_STYLE_HTML =>
            HighlightStyle.HTML
          case _ => throw new UnsupportedOperationException("Bad FTS style")
        })
      } else None
      val fields: Option[Seq[String]] =
        if (o.getHighlight.getFieldsCount > 0) Some(o.getHighlight.getFieldsList.asScala)
        else None
      opts = opts.highlight(style, fields)
    }
    if (o.getFieldsCount > 0) opts = opts.fields(o.getFieldsList.toArray(new Array[String](0)))
    if (o.hasScanConsistency)
      if (o.getScanConsistency == SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED)
        opts = opts.scanConsistency(SearchScanConsistency.NotBounded)
      else throw new UnsupportedOperationException("FTS consistency")
    if (o.hasConsistentWith)
      opts = opts.scanConsistency(
        SearchScanConsistency.ConsistentWith(convertMutationState(o.getConsistentWith))
      )
    if (o.getSortCount > 0)
      opts = opts.sort(
        o.getSortList.asScala
          .map(sort => {
            if (sort.hasField) {
              val ss = sort.getField
              SearchSort.FieldSort(
                ss.getField,
                if (ss.hasType) Some(ss.getType match {
                  case "auto"   => FieldSortType.Auto
                  case "date"   => FieldSortType.Date
                  case "number" => FieldSortType.Number
                  case "string" => FieldSortType.String
                  case _        => throw new UnsupportedOperationException("Bad FTS sort")
                })
                else None,
                if (ss.hasMode) Some(ss.getMode match {
                  case "default" => FieldSortMode.Default
                  case "min"     => FieldSortMode.Min
                  case "max"     => FieldSortMode.Max
                  case _         => throw new UnsupportedOperationException("Bad FTS mode")
                })
                else None,
                if (ss.hasMissing) Some(ss.getMissing match {
                  case "first" => FieldSortMissing.First
                  case "last"  => FieldSortMissing.Last
                  case _       => throw new UnsupportedOperationException("Bad FTS missing")
                })
                else None,
                if (ss.hasDesc) Some(ss.getDesc) else None
              )
            } else if (sort.hasScore) {
              val ss = sort.getScore
              SearchSort.ScoreSort(if (ss.hasDesc) Some(ss.getDesc) else None)
            } else if (sort.hasId()) {
              val ss = sort.getId();
              SearchSort.IdSort(if (ss.hasDesc) Some(ss.getDesc) else None)
            } else if (sort.hasGeoDistance()) {
              val ss = sort.getGeoDistance();
              SearchSort.GeoDistanceSort(
                Seq(ss.getLocation().getLon(), ss.getLocation().getLat()),
                ss.getField(),
                if (ss.hasDesc) Some(ss.getDesc) else None,
                if (ss.hasUnit()) {
                  Some(ss.getUnit() match {
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_METERS      => "meters"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_MILES       => "miles"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_CENTIMETERS =>
                      "centimeters"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_MILLIMETERS =>
                      "millimeters"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_NAUTICAL_MILES =>
                      "nauticalmiles"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_KILOMETERS => "kilometers"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_FEET       => "feet"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_YARDS      => "yards"
                    case SearchGeoDistanceUnits.SEARCH_GEO_DISTANCE_UNITS_INCHES     => "inches"
                    case _ => throw new UnsupportedOperationException();
                  })
                } else None
              )
            } else if (sort.hasRaw()) {
              throw new UnsupportedOperationException()
            } else throw new UnsupportedOperationException()
          })
      )
    if (o.getFacetsCount > 0) {
      val facets: Map[String, SearchFacet] = o.getFacetsMap.asScala
        .map(x => {
          val k                  = x._1
          val v                  = x._2
          val facet: SearchFacet = if (v.hasTerm) {
            val f = v.getTerm
            TermFacet(f.getField, if (f.hasSize) Some(f.getSize) else None)
          } else if (v.hasNumericRange) {
            val f = v.getNumericRange
            NumericRangeFacet(
              f.getField,
              f.getNumericRangesList.asScala
                .map(nr =>
                  NumericRange(
                    nr.getName,
                    if (nr.hasMin) Some(nr.getMin) else None,
                    if (nr.hasMax) Some(nr.getMax)
                    else None
                  )
                ),
              if (f.hasSize) Some(f.getSize) else None
            )
          } else if (v.hasDateRange) {
            val f = v.getDateRange
            DateRangeFacet(
              f.getField,
              f.getDateRangesList.asScala
                .map(nr =>
                  DateRange
                    .create(
                      nr.getName,
                      convertTimestamp(nr.hasStart, nr.getStart),
                      convertTimestamp(nr.hasEnd, nr.getEnd)
                    )
                ),
              if (f.hasSize) Some(f.getSize) else None
            )
          } else throw new UnsupportedOperationException()
          k -> facet
        })
        .toMap
      opts = opts.facets(facets)
    }
    if (o.hasTimeoutMillis) {
      // [if:1.4.5]
      opts = opts.timeout(Duration(o.getTimeoutMillis, TimeUnit.MILLISECONDS))
      // [else]
      // format: off
      //? throw new UnsupportedOperationException()
      // format: on
      // [end]
    }
    if (o.hasParentSpanId) throw new UnsupportedOperationException()
    if (o.getRawCount > 0) opts = opts.raw(o.getRawMap.asScala.toMap)
    if (o.hasIncludeLocations) opts = opts.includeLocations(o.getIncludeLocations)
    Some(opts)
  }

  private def convertTimestamp(timestamp: Timestamp) = Instant.ofEpochSecond(timestamp.getSeconds)

  private def convertSearchQueries(
      queries: Seq[com.couchbase.client.protocol.sdk.search.SearchQuery]
  ): Seq[SearchQuery] =
    queries.map(v => convertSearchQuery(v))

  private def convertSearchQuery(
      q: com.couchbase.client.protocol.sdk.search.SearchQuery
  ): SearchQuery = {
    q.getQueryCase match {
      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.MATCH =>
        val qr  = q.getMatch
        var out = SearchQuery.matchQuery(qr.getMatch)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasAnalyzer) out = out.analyzer(qr.getAnalyzer)
        if (qr.hasPrefixLength) out = out.prefixLength(qr.getPrefixLength)
        if (qr.hasFuzziness) out = out.fuzziness(qr.getFuzziness)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        if (qr.hasOperator)
          if (qr.getOperator eq SEARCH_MATCH_OPERATOR_OR) out = out.matchOperator(MatchOperator.Or)
          else if (qr.getOperator eq SEARCH_MATCH_OPERATOR_AND)
            out = out.matchOperator(MatchOperator.And)
          else throw new UnsupportedOperationException
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.MATCH_PHRASE =>
        val qr  = q.getMatchPhrase
        var out = SearchQuery.matchPhrase(qr.getMatchPhrase)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasAnalyzer) out = out.analyzer(qr.getAnalyzer)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.REGEXP =>
        val qr  = q.getRegexp
        var out = SearchQuery.regexp(qr.getRegexp)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.QUERY_STRING =>
        val qr  = q.getQueryString
        var out = SearchQuery.queryString(qr.getQuery)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.WILDCARD =>
        val qr  = q.getWildcard
        var out = SearchQuery.wildcard(qr.getWildcard)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.DOC_ID =>
        val qr  = q.getDocId
        var out = SearchQuery.docId(qr.getIdsList.asScala: _*)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.SEARCH_BOOLEAN_FIELD =>
        val qr  = q.getSearchBooleanField
        var out = SearchQuery.booleanField(qr.getBool)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.DATE_RANGE =>
        val qr  = q.getDateRange
        var out = SearchQuery.dateRange
        if (qr.hasStart)
          if (qr.hasInclusiveStart) out = out.start(qr.getStart, qr.getInclusiveStart)
          else out = out.start(qr.getStart)
        if (qr.hasEnd)
          if (qr.hasInclusiveEnd) out = out.end(qr.getEnd, qr.getInclusiveEnd)
          else out = out.end(qr.getEnd)
        if (qr.hasDatetimeParser) out = out.dateTimeParser(qr.getDatetimeParser)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.NUMERIC_RANGE =>
        val qr  = q.getNumericRange
        var out = SearchQuery.numericRange
        if (qr.hasMin)
          if (qr.hasInclusiveMin) out = out.min(qr.getMin, qr.getInclusiveMin)
          else out = out.min(qr.getMin)
        if (qr.hasMax)
          if (qr.hasInclusiveMax) out = out.max(qr.getMax, qr.getInclusiveMax)
          else out = out.max(qr.getMax)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.TERM_RANGE =>
        val qr  = q.getTermRange
        var out = SearchQuery.termRange
        if (qr.hasMin)
          if (qr.hasInclusiveMin) out = out.min(qr.getMin, qr.getInclusiveMin)
          else out = out.min(qr.getMin)
        if (qr.hasMax)
          if (qr.hasInclusiveMax) out = out.max(qr.getMax, qr.getInclusiveMax)
          else out = out.max(qr.getMax)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.GEO_DISTANCE =>
        val qr  = q.getGeoDistance
        var out =
          SearchQuery.geoDistance(qr.getLocation.getLon, qr.getLocation.getLat, qr.getDistance)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.GEO_BOUNDING_BOX =>
        val qr  = q.getGeoBoundingBox
        var out = SearchQuery.geoBoundingBox(
          qr.getTopLeft.getLon,
          qr.getTopLeft.getLat,
          qr.getBottomRight.getLon,
          qr.getBottomRight.getLat
        )
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.CONJUNCTION =>
        val qr        = q.getConjunction
        val converted = convertSearchQueries(qr.getConjunctsList.asScala)
        var out       = SearchQuery.conjuncts(converted: _*)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.DISJUNCTION =>
        val qr        = q.getDisjunction
        val converted = convertSearchQueries(qr.getDisjunctsList.asScala)
        var out       = SearchQuery.disjuncts(converted: _*)
        if (qr.hasMin) out = out.min(qr.getMin)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.BOOLEAN =>
        val qr  = q.getBoolean
        var out = SearchQuery.boolean
        if (qr.getMustCount > 0) {
          val converted = convertSearchQueries(qr.getMustList.asScala)
          out = out.must(converted: _*)
        }
        if (qr.getShouldCount > 0) {
          val converted = convertSearchQueries(qr.getShouldList.asScala)
          out = out.should(converted: _*)
        }
        if (qr.getMustNotCount > 0) {
          val converted = convertSearchQueries(qr.getMustNotList.asScala)
          out = out.mustNot(converted: _*)
        }
        if (qr.hasShouldMin) out = out.shouldMin(qr.getShouldMin)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.TERM =>
        val qr  = q.getTerm
        var out = SearchQuery.term(qr.getTerm)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasFuzziness) out = out.fuzziness(qr.getFuzziness)
        if (qr.hasPrefixLength) out = out.prefixLength(qr.getPrefixLength)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.PREFIX =>
        val qr  = q.getPrefix
        var out = SearchQuery.prefix(qr.getPrefix)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.PHRASE =>
        val qr  = q.getPhrase
        var out = SearchQuery.phrase(qr.getTermsList.asScala: _*)
        if (qr.hasField) out = out.field(qr.getField)
        if (qr.hasBoost) out = out.boost(qr.getBoost)
        out

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.MATCH_ALL =>
        SearchQuery.matchAll

      case com.couchbase.client.protocol.sdk.search.SearchQuery.QueryCase.MATCH_NONE =>
        SearchQuery.matchNone

      case _ => throw new UnsupportedOperationException
    }
  }

  def handleSearchQueryBlocking(
      cluster: Cluster,
      command: com.couchbase.client.protocol.sdk.search.Search
  ): Result.Builder = {
    val q       = command.getQuery
    val query   = SearchHelper.convertSearchQuery(q)
    val options = SearchHelper.convertSearchOptions(command.hasOptions, command.getOptions)
    val result  = Result.newBuilder
    result.setInitiated(getTimeNow)
    val start = System.nanoTime
    val r     = options match {
      case Some(opts) => cluster.searchQuery(command.getIndexName, query, opts).get
      case _          => cluster.searchQuery(command.getIndexName, query).get
    }
    result.setElapsedNanos(System.nanoTime - start)
    result.setSdk(
      com.couchbase.client.protocol.sdk.Result.newBuilder
        .setSearchBlockingResult(
          convertResult(r, if (command.hasFieldsAs) Some(command.getFieldsAs) else None)
        )
    )
    result
  }

  // [if:1.6.0]
  def handleSearchBlocking(
      cluster: Cluster,
      command: com.couchbase.client.protocol.sdk.search.SearchWrapper
  ): Result.Builder = {
    val search  = command.getSearch
    val request = SearchHelper.convertSearchRequest(search.getRequest)
    val options = SearchHelper.convertSearchOptions(search.hasOptions, search.getOptions)
    val result  = Result.newBuilder
    result.setInitiated(getTimeNow)
    val start = System.nanoTime
    val r     = options match {
      case Some(opts) => cluster.search(search.getIndexName, request, opts).get
      case _          => cluster.search(search.getIndexName, request).get
    }
    result.setElapsedNanos(System.nanoTime - start)
    result.setSdk(
      com.couchbase.client.protocol.sdk.Result.newBuilder
        .setSearchBlockingResult(
          convertResult(r, if (command.hasFieldsAs) Some(command.getFieldsAs) else None)
        )
    )
    result
  }

  def handleScopeSearchBlocking(
      scope: Scope,
      command: com.couchbase.client.protocol.sdk.search.SearchWrapper
  ): Result.Builder = {
    val search  = command.getSearch
    val request = SearchHelper.convertSearchRequest(search.getRequest)
    val options = SearchHelper.convertSearchOptions(search.hasOptions, search.getOptions)
    val result  = Result.newBuilder
    result.setInitiated(getTimeNow)
    val start = System.nanoTime
    val r     = options match {
      case Some(opts) => scope.search(search.getIndexName, request, opts).get
      case _          => scope.search(search.getIndexName, request).get
    }
    result.setElapsedNanos(System.nanoTime - start)
    result.setSdk(
      com.couchbase.client.protocol.sdk.Result.newBuilder
        .setSearchBlockingResult(
          convertResult(r, if (command.hasFieldsAs) Some(command.getFieldsAs) else None)
        )
    )
    result
  }

  def convertVectorSearch(
      vectorSearch: com.couchbase.client.protocol.sdk.search.VectorSearch
  ): com.couchbase.client.scala.search.vector.VectorSearch = {
    var out = com.couchbase.client.scala.search.vector.VectorSearch(
      vectorSearch.getVectorQueryList.asScala
        .map(convertVectorQuery)
    )
    if (vectorSearch.hasOptions) {
      val opts = vectorSearch.getOptions
      if (opts.hasVectorQueryCombination) {
        val vqc: com.couchbase.client.scala.search.vector.VectorQueryCombination =
          opts.getVectorQueryCombination match {
            case com.couchbase.client.protocol.sdk.search.VectorQueryCombination.AND =>
              com.couchbase.client.scala.search.vector.VectorQueryCombination.And
            case com.couchbase.client.protocol.sdk.search.VectorQueryCombination.OR =>
              com.couchbase.client.scala.search.vector.VectorQueryCombination.Or
            case _ => throw new UnsupportedOperationException()
          }
        out = out.vectorSearchOptions(
          com.couchbase.client.scala.search.vector
            .VectorSearchOptions()
            .vectorQueryCombination(vqc)
        )
      }
    }
    out
  }

  def convertVectorQuery(
      vq: com.couchbase.client.protocol.sdk.search.VectorQuery
  ): com.couchbase.client.scala.search.vector.VectorQuery = {
    // [if:1.6.2]
    var out = if (vq.hasBase64VectorQuery) {
      com.couchbase.client.scala.search.vector
        .VectorQuery(vq.getVectorFieldName, vq.getBase64VectorQuery)
    } else {
      val query: Array[Float] =
        vq.getVectorQueryList.asScala.toArray.map(v => v.asInstanceOf[Float])
      com.couchbase.client.scala.search.vector.VectorQuery(vq.getVectorFieldName, query)
    }
    // [else]
    // format: off
    //? val query: Array[Float] = vq.getVectorQueryList.asScala.toArray.map(v => v.asInstanceOf[Float])
    //? var out                 = com.couchbase.client.scala.search.vector.VectorQuery(vq.getVectorFieldName, query)
    // format: on
    // [end]
    if (vq.hasOptions) {
      val opts = vq.getOptions
      if (opts.hasNumCandidates) out = out.numCandidates(opts.getNumCandidates)
      if (opts.hasBoost) out = out.boost(opts.getBoost)
      // [if:1.9.0]
      if (opts.hasPrefilter) out = out.prefilter(convertSearchQuery(opts.getPrefilter))
      // [end]
    }
    out
  }

  def convertSearchRequest(
      request: com.couchbase.client.protocol.sdk.search.SearchRequest
  ): com.couchbase.client.scala.search.vector.SearchRequest = {
    if (request.hasSearchQuery) {
      var out = com.couchbase.client.scala.search.vector.SearchRequest
        .searchQuery(convertSearchQuery(request.getSearchQuery))
      if (request.hasVectorSearch) {
        out = out.vectorSearch(convertVectorSearch(request.getVectorSearch))
      }
      out
    } else if (request.hasVectorSearch) {
      com.couchbase.client.scala.search.vector.SearchRequest
        .vectorSearch(convertVectorSearch(request.getVectorSearch))
    } else {
      com.couchbase.client.scala.search.vector.SearchRequest.searchQuery(null)
    }
  }

  // [end]

  private def convertResult(
      result: SearchResult,
      fieldsAs: Option[ContentAs]
  ): com.couchbase.client.protocol.sdk.search.BlockingSearchResult =
    com.couchbase.client.protocol.sdk.search.BlockingSearchResult.newBuilder
      .addAllRows(result.rows.map(v => convertRow(v, fieldsAs)).asJava)
      .setMetaData(convertMetaData(result.metaData))
      .setFacets(convertFacets(result.facets))
      .build

  def convertFacets(
      facets: Map[String, SearchFacetResult]
  ): com.couchbase.client.protocol.sdk.search.SearchFacets = {
    com.couchbase.client.protocol.sdk.search.SearchFacets.newBuilder
      .putAllFacets(
        facets
          .map(x => {
            val v = x._2
            x._1 -> com.couchbase.client.protocol.sdk.search.SearchFacetResult.newBuilder
              .setName(v.name)
              .setField(v.field)
              .setTotal(v.total)
              .setMissing(v.missing)
              .setOther(v.other)
              .build
          })
          .asJava
      )
      .build
  }

  def convertMetaData(
      metaData: SearchMetaData
  ): com.couchbase.client.protocol.sdk.search.SearchMetaData =
    com.couchbase.client.protocol.sdk.search.SearchMetaData.newBuilder
      .setMetrics(
        com.couchbase.client.protocol.sdk.search.SearchMetrics.newBuilder
          .setTookMsec(metaData.metrics.took.toMillis)
          .setTotalRows(metaData.metrics.totalRows)
          .setMaxScore(metaData.metrics.maxScore)
          .setTotalPartitionCount(metaData.metrics.totalPartitionCount)
          .setSuccessPartitionCount(metaData.metrics.successPartitionCount)
          .setErrorPartitionCount(metaData.metrics.errorPartitionCount)
      )
      .putAllErrors(metaData.errors.asJava)
      .build

  def convertRow(
      v: SearchRow,
      fieldsAs: Option[ContentAs]
  ): com.couchbase.client.protocol.sdk.search.SearchRow = {
    val builder = com.couchbase.client.protocol.sdk.search.SearchRow.newBuilder
      .setIndex(v.index)
      .setId(v.id)
      .setScore(v.score)
      .setExplanation(ByteString.copyFrom(v.explanationAs[Array[Byte]].get))

    builder.putAllFragments(
      v.fragments
        .map(v => {
          v._1 -> com.couchbase.client.protocol.sdk.search.SearchFragments.newBuilder
            .addAllFragments(v._2.asJava)
            .build
        })
        .asJava
    )

    v.locations.map(locations => {
      builder.addAllLocations(
        locations.getAll
          .map(l => {
            com.couchbase.client.protocol.sdk.search.SearchRowLocation.newBuilder
              .setField(l.field)
              .setTerm(l.term)
              .setPosition(l.pos)
              .setStart(l.start)
              .setEnd(l.end)
              .addAllArrayPositions(
                l.arrayPositions.getOrElse(Seq()).map(v => v.asInstanceOf[Integer]).asJava
              )
              .build
          })
          .asJava
      )
    })

    fieldsAs match {
      case Some(fa) =>
        val content = ContentAsUtil.contentType(
          fa,
          () => v.fieldsAs[Array[Byte]],
          () => v.fieldsAs[String],
          () => v.fieldsAs[JsonObject],
          () => v.fieldsAs[JsonArray],
          () => v.fieldsAs[Boolean],
          () => v.fieldsAs[Int],
          () => v.fieldsAs[Double]
        )
        content match {
          case Failure(exception) => throw exception
          case Success(value)     => builder.setFields(value)
        }
      case _ =>
    }
    builder.build
  }

  def handleClusterSearchIndexManager(
      cluster: Cluster,
      command: com.couchbase.client.protocol.sdk.Command
  ): com.couchbase.client.protocol.run.Result.Builder = {
    val sim = command.getClusterCommand.getSearchIndexManager
    if (!sim.hasShared) throw new UnsupportedOperationException
    handleSearchIndexManagerBlockingShared(cluster, command, sim.getShared)
  }

  // [start:1.6.0]
  def handleScopeSearchIndexManager(
      scope: Scope,
      command: com.couchbase.client.protocol.sdk.Command
  ): com.couchbase.client.protocol.run.Result.Builder = {
    val sim = command.getScopeCommand.getSearchIndexManager
    if (!sim.hasShared) throw new UnsupportedOperationException
    handleScopeSearchIndexManagerBlockingShared(scope, command, sim.getShared)
  }
  // [end:1.6.0]

  private def handleSearchIndexManagerBlockingShared(
      cluster: Cluster,
      op: com.couchbase.client.protocol.sdk.Command,
      command: com.couchbase.client.protocol.sdk.search.indexmanager.Command
  ) = {
    val result = Result.newBuilder
    if (command.hasGetIndex) {
      val req = command.getGetIndex

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val index = cluster.searchIndexes
        .getIndex(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, index)
      else setSuccess(result)
    } else if (command.hasGetAllIndexes) {
      val req = command.getGetAllIndexes

      result.setInitiated(getTimeNow)
      val start   = System.nanoTime
      val indexes = cluster.searchIndexes
        .getAllIndexes(
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, indexes)
      else setSuccess(result)
    } else if (command.hasUpsertIndex) {
      val req = command.getUpsertIndex

      // [if:1.4.5]
      val converted = SearchIndex.fromJson(req.getIndexDefinition.toStringUtf8).get
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .upsertIndex(
          converted,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
      // [else]
      // format: off
      //? throw new UnsupportedOperationException()
      // format: on
      // [end]
    } else if (command.hasDropIndex) {
      val req = command.getDropIndex

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .dropIndex(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
      // [start:1.6.0]
    } else if (command.hasGetIndexedDocumentsCount) {
      val req = command.getGetIndexedDocumentsCount

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val count = cluster.searchIndexes
        .getIndexedDocumentsCount(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      result.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setSearchIndexManagerResult(
            com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder
              .setIndexedDocumentCounts(count.toInt)
          )
      )
    } else if (command.hasPauseIngest) {
      val req = command.getPauseIngest

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .pauseIngest(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasResumeIngest) {
      val req = command.getResumeIngest

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .resumeIngest(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasAllowQuerying) {
      val req = command.getAllowQuerying

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .allowQuerying(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasDisallowQuerying) {
      val req = command.getDisallowQuerying

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .disallowQuerying(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasFreezePlan) {
      val req = command.getFreezePlan

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .freezePlan(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasUnfreezePlan) {
      val req = command.getUnfreezePlan

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .unfreezePlan(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasAnalyzeDocument) {
      val req = command.getAnalyzeDocument

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      cluster.searchIndexes
        .analyzeDocument(
          req.getIndexName,
          JsonObject.fromJson(req.getDocument.toStringUtf8),
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
      // [end:1.6.0]
    } else
      throw new UnsupportedOperationException(
        "Unknown FTS operation"
      )

    result
  }

  // [start:1.6.0]
  private def handleScopeSearchIndexManagerBlockingShared(
      scope: Scope,
      op: com.couchbase.client.protocol.sdk.Command,
      command: com.couchbase.client.protocol.sdk.search.indexmanager.Command
  ) = {
    val result = Result.newBuilder
    if (command.hasGetIndex) {
      val req = command.getGetIndex

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val index = scope.searchIndexes
        .getIndex(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, index)
      else setSuccess(result)
    } else if (command.hasGetAllIndexes) {
      val req = command.getGetAllIndexes

      result.setInitiated(getTimeNow)
      val start   = System.nanoTime
      val indexes = scope.searchIndexes
        .getAllIndexes(
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, indexes)
      else setSuccess(result)
    } else if (command.hasUpsertIndex) {
      val req = command.getUpsertIndex

      val converted = SearchIndex.fromJson(req.getIndexDefinition.toStringUtf8).get
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .upsertIndex(
          converted,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasDropIndex) {
      val req = command.getDropIndex

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .dropIndex(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
      // [start:1.6.0]
    } else if (command.hasGetIndexedDocumentsCount) {
      val req = command.getGetIndexedDocumentsCount

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val count = scope.searchIndexes
        .getIndexedDocumentsCount(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      result.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setSearchIndexManagerResult(
            com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder
              .setIndexedDocumentCounts(count.toInt)
          )
      )
    } else if (command.hasPauseIngest) {
      val req = command.getPauseIngest

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .pauseIngest(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasResumeIngest) {
      val req = command.getResumeIngest

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .resumeIngest(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasAllowQuerying) {
      val req = command.getAllowQuerying

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .allowQuerying(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasDisallowQuerying) {
      val req = command.getDisallowQuerying

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .disallowQuerying(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasFreezePlan) {
      val req = command.getFreezePlan

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .freezePlan(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasUnfreezePlan) {
      val req = command.getUnfreezePlan

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .unfreezePlan(
          req.getIndexName,
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (command.hasAnalyzeDocument) {
      val req = command.getAnalyzeDocument

      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      scope.searchIndexes
        .analyzeDocument(
          req.getIndexName,
          JsonObject.fromJson(req.getDocument.toStringUtf8),
          if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
            Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
        .get
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
      // [end:1.6.0]
    } else
      throw new UnsupportedOperationException(
        "The Scala SDK doesn't support several tertiary FTS index operations"
      )

    result
  }
  // [end:1.6.0]

  private def populateResult(result: Result.Builder, indexes: Seq[SearchIndex]): Unit = {
    result.setSdk(
      com.couchbase.client.protocol.sdk.Result.newBuilder.setSearchIndexManagerResult(
        com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder.setIndexes(
          SearchIndexes.newBuilder
            .addAllIndexes(indexes.map(SearchHelper.convertSearchIndex).asJava)
            .build
        )
      )
    )
  }

  private def populateResult(result: Result.Builder, index: SearchIndex): Unit = {
    result.setSdk(
      com.couchbase.client.protocol.sdk.Result.newBuilder.setSearchIndexManagerResult(
        com.couchbase.client.protocol.sdk.search.indexmanager.Result.newBuilder
          .setIndex(convertSearchIndex(index))
      )
    )
  }

  private def convertSearchIndex(i: SearchIndex) = {
    val builder = com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndex.newBuilder
      .setName(i.name)
    i.uuid.foreach(v => builder.setUuid(v))
    i.typ.foreach(v => builder.setType(v))
    i.paramsAs[Array[Byte]].foreach(v => builder.setParams(ByteString.copyFrom(v)))
    i.sourceParamsAs[Array[Byte]].foreach(v => builder.setSourceParams(ByteString.copyFrom(v)))
    i.planParamsAs[Array[Byte]].foreach(v => builder.setPlanParams(ByteString.copyFrom(v)))
    i.sourceUUID.foreach(v => builder.setSourceUuid(v))
    i.sourceType.foreach(v => builder.setSourceType(v))
    builder.build
  }

  def convertTimestamp(has: Boolean, value: Timestamp): Option[Instant] = {
    if (has) Some(Instant.ofEpochSecond(value.getSeconds))
    else None
  }

}
