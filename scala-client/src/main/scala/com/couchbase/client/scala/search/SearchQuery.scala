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
package com.couchbase.client.scala.search

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.scala.json.{JsonArray, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.queries._
import com.couchbase.client.scala.search.result.{SearchQueryRow, SearchResult}
import com.couchbase.client.scala.search.sort.SearchSort

import scala.concurrent.duration.Duration
import scala.util.Success

/**
  * The FTS API entry point. Describes an FTS query entirely (index, query body and parameters) and can
  * be used at the [[com.couchbase.client.scala.Cluster]] level to perform said query. Also has factory methods for
  * all types of
  * FTS queries (as in the various types a query body can have: term, match, conjunction, ...).
  *
  * @param indexName the name of the index targeted by this query.
  */
case class SearchQuery(indexName: String,
                       private[scala] val queryPart: AbstractFtsQuery,
                       private[scala] val limit: Option[Int] = None,
                       private[scala] val skip: Option[Int] = None,
                       private[scala] val explain: Option[Boolean] = None,
                       private[scala] val highlightStyle: Option[HighlightStyle] = None,
                       private[scala] val highlightFields: Option[Seq[String]] = None,
                       private[scala] val fields: Option[Seq[String]] = None,
                       private[scala] val sort: Option[JsonArray] = None,
                       private[scala] val facets: Option[Map[String, SearchFacet]] = None,
                       private[scala] val serverSideTimeout: Option[Duration] = None,
                       private[scala] val mutationState: Option[Seq[MutationToken]] = None,
                       private[scala] val deferredError: Option[RuntimeException] = None) {

  /** Add a limit to the query on the number of rows it can return.
    *
    * @param limit the maximum number of rows to return.
    *
    * @return this SearchQuery for chaining.
    */
  def limit(limit: Int): SearchQuery = {
    copy(limit = Some(limit))
  }

  /** Set the number of rows to skip (eg. for pagination).
    *
    * @param skip the number of results to skip.
    *
    * @return this SearchQuery for chaining.
    */
  def skip(skip: Int): SearchQuery = {
    copy(skip = Some(skip))
  }

  /** Activates the explanation of each result hit in the response.  The default is false.
    *
    * @return this SearchQuery for chaining.
    */
  def explain(value: Boolean): SearchQuery = {
    copy(explain = Some(value))
  }

  /** Configures the highlighting of matches in the response.
    *
    * Highlighting is disabled by default.
    *
    * This drives the inclusion of the [[SearchQueryRow.fragments]] fragments
    * in each [[SearchQueryRow]] row.
    *
    * Note that to be highlighted, the fields must be stored in the FTS index.
    *
    * @param style  the [[HighlightStyle]] to apply.
    * @param fields the optional fields on which to highlight. If none, all fields where there is a match are
    *               highlighted.
    *
    * @return this SearchQuery for chaining.
    */
  def highlight(style: HighlightStyle, fields: String*): SearchQuery = {
    copy(fields = Some(fields.toSeq), highlightStyle = Some(style))
  }

  /** Configures the highlighting of matches in the response.
    *
    * Highlighting is disabled by default.
    *
    * This drives the inclusion of the [[SearchQueryRow.fragments]] fragments} in each [[SearchQueryRow]] hit.
    *
    * Note that to be highlighted, the fields must be stored in the FTS index.
    *
    * @param fields the optional fields on which to highlight. If none, all fields where there is a match are
    *               highlighted.
    *
    * @return this SearchQuery for chaining.
    */
  def highlight(fields: String*): SearchQuery = {
    copy(fields = Some(fields.toSeq))
  }

  /** Configures the list of fields for which the whole value should be included in the response. If empty, no field
    * values are included.
    *
    * This drives the inclusion of the [[SearchQueryRow.fields]] fields in each [[SearchQueryRow]] hit.
    *
    * Note that to be highlighted, the fields must be stored in the FTS index.
    *
    * @param fields
    *
    * @return this SearchQuery for chaining.
    */
  def fields(fields: String*): SearchQuery = {
    copy(fields = Some(fields.toSeq))
  }

  /** Configures the list of fields (including special fields) which are used for sorting purposes. If empty, the
    * default sorting (descending by score) is used by the server.
    *
    * The list of sort fields can include actual fields (like "firstname" but then they must be stored in the index,
    * configured in the server side mapping). Fields provided first are considered first and in a "tie" case the
    * next sort field is considered. So sorting by "firstname" and then "lastname" will first sort ascending by
    * the firstname and if the names are equal then sort ascending by lastname. Special fields like "_id" and "_score"
    * can also be used. If prefixed with "-" the sort order is set to descending.
    *
    * If no sort is provided, it is equal to sort("-_score"), since the server will sort it by score in descending
    * order.
    *
    * @param sortFields the fields that should take part in the sorting.
    *
    * @return this SearchQuery for chaining.
    */
  def sort(sortFields: Any*): SearchQuery = {
    var out = this

    sortFields.foreach {
      case v: String =>
        out = out.copy(sort = Some(sort.getOrElse(JsonArray.create).add(v)))
      case v: SearchSort =>
        val params = JsonObject.create
        v.injectParams(params)
        out = copy(sort = Some(sort.getOrElse(JsonArray.create).add(params)))
      case v: JsonObject =>
        out = copy(sort = Some(sort.getOrElse(JsonArray.create).add(v)))
      case _ =>
        out = copy(deferredError =
          Some(new IllegalArgumentException("Only String or SearchSort instances are allowed as sort arguments")))
    }

    out
  }

  /** Sets the consistency to consider for this FTS query to AT_PLUS and
    * uses the mutation information from the given mutation tokens to parameterize
    * the consistency. This replaces any consistency tuning previously set.
    *
    * @return this SearchQuery for chaining.
    */
  def consistentWith(mutationTokens: MutationToken*): SearchQuery = {
    copy(mutationState = Some(mutationTokens))
  }

  /** Adds one [[SearchFacet]] to the query.
    *
    * This is an additive operation (the given facets are added to any facet previously requested),
    * but if an existing facet has the same name it will be replaced.
    *
    * This drives the inclusion of the facets in the [[SearchResult]].
    *
    * Note that to be faceted, a field's value must be stored in the FTS index.
    *
    * @param facetName the name of the facet to add (or replace if one already exists with same name).
    * @param facet     the facet to add.
    */
  def addFacet(facetName: String, facet: SearchFacet): SearchQuery = {
    val f = this.facets.getOrElse(Map.empty) + (facetName -> facet)
    copy(facets = Some(f))
  }

  /** Sets the server side timeout. By default, the SDK will set this value to the configured
    * `ClusterEnvironment.timeoutConfig.searchTimeout` from the environment.
    *
    * @param timeout the server side timeout to apply.
    *
    * @return this SearchQuery for chaining.
    */
  def serverSideTimeout(timeout: Duration): SearchQuery = {
    copy(serverSideTimeout = Some(timeout))
  }

  def query = queryPart

  /** Exports the whole query as a [[JsonObject]].
    */
  private[scala] def export(): JsonObject = {
    val result = JsonObject.create
    injectParams(result)

    val queryJson = JsonObject.create
    queryPart.injectParamsAndBoost(queryJson)
    result.put("query", queryJson)
    result
  }

  /** Inject the top level parameters of a query into a prepared [[JsonObject]]
    * that represents the root of the query.
    *
    * @param queryJson the prepared { @link JsonObject} for the whole query.
    */
  private[scala] def injectParams(queryJson: JsonObject): Unit = {
    limit.foreach(v => if (v > 0) queryJson.put("size", v))
    skip.foreach(v => if (v > 0) queryJson.put("from", v))
    explain.foreach(v => queryJson.put("explain", v))

    highlightStyle.foreach(hs => {
      val highlight = JsonObject.create
      hs match {
        case HighlightStyle.ServerDefault =>
        case HighlightStyle.HTML => highlight.put("style", "html")
        case HighlightStyle.ANSI => highlight.put("style", "ansi")
      }

      highlightFields.foreach(hf => {
        if (hf.nonEmpty) highlight.put("fields", JsonArray(hf))
      })

      queryJson.put("highlight", highlight)
    })
    fields.foreach(f => {
      if (f.nonEmpty) queryJson.put("fields", JsonArray(f))
    })

    sort.foreach(v => queryJson.put("sort", v))
    facets.foreach(f => {
      val facets = JsonObject.create
      for ( entry <- f ) {
        val facetJson = JsonObject.create
        entry._2.injectParams(facetJson)
        facets.put(entry._1, facetJson)
      }
      queryJson.put("facets", facets)
    })
    val control = JsonObject.create
    serverSideTimeout.foreach(v => control.put("timeout", v))
    mutationState.foreach(ms => {
      val consistencyJson = JsonObject.create
      consistencyJson.put("level", "at_plus")
      consistencyJson.put("vectors", JsonObject(indexName -> convertMutationTokens(ms)))
      control.put("consistency", consistencyJson)
    })
    if (!control.isEmpty) queryJson.put("ctl", control)
  }

  private def convertMutationTokens(tokens: Seq[MutationToken]): JsonObject = {
    val result = JsonObjectSafe.create
    for ( token <- tokens ) {
      val bucket: JsonObjectSafe = result.obj(token.bucket) match {
        case Success(bucket) => bucket
        case _ =>
          val out = JsonObjectSafe.create
          result.put(token.bucket, out)
          out
      }

      bucket.put(String.valueOf(token.vbucketID),
        JsonArray(token.sequenceNumber, String.valueOf(token.vbucketUUID)))
    }

    result.o
  }
}

object SearchQuery {

  /** An FTS query that performs a search according to the "query string" syntax.
    *
    * @param query The query string to be analyzed and used against.
    */
  def queryString(query: String) = QueryStringQuery(query)

  /** An FTS query that matches a given term, applying further processing to it
    * like analyzers, stemming and even fuzziness.
    *
    * @param matchStr input string to be matched against.
    *
    * @since 1.0.0
    */
  def matchQuery(matchStr: String) = MatchQuery(matchStr)

  /** An FTS query that matches several given terms (a "phrase"), applying further processing
    * like analyzers to them.
    *
    * @param matchPhrase The input phrase to be matched against.
    */
  def matchPhrase(matchPhrase: String) = MatchPhraseQuery(matchPhrase)

  /** An FTS query that allows for simple matching on a given prefix.
    */
  def prefix(prefix: String) = PrefixQuery(prefix)

  /** An FTS query that allows for simple matching of regular expressions.
    *
    * @param regexp The regexp to be analyzed and used against.
    */
  def regexp(regexp: String) = RegexpQuery(regexp)

  /** An FTS query that matches documents on a range of values. At least one bound is required, and the
    * inclusiveness of each bound can be configured.
    *
    * At least one of min and max must be provided. */
  def termRange = TermRangeQuery()

  /** An FTS query that matches documents on a range of values. At least one bound is required, and the
    * inclusiveness of each bound can be configured.
    */
  def numericRange = NumericRangeQuery()

  /** An FTS query that matches documents on a range of dates. At least one bound is required, and the parser
    * to use for the date (in [[String]] form) can be customized.
    */
  def dateRange = DateRangeQuery()

  /** A compound FTS query that performs a logical OR between all its sub-queries (disjunction).
    * It requires that a configurable minimum of the queries match (the default is 1).
    */
  def disjuncts(queries: AbstractFtsQuery*) = DisjunctionQuery(queries)

  /** A compound FTS query that performs a logical AND between all its sub-queries (conjunction).
    */
  def conjuncts(queries: AbstractFtsQuery*) = ConjunctionQuery(queries)

  /** A compound FTS query that allows various combinations of sub-queries.
    */
  def boolean = BooleanQuery()

  /** An FTS query that allows for simple matching using wildcard characters (* and ?). */
  def wildcard(wildcard: String) = WildcardQuery(wildcard)

  /** An FTS query that matches on Couchbase document IDs. Useful to restrict the search space to a list of keys
    * (by using this in an [[AbstractCompoundQuery]] compound query).
    *
    * @param docIds list of document IDs to be restricted against. At least one ID is required.
    */
  def docId(docIds: String*) = DocIdQuery(docIds)

  /** An FTS query that queries fields explicitly indexed as boolean.
    */
  def booleanField(value: Boolean) = BooleanFieldQuery(value)

  /** Prepare a [[TermQuery]] body. */
  def term(term: String) = TermQuery(term)

  /** An FTS query that matches several terms (a "phrase") as is. The order of the terms matter and no
    * further processing is applied to them, so they must appear in the index exactly as provided.
    * Usually for debugging purposes, prefer [[MatchPhraseQuery]].
    *
    * @param terms The mandatory list of terms that must exactly match in the index.  Must contain at least one term.
    */
  def phrase(terms: String*) = PhraseQuery(terms)

  /** An FTS query that matches all indexed documents (usually for debugging purposes).
    */
  def matchAll = MatchAllQuery()

  /** An FTS query that matches 0 document (usually for debugging purposes).
    */
  def matchNone = MatchNoneQuery()

  /** An FTS query which finds all matches within a given box (identified by the upper left and lower right corner
    * coordinates).
    *
    * @param topLeftLon     the longitude of the top-left point of the box
    * @param topLeftLat     the latitude of the top-left point of the box
    * @param bottomRightLon the longitude of the bottom-right point of the box
    * @param bottomRightLat the latitude of the bottom-right point of the box
    */
  def geoBoundingBox(topLeftLon: Double,
                     topLeftLat: Double,
                     bottomRightLon: Double,
                     bottomRightLat: Double): GeoBoundingBoxQuery = {
    GeoBoundingBoxQuery(topLeftLon, topLeftLat, bottomRightLon, bottomRightLat)
  }

  /** An FTS query that finds all matches from a given location (point) within the given distance.
    *
    * @param locationLon the location's longitude
    * @param locationLat the location's latitude
    * @param distance    the distance to search from the location, e.g. "10mi"
    */
  def geoDistance(locationLon: Double, locationLat: Double, distance: String): GeoDistanceQuery = {
    GeoDistanceQuery(locationLon, locationLat, distance)
  }
}
