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

package com.couchbase.client.scala.search.queries

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.search.CoreSearchQuery
import com.couchbase.client.scala.util.Coordinate

/** Base class for FTS queries that are composite, compounding several other [[SearchQuery]].
  *
  * @since 1.0.0
  */
trait AbstractCompoundQuery extends SearchQuery

/** A base class for all FTS query classes. Exposes the common FTS query parameters.
  * In order to instantiate various flavors of queries, look at concrete classes or
  * static factory methods in [[com.couchbase.client.scala.search.SearchOptions]].
  *
  * @since 1.0.0
  */
trait SearchQuery {
  private[scala] def toCore: CoreSearchQuery

  /** @return the String representation of the FTS query, which is its JSON representation without global parameters.
    */
  override def toString: String = toCore.`export`.toString
}

object SearchQuery {

  /** An FTS query that performs a search according to the "query string" syntax.
    *
    * @param query The query string to be analyzed and used against.
    */
  def queryString(query: String): QueryStringQuery = QueryStringQuery(query)

  /** An FTS query that matches a given term, applying further processing to it
    * like analyzers, stemming and even fuzziness.
    *
    * @param matchStr input string to be matched against.
    *
    * @since 1.0.0
    */
  def matchQuery(matchStr: String): MatchQuery = MatchQuery(matchStr)

  /** An FTS query that matches several given terms (a "phrase"), applying further processing
    * like analyzers to them.
    *
    * @param matchPhrase The input phrase to be matched against.
    */
  def matchPhrase(matchPhrase: String): MatchPhraseQuery = MatchPhraseQuery(matchPhrase)

  /** An FTS query that allows for simple matching on a given prefix.
    */
  def prefix(prefix: String): PrefixQuery = PrefixQuery(prefix)

  /** An FTS query that allows for simple matching of regular expressions.
    *
    * @param regexp The regexp to be analyzed and used against.
    */
  def regexp(regexp: String): RegexpQuery = RegexpQuery(regexp)

  /** An FTS query that matches documents on a range of values. At least one bound is required, and the
    * inclusiveness of each bound can be configured.
    *
    * At least one of min and max must be provided. */
  def termRange: TermRangeQuery = TermRangeQuery()

  /** An FTS query that matches documents on a range of values. At least one bound is required, and the
    * inclusiveness of each bound can be configured.
    */
  def numericRange: NumericRangeQuery = NumericRangeQuery()

  /** An FTS query that matches documents on a range of dates. At least one bound is required, and the parser
    * to use for the date (in `String` form) can be customized.
    */
  def dateRange: DateRangeQuery = DateRangeQuery()

  /** A compound FTS query that performs a logical OR between all its sub-queries (disjunction).
    * It requires that a configurable minimum of the queries match (the default is 1).
    */
  def disjuncts(queries: SearchQuery*): DisjunctionQuery = DisjunctionQuery(queries)

  /** A compound FTS query that performs a logical AND between all its sub-queries (conjunction).
    */
  def conjuncts(queries: SearchQuery*): ConjunctionQuery = ConjunctionQuery(queries)

  /** A compound FTS query that allows various combinations of sub-queries.
    */
  def boolean: BooleanQuery = BooleanQuery()

  /** An FTS query that allows for simple matching using wildcard characters (* and ?). */
  def wildcard(wildcard: String): WildcardQuery = WildcardQuery(wildcard)

  /** An FTS query that matches on Couchbase document IDs. Useful to restrict the search space to a list of keys
    * (by using this in an [[AbstractCompoundQuery]] compound query).
    *
    * @param docIds list of document IDs to be restricted against. At least one ID is required.
    */
  def docId(docIds: String*): DocIdQuery = DocIdQuery(docIds)

  /** An FTS query that queries fields explicitly indexed as boolean.
    */
  def booleanField(value: Boolean): BooleanFieldQuery = BooleanFieldQuery(value)

  /** Prepare a [[TermQuery]] body. */
  def term(term: String): TermQuery = TermQuery(term)

  /** An FTS query that matches several terms (a "phrase") as is. The order of the terms matter and no
    * further processing is applied to them, so they must appear in the index exactly as provided.
    * Usually for debugging purposes, prefer [[MatchPhraseQuery]].
    *
    * @param terms The mandatory list of terms that must exactly match in the index.  Must contain at least one term.
    */
  def phrase(terms: String*): PhraseQuery = PhraseQuery(terms)

  /** An FTS query that matches all indexed documents (usually for debugging purposes).
    */
  def matchAll: MatchAllQuery = MatchAllQuery()

  /** An FTS query that matches 0 document (usually for debugging purposes).
    */
  def matchNone: MatchNoneQuery = MatchNoneQuery()

  @SinceCouchbase("6.5.1")
  def geoPolygon(coordinates: Seq[Coordinate]): GeoPolygonQuery = GeoPolygonQuery(coordinates)

  /** An FTS query which finds all matches within a given box (identified by the upper left and lower right corner
    * coordinates).
    *
    * @param topLeftLon     the longitude of the top-left point of the box
    * @param topLeftLat     the latitude of the top-left point of the box
    * @param bottomRightLon the longitude of the bottom-right point of the box
    * @param bottomRightLat the latitude of the bottom-right point of the box
    */
  def geoBoundingBox(
      topLeftLon: Double,
      topLeftLat: Double,
      bottomRightLon: Double,
      bottomRightLat: Double
  ): GeoBoundingBoxQuery = {
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
