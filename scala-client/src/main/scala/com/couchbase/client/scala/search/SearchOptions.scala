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

import com.couchbase.client.core.api.search.facet.CoreSearchFacet
import com.couchbase.client.core.api.search.sort.CoreSearchSort
import com.couchbase.client.core.api.search.{
  CoreHighlightStyle,
  CoreSearchOptions,
  CoreSearchScanConsistency
}
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.sort.SearchSort

import java.{lang, util}
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/** Options to be used with a Full Text Search query.
  */
case class SearchOptions(
    private[scala] val limit: Option[Int] = None,
    private[scala] val skip: Option[Int] = None,
    private[scala] val explain: Option[Boolean] = None,
    private[scala] val highlightStyle: Option[HighlightStyle] = None,
    private[scala] val highlightFields: Option[Seq[String]] = None,
    private[scala] val fields: Option[Seq[String]] = None,
    private[scala] val collections: Option[Seq[String]] = None,
    private[scala] val sort: Option[Seq[SearchSort]] = None,
    private[scala] val facets: Option[Map[String, SearchFacet]] = None,
    private[scala] val serverSideTimeout: Option[Duration] = None,
    private[scala] val deferredError: Option[RuntimeException] = None,
    private[scala] val scanConsistency: Option[SearchScanConsistency] = None,
    private[scala] val timeout: Option[Duration] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val raw: Option[Map[String, Any]] = None,
    private[scala] val disableScoring: Boolean = false,
    private[scala] val includeLocations: Boolean = false
) {

  /** Sets the parent `RequestSpan`.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def parentSpan(value: RequestSpan): SearchOptions = {
    copy(parentSpan = Some(value))
  }

  /** Add a limit to the query on the number of rows it can return.
    *
    * If not set, the default is 10.
    *
    * @param limit the maximum number of rows to return.
    *
    * @return this SearchOptions for chaining.
    */
  def limit(limit: Int): SearchOptions = {
    copy(limit = Some(limit))
  }

  /** Set the number of rows to skip (eg. for pagination).
    *
    * If not set, the default is 0.
    *
    * @param skip the number of results to skip.
    *
    * @return this SearchOptions for chaining.
    */
  def skip(skip: Int): SearchOptions = {
    copy(skip = Some(skip))
  }

  /** Activates the explanation of each result hit in the response.  The default is false.
    *
    * @return this SearchOptions for chaining.
    */
  def explain(value: Boolean): SearchOptions = {
    copy(explain = Some(value))
  }

  /** Configures the highlighting of matches in the response.
    *
    * Highlighting is disabled by default.
    *
    * This drives the inclusion of the fragments in each [[com.couchbase.client.scala.search.result.SearchRow]] row.
    *
    * Note that to be highlighted, the fields must be stored in the FTS index.
    *
    * Both `style` and `fields` are optional.
    *
    * @param style  the [[HighlightStyle]] to apply.
    * @param fields the optional fields on which to highlight. If none, all fields where there is a match are
    *               highlighted.
    *
    * @return this SearchOptions for chaining.
    */
  def highlight(style: Option[HighlightStyle], fields: Option[Seq[String]]): SearchOptions = {
    copy(fields = fields, highlightStyle = style)
  }

  /** Configures the list of fields for which the whole value should be included in the response. If empty, no field
    * values are included.
    *
    * This drives the inclusion of the [[com.couchbase.client.scala.search.result.SearchRow.fieldsAs]] fields in each
    * `SearchRow` hit.
    *
    * Note that to be highlighted, the fields must be stored in the FTS index.
    *
    * @return this SearchOptions for chaining.
    */
  def fields(fields: Seq[String]): SearchOptions = {
    copy(fields = Some(fields))
  }

  /** Allows to limit the search query to a specific list of collection names.
    *
    * NOTE: this is only supported with server 7.0 and later.
    *
    * @param collectionNames the names of the collections this query should be limited to.
    * @return this SearchOptions for chaining.
    */
  def collections(collectionNames: Seq[String]): SearchOptions = {
    copy(fields = Some(collectionNames))
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
    * If no sort is provided, it is equal to sort(Seq("-_score")), since the server will sort it by score in descending
    * order.
    *
    * @param fields the fields that should take part in the sorting.
    *
    * @return this SearchOptions for chaining.
    */
  def sortByFields(fields: Seq[String]): SearchOptions = {
    val fieldSorts = for (field <- fields) yield SearchSort.FieldSort(field)
    copy(sort = Some(fieldSorts))
  }

  /** Configures the list of fields (including special fields) which are used for sorting purposes. If empty, the
    * default sorting (descending by score) is used by the server.
    *
    * See [[com.couchbase.client.scala.search.sort.SearchSort]] for the various sorting options.
    *
    * If no sort is provided, it is equal to sort(Seq("-_score")), since the server will sort it by score in descending
    * order.
    *
    * @param fields the fields that should take part in the sorting.
    *
    * @return this SearchOptions for chaining.
    */
  def sort(fields: Seq[SearchSort]): SearchOptions = {
    copy(sort = Some(fields))
  }

  /** Sets the consistency to consider for this FTS query.  See [[SearchScanConsistency]] for documentation.
    *
    * @return this SearchOptions for chaining.
    */
  def scanConsistency(scanConsistency: SearchScanConsistency): SearchOptions = {
    copy(scanConsistency = Some(scanConsistency))
  }

  /** Sets the search facets, which allow aggregating information collected on a particular result-set.
    *
    * @param facets the facets to use
    *
    * @return this SearchOptions for chaining.
    */
  def facets(facets: Map[String, SearchFacet]): SearchOptions = {
    copy(facets = Some(facets))
  }

  /** Allows providing custom JSON key/value pairs for advanced usage.
    *
    * If available, it is recommended to use the methods on this object to customize the query. This method should
    * only be used if no such setter can be found (i.e. if an undocumented property should be set or you are using
    * an older client and a new server-configuration property has been added to the cluster).
    *
    * Note that the values will be passed through a JSON encoder, so do not provide already encoded JSON as the value. If
    * you want to pass objects or arrays, you can use `JsonObject` and `JsonArray` respectively.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def raw(raw: Map[String, Any]): SearchOptions = {
    copy(raw = Some(raw))
  }

  /** If set to true, the server will not perform any scoring on the hits.
    *
    * This could provide a performance improvement in cases where the score is not required.
    *
    * This parameter only has an effect if the Couchbase Cluster version is 6.6.0 or above.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def disableScoring(value: Boolean): SearchOptions = {
    copy(disableScoring = value)
  }

  /** If set to true, will include the location in the search rows.
    *
    * @param value set to true if the locations should be included.
    * @return a copy of this with the change applied, for chaining.
    */
  def includeLocations(value: Boolean): SearchOptions = {
    copy(includeLocations = value)
  }

  /** Sets a maximum timeout for processing.
    *
    * @param timeout the duration of the timeout.
    * @return a copy of this with the change applied, for chaining.
    */
  def timeout(timeout: Duration): SearchOptions = {
    copy(timeout = Option(timeout))
  }

  private[scala] def toCore: CoreSearchOptions = {
    val x = this
    val common = CoreCommonOptions.ofOptional(
      timeout
        .map(v => com.couchbase.client.scala.util.DurationConversions.scalaDurationToJava(v))
        .asJava,
      retryStrategy.asJava,
      parentSpan.asJava
    )

    new CoreSearchOptions {
      override def collections(): util.List[String] = x.collections.getOrElse(Seq()).asJava

      override def consistency(): CoreSearchScanConsistency = x.scanConsistency match {
        case Some(SearchScanConsistency.NotBounded) => CoreSearchScanConsistency.NOT_BOUNDED
        case _                                      => null
      }

      override def consistentWith(): CoreMutationState = x.scanConsistency match {
        case Some(SearchScanConsistency.ConsistentWith(ms)) => ms.toCore
        case _                                              => null
      }

      override def disableScoring(): lang.Boolean = x.disableScoring

      override def explain(): lang.Boolean = x.explain.map(_.asInstanceOf[lang.Boolean]).orNull

      override def facets(): util.Map[String, CoreSearchFacet] =
        x.facets.map(_.map(k => k._1 -> k._2.toCore)).getOrElse(Map()).asJava

      override def fields(): util.List[String] = x.fields.getOrElse(Seq()).asJava

      override def highlightFields(): util.List[String] = x.highlightFields.getOrElse(Seq()).asJava

      override def highlightStyle(): CoreHighlightStyle =
        x.highlightStyle.map {
          case HighlightStyle.ANSI          => CoreHighlightStyle.HTML
          case HighlightStyle.HTML          => CoreHighlightStyle.ANSI
          case HighlightStyle.ServerDefault => CoreHighlightStyle.SERVER_DEFAULT
        }.orNull

      override def limit(): Integer = x.limit.map(_.asInstanceOf[Integer]).orNull

      override def raw(): JsonNode =
        x.raw.map(v => Mapper.convertValue(v, classOf[JsonNode])).orNull

      override def skip(): Integer = x.skip.map(_.asInstanceOf[Integer]).orNull

      override def sort(): util.List[CoreSearchSort] = x.sort.getOrElse(Seq()).map(_.toCore).asJava

      override def includeLocations(): lang.Boolean = x.includeLocations

      override def commonOptions(): CoreCommonOptions = common
    }
  }
}
