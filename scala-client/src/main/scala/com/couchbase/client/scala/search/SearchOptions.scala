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

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.{JsonArray, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.search.facet.SearchFacet
import com.couchbase.client.scala.search.queries._
import com.couchbase.client.scala.search.result.SearchRow
import com.couchbase.client.scala.search.sort.SearchSort

import scala.concurrent.duration.Duration
import scala.util.Success

/** Options to be used with a Full Text Search query.
  */
case class SearchOptions(
    private[scala] val limit: Option[Int] = None,
    private[scala] val skip: Option[Int] = None,
    private[scala] val explain: Option[Boolean] = None,
    private[scala] val highlightStyle: Option[HighlightStyle] = None,
    private[scala] val highlightFields: Option[Seq[String]] = None,
    private[scala] val fields: Option[Seq[String]] = None,
    private[scala] val sort: Option[JsonArray] = None,
    private[scala] val facets: Option[Map[String, SearchFacet]] = None,
    private[scala] val serverSideTimeout: Option[Duration] = None,
    private[scala] val deferredError: Option[RuntimeException] = None,
    private[scala] val scanConsistency: Option[SearchScanConsistency] = None,
    private[scala] val timeout: Option[Duration] = None,
    private[scala] val retryStrategy: Option[RetryStrategy] = None,
    private[scala] val parentSpan: Option[RequestSpan] = None,
    private[scala] val raw: Option[Map[String, Any]] = None
) {

  /** Sets the parent `RequestSpan`.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  @Volatile
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
    * This drives the inclusion of the [[SearchRow.fragments]] fragments
    * in each [[SearchRow]] row.
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
    * This drives the inclusion of the [[SearchRow.fields]] fields in each [[SearchRow]] hit.
    *
    * Note that to be highlighted, the fields must be stored in the FTS index.
    *
    * @return this SearchOptions for chaining.
    */
  def fields(fields: Seq[String]): SearchOptions = {
    copy(fields = Some(fields))
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
    * @param fields the fields that should take part in the sorting.
    *
    * @return this SearchOptions for chaining.
    */
  def sortByFields(fields: Seq[String]): SearchOptions = {
    val elems = sort.getOrElse(JsonArray.create)
    fields.foreach(s => elems.add(s))

    copy(sort = Some(elems))
  }

  /** Configures the list of fields (including special fields) which are used for sorting purposes. If empty, the
    * default sorting (descending by score) is used by the server.
    *
    * See [[SearchSort]] for the various sorting options.
    *
    * If no sort is provided, it is equal to sort("-_score"), since the server will sort it by score in descending
    * order.
    *
    * @param fields the fields that should take part in the sorting.
    *
    * @return this SearchOptions for chaining.
    */
  def sort(fields: Seq[SearchSort]): SearchOptions = {
    val params = JsonObject.create
    fields.foreach(field => field.injectParams(params))

    copy(sort = Some(sort.getOrElse(JsonArray.create).add(params)))
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
    * you want to pass objects or arrays, you can use [[JsonObject]] and [[JsonArray]] respectively.
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def raw(raw: Map[String, Any]): SearchOptions = {
    copy(raw = Some(raw))
  }

  /** Exports the whole query as a [[JsonObject]].
    */
  private[scala] def export(indexName: String, query: SearchQuery): JsonObject = {
    val result = JsonObject.create
    injectParams(indexName, result)

    val queryJson = JsonObject.create
    query.injectParamsAndBoost(queryJson)
    result.put("query", queryJson)
    result
  }

  /** Inject the top level parameters of a query into a prepared [[JsonObject]]
    * that represents the root of the query.
    *
    * @param queryJson the prepared { @link JsonObject} for the whole query.
    */
  private[scala] def injectParams(indexName: String, queryJson: JsonObject): Unit = {
    limit.foreach(v => if (v > 0) queryJson.put("size", v))
    skip.foreach(v => if (v > 0) queryJson.put("from", v))
    explain.foreach(v => queryJson.put("explain", v))

    highlightStyle.foreach(hs => {
      val highlight = JsonObject.create
      hs match {
        case HighlightStyle.ServerDefault =>
        case HighlightStyle.HTML          => highlight.put("style", "html")
        case HighlightStyle.ANSI          => highlight.put("style", "ansi")
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
      for (entry <- f) {
        val facetJson = JsonObject.create
        entry._2.injectParams(facetJson)
        facets.put(entry._1, facetJson)
      }
      queryJson.put("facets", facets)
    })
    val control = JsonObject.create
    serverSideTimeout.foreach(v => control.put("timeout", v))
    scanConsistency.foreach {
      case SearchScanConsistency.ConsistentWith(ms) =>
        val consistencyJson = JsonObject.create
        consistencyJson.put("level", "at_plus")
        consistencyJson.put("vectors", JsonObject(indexName -> convertMutationTokens(ms.tokens)))
        control.put("consistency", consistencyJson)
      case _ =>
    }

    if (!control.isEmpty) queryJson.put("ctl", control)

    raw.foreach(_.foreach(x => queryJson.put(x._1, x._2)))
  }

  private def convertMutationTokens(tokens: Seq[MutationToken]): JsonObject = {
    val result = JsonObjectSafe.create
    for (token <- tokens) {
      val tokenKey = token.partitionID.toString + "/" + token.partitionUUID

      result.numLong(tokenKey) match {
        case Success(seqno) =>
          // Only store the highest seqno for each vbucket
          if (seqno < token.sequenceNumber) {
            result.put(tokenKey, token.sequenceNumber)
          }
        case _ =>
          result.put(tokenKey, token.sequenceNumber)
      }
    }

    result.o
  }
}
