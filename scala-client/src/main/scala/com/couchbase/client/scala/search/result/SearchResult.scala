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

package com.couchbase.client.scala.search.result

import com.couchbase.client.core.api.search.result.{CoreReactiveSearchResult, CoreSearchResult}
import com.couchbase.client.scala.util.CoreCommonConverters.convert

import scala.jdk.CollectionConverters._

/** The results of an FTS query.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class SearchResult(private val internal: CoreSearchResult) {

  /** All returned rows.  All rows are buffered from the FTS service first.
    *
    * @return either `Success` if all rows could be decoded successfully, or a Failure containing the first error
    */
  def rows: Seq[SearchRow] = internal.rows.asScala.toSeq.map(row => SearchRow(row))

  /** Any additional information related to the FTS query. */
  def metaData: SearchMetaData = SearchMetaData(internal.metaData)

  /** Any search facets returned. */
  def facets: Map[String, SearchFacetResult] =
    internal.facets.asScala.map(k => k._1 -> convert(k._2)).toMap
}
