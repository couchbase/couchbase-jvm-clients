/*
 * Copyright (c) 2025 Couchbase, Inc.
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

import com.couchbase.client.core.api.search.result.CoreReactiveSearchResult
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.jdk.CollectionConverters._

/** The results of an FTS query, as returned by the reactive API. */
case class ReactiveSearchResult (private val internal: CoreReactiveSearchResult) {
  def rows: SFlux[SearchRow] = SFlux(internal.rows).map(row => SearchRow(row))

  /** Any additional information related to the FTS query. */
  def metaData: SMono[SearchMetaData] = SMono(internal.metaData).map(md => SearchMetaData(md))

  /** Any search facets returned. */
  def facets: SMono[Map[String, SearchFacetResult]] =
    SMono(internal.facets).map(_.asScala).map(v => v.map(k => k._1 -> convert(k._2)).toMap)
}
