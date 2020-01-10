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

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.msg.search.SearchChunkRow
import com.couchbase.client.scala.codec.{Conversions, JsonDeserializer}
import com.couchbase.client.scala.json.JsonObjectSafe
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** The results of an FTS query.
  *
  * @param facets          any search facets returned
  * @param metaData            any additional information related to the FTS query
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class SearchResult(
    private[scala] val _rows: Seq[SearchRow],
    facets: Map[String, SearchFacetResult],
    metaData: SearchMetaData
) {

  /** All returned rows.  All rows are buffered from the FTS service first.
    *
    * @return either `Success` if all rows could be decoded successfully, or a Failure containing the first error
    */
  def rows: Seq[SearchRow] = _rows
}

/** The results of an FTS query, as returned by the reactive API.
  *
  * @param rows            a Flux of any returned rows.  If the FTS service returns an error while returning the
  *                        rows, it will be raised on this Flux
  * @param meta            any additional information related to the FTS query
  */
case class ReactiveSearchResult(
    rows: SFlux[SearchRow],
    facets: SMono[Map[String, SearchFacetResult]],
    meta: SMono[SearchMetaData]
)
