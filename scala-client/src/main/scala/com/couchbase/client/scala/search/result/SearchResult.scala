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

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.msg.search.SearchChunkRow
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.JsonObjectSafe
import reactor.core.scala.publisher.{Flux, Mono}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** The results of an FTS query.
  *
  * @param rows            all rows returned from the FTS service
  * @param errors          any execution error that happened.  Note that it is possible to have both rows and errors.
  * @param meta            any additional information related to the FTS query
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class SearchResult(private[scala] val _rows: Try[Seq[SearchQueryRow]],
                        errors: Seq[RuntimeException],
                        meta: SearchMeta) {
  /** Returns an [[Iterator]] of any returned rows.  All rows are buffered from the FTS service first.
    *
    * The return type is of `Iterator[Try[SearchQueryRow]]` in case any row cannot be decoded.  See `allRowsAs` for a more
    * convenient interface that does not require handling individual row decode errors.
    */
  def rows: Try[Iterator[SearchQueryRow]] = {
    _rows.map(_.iterator)
  }

  /** All returned rows.  All rows are buffered from the FTS service first.
    *
    * @return either `Success` if all rows could be decoded successfully, or a Failure containing the first error
    */
  def allRows: Try[Seq[SearchQueryRow]] = _rows

  /** All returned rows.  All rows are buffered from the FTS service first.
    *
    * @return either `Success` if all rows could be decoded successfully, or a Failure containing the first error.  Note
    *         that if `errors` is nonEmpty, the first error from that will be returned.
    */
  def allRowsOrErrors: Try[Seq[SearchQueryRow]] = {
    if (errors.nonEmpty) {
      Failure(errors.head)
    }
    else _rows
  }
}

/** The results of an FTS query, as returned by the reactive API.
  *
  * @param rows            a Flux of any returned rows.  If the FTS service returns an error while returning the
  *                        rows, it will be raised on this Flux
  * @param meta            any additional information related to the FTS query
  */
case class ReactiveSearchResult(private[scala] val rows: Flux[SearchChunkRow],
                                meta: Mono[SearchMeta]) {
  /** Return all rows, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def rowsAs[T](implicit ev: Conversions.Decodable[T]): Flux[T] = {
    rows.map(row => ev.decode(row.data(), Conversions.JsonFlags) match {
      case Success(v) => v
      case Failure(err) => throw err
    })
  }
}

/** Metrics of a given FTS request.
  *
  * @param took        how long a request took executing on the server side
  * @param totalRows   number of rows returned
  * @param maxScore    the largest score amongst the rows.
  */
case class SearchMetrics(took: Duration,
                         totalRows: Long,
                         maxScore: Double)

/** Represents the status of a FTS query.
  *
  * @param totalCount   the total number of FTS pindexes that were queried.
  * @param successCount the number of FTS pindexes queried that successfully answered.
  * @param errorCount   the number of FTS pindexes queried that gave an error. If &gt; 0,
  */
case class SearchStatus(totalCount: Long,
                        successCount: Long,
                        errorCount: Long) {

  /** If all FTS indexes answered successfully. */
  def isSuccess: Boolean = errorCount == 0
}

/** Additional information returned by the FTS service after any rows and errors.
  *
  * @param metrics         metrics related to the FTS request, if they are available
  * @param warnings        any warnings returned from the FTS service
  * @param status          the raw status string returned from the FTS service
  */
case class SearchMeta(status: SearchStatus,
                      metrics: SearchMetrics)

private[scala] object SearchStatus {
  def fromBytes(in: Array[Byte]): SearchStatus = {
    JsonObjectSafe.fromJson(new String(in, CharsetUtil.UTF_8)) match {
      case Success(jo) =>
        SearchStatus(
          jo.numLong("total").getOrElse(0),
          jo.numLong("failed").getOrElse(0),
          jo.numLong("successful").getOrElse(0)
        )

      case Failure(err) =>
        SearchStatus(0, 0, 0)
    }
  }
}
