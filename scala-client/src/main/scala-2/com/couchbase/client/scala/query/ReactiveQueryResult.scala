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

package com.couchbase.client.scala.query

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.query.QueryChunkRow
import com.couchbase.client.core.util.Golang
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.util.{DurationConversions, RowTraversalUtil}
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** The results of a N1QL query, as returned by the reactive API.
  *
  * @param metaData            any additional information related to the query
  */
case class ReactiveQueryResult(
    private[scala] val rows: SFlux[QueryChunkRow],
    metaData: SMono[QueryMetaData]
) {

  /** A Flux of any returned rows, streamed directly from the query service.  If the query service returns an error
    * while returning the rows, it will be raised on this.
    *
    * @tparam T any supported type; see [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]] for a full list
    */
  def rowsAs[T](implicit deserializer: JsonDeserializer[T]): SFlux[T] = {
    rows.map(row => {
      // The .get will raise an exception as .onError on the flux
      deserializer.deserialize(row.data()).get
    })
  }
}
