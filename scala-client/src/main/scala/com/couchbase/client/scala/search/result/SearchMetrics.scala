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
import com.couchbase.client.core.msg.search.{SearchChunkTrailer, SearchResponse}
import com.couchbase.client.scala.json.JsonObjectSafe

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** Metrics and status of a given FTS request.
  *
  * @param took        how long a request took executing on the server side
  * @param totalRows   number of rows returned
  * @param maxScore    the largest score amongst the rows.
  * @param totalPartitionCount   the total number of FTS pindexes that were queried.
  * @param successPartitionCount the number of FTS pindexes queried that successfully answered.
  * @param errorPartitionCount   the number of FTS pindexes queried that gave an error. If &gt; 0,
  */
case class SearchMetrics(
    took: Duration,
    totalRows: Long,
    maxScore: Double,
    totalPartitionCount: Long,
    successPartitionCount: Long,
    errorPartitionCount: Long
)
