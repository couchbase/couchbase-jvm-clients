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
package com.couchbase.client.scala

import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.scala.AsyncScopeBase
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import scala.concurrent.Future
import scala.util.{Failure, Success}

class AsyncScope private[scala] (
                                  private[scala] val scopeName: String,
                                  val bucketName: String,
                                  private[scala] val couchbaseOps: CoreCouchbaseOps,
                                  private[scala] val environment: ClusterEnvironment
                                ) extends AsyncScopeBase {

  /** Performs a Full Text Search (FTS) query.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * Use this to access scoped FTS indexes, and [[Cluster.search]] for global indexes.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    * @param options   see [[com.couchbase.client.scala.search.SearchOptions]]
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  @SinceCouchbase("7.6")
  def search(
      indexName: String,
      request: SearchRequest,
      options: SearchOptions = SearchOptions.Default
  ): Future[SearchResult] = {
    request.toCore match {
      case Failure(err) => Future.failed(err)
      case Success(req) =>
        convert(searchOps.searchAsync(indexName, req, options.toCore))
          .map(result => SearchResult(result))
    }
  }
}
