/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.client.scala.search.vector

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.api.search.queries.CoreSearchRequest
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.scala.search.queries.{MatchNoneQuery, SearchQuery}

import scala.util.{Failure, Success, Try}

/** A request to the FTS (Full Text Search) service, allowing a [[SearchQuery]] and/or a [[VectorSearch]] to be run.
  *
  * If both are provided, the FS service will merge the results.
  */
@Volatile
case class SearchRequest private (
    private val searchQuery: Option[SearchQuery] = None,
    private val vectorSearch: Option[VectorSearch] = None,
    private val deferredError: Option[Throwable] = None
) {

  /** Include a [[VectorSearch]].
    *
    * A maximum of one [[VectorSearch]] and one [[SearchQuery]] may be specified.
    *
    * @return a copy of this, for chaining.
    */
  def vectorSearch(vectorSearch: VectorSearch): SearchRequest = {
    this.vectorSearch match {
      case Some(_) =>
        copy(deferredError = Some(
          new InvalidArgumentException(
            "Cannot specify multiple VectorSearch objects.  Note that a single VectorSearch can take multiple VectorQuery objects, allowing multiple vector queries to be run.",
            null,
            null
          )
        )
        )
      case None =>
        copy(vectorSearch = Some(vectorSearch))
    }
  }

  /** Include a [[SearchQuery]].
    *
    * A maximum of one [[VectorSearch]] and one [[SearchQuery]] may be specified.
    *
    * @return a copy of this, for chaining.
    */
  def searchQuery(searchQuery: SearchQuery): SearchRequest = {
    this.searchQuery match {
      case Some(_) =>
        copy(deferredError = Some(
          new InvalidArgumentException(
            "Cannot specify multiple SearchQuery objects.  Note that a BooleanQuery can be used to combine multiple SearchQuery objects.",
            null,
            null
          )
        )
        )
      case None =>
        copy(searchQuery = Some(searchQuery))
    }
  }

  private[scala] def toCore: Try[CoreSearchRequest] =
    deferredError match {
      case Some(err) => Failure(err)
      case None =>
        Success(
          new CoreSearchRequest(
            searchQuery.map(_.toCore).orNull,
            vectorSearch.map(_.toCore).orNull
          )
        )
    }
}

@Volatile
object SearchRequest {

  /** Execute an FTS [[SearchQuery]]. */
  def searchQuery(searchQuery: SearchQuery): SearchRequest =
    new SearchRequest(Some(searchQuery), None)

  /** Execute a [[VectorSearch]]. */
  def vectorSearch(vectorSearch: VectorSearch): SearchRequest =
    new SearchRequest(None, Some(vectorSearch))
}
