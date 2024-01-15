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
import com.couchbase.client.core.api.search.vector.CoreVectorSearch
import scala.collection.JavaConverters._

/** Allows one or more [[VectorQuery]]s to be executed. */
@Volatile
case class VectorSearch private (
    private val vectorQueries: Iterable[VectorQuery],
    private val vectorSearchOptions: Option[VectorSearchOptions] = None
) {

  /** Controls how this [[VectorSearch]] is executed.
    *
    * @return a copy of this, for chaining.
    */
  def vectorSearchOptions(vectorSearchOptions: VectorSearchOptions): VectorSearch =
    copy(vectorSearchOptions = Some(vectorSearchOptions))

  private[scala] def toCore: CoreVectorSearch =
    new CoreVectorSearch(vectorQueries.map(_.toCore).toList.asJava,
      vectorSearchOptions.map(_.toCore).orNull)
}

@Volatile
object VectorSearch {

  /** Create a [[VectorSearch]] containing a single [[VectorQuery]]. */
  def apply(vectorQuery: VectorQuery): VectorSearch =
    apply(Seq(vectorQuery))

  /** Create a [[VectorSearch]] containing multiple [[VectorQuery]]s. */
  def apply(vectorQueries: Iterable[VectorQuery]): VectorSearch =
    new VectorSearch(vectorQueries)
}
