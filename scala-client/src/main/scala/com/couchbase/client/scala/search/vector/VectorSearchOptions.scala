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

import com.couchbase.client.core.api.search.vector.{
  CoreVectorQueryCombination,
  CoreVectorSearchOptions
}

/** Specifies how multiple [[VectorQuery]]s in a [[VectorSearch]] are combined. */
sealed trait VectorQueryCombination
object VectorQueryCombination {

  /** All vector queries must match a document for it to included. */
  case object And extends VectorQueryCombination

  /** Any vector query may match a document for it to included. */
  case object Or extends VectorQueryCombination
}

/** Options related to executing a [[VectorSearch]]. */
case class VectorSearchOptions (
    private val vectorQueryCombination: Option[VectorQueryCombination] = None
) {

  /** Specifies how multiple [[VectorQuery]]s in a [[VectorSearch]] are combined.
    *
    * @return this, for chaining.
    */
  def vectorQueryCombination(vectorQueryCombination: VectorQueryCombination): VectorSearchOptions =
    copy(vectorQueryCombination = Some(vectorQueryCombination))

  private[scala] def toCore: CoreVectorSearchOptions =
    new CoreVectorSearchOptions(vectorQueryCombination.map {
      case VectorQueryCombination.And => CoreVectorQueryCombination.AND
      case VectorQueryCombination.Or  => CoreVectorQueryCombination.OR
    }.orNull)
}

object VectorSearchOptions {
  def apply(): VectorSearchOptions =
    new VectorSearchOptions()
}
