/*
 * Copyright (c) 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala.search.queries

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.search.queries.CoreCustomScoreQuery
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.transformers.JacksonTransformers

import scala.jdk.CollectionConverters._

/** A search query that passes the results of an inner query through
  * a user-defined JavaScript scoring function, which assigns a new score to each result.
  *
  * The function takes two parameters (`doc` and `params`) and returns a number: the new score of the document. For example:
  *
  * {{{
  * function hotel_score(doc, params) {
  *   const maxPrice = params.max_price || 1000;
  *   const f = doc.fields || {};
  *   const price = (f.price !== undefined) ? f.price : maxPrice;
  *   return doc.score * (2.0 - Math.min(price / maxPrice, 1.0));
  * }
  * }}}
  *
  * The function can access a document field only if the field is listed via `fields`.
  *
  * Custom script queries are disabled by default. A server administrator must enable them
  * by setting the Search service's `customScriptQueriesEnabled` manager option to `"true"`.
  *
  * @param query  the query whose results are passed through the function
  * @param source the JavaScript source code of the function
  */
@SinceCouchbase("8.5")
case class CustomScoreQuery(
    private[scala] val query: SearchQuery,
    private[scala] val source: String,
    private[scala] val fields: Seq[String] = Seq.empty,
    private[scala] val parameters: Option[JsonObject] = None
) extends SearchQuery {

  /** Specifies the document fields made available to the function.
    * Pass `"*"` to make all fields available.
    *
    * If not set, the function does not have access to any document fields.
    *
    * @return a copy of this, for chaining
    */
  def fields(fields: String*): CustomScoreQuery = {
    copy(fields = fields)
  }

  /** Specifies parameters made available to the function via its `params` argument.
    *
    * @return a copy of this, for chaining
    */
  def parameters(parameters: JsonObject): CustomScoreQuery = {
    copy(parameters = Some(parameters))
  }

  override private[scala] def toCore =
    new CoreCustomScoreQuery(
      query.toCore,
      source,
      fields.asJava,
      parameters
        .map(p => JacksonTransformers.MAPPER.convertValue(p, classOf[java.util.Map[String, Any]]))
        .orNull
    )
}
