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
package com.couchbase.client.scala.search.queries

import com.couchbase.client.scala.json.{JsonArray, JsonObject}

/** An FTS query that matches several terms (a "phrase") as is. The order of the terms matter and no
  * further processing is applied to them, so they must appear in the index exactly as provided.
  * Usually for debugging purposes, prefer [[MatchPhraseQuery]].
  *
  * @param terms The mandatory list of terms that must exactly match in the index.  Must contain at least one term.
  *
  * @since 1.0.0
  */
case class PhraseQuery(
    private[scala] val terms: Seq[String],
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends SearchQuery {

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): PhraseQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): PhraseQuery = {
    copy(boost = Some(boost))
  }

  override protected def injectParams(input: JsonObject): Unit = {
    input.put("terms", JsonArray(terms: _*))
    boost.foreach(v => input.put("boost", v))
    field.foreach(v => input.put("field", v))
  }
}
