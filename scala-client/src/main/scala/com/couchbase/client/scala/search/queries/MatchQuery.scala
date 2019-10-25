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

import com.couchbase.client.scala.json.JsonObject

/** An FTS query that matches a given term, applying further processing to it
  * like analyzers, stemming and even fuzziness.
  *
  * @param matchStr input string to be matched against.
  *
  * @since 1.0.0
  */
case class MatchQuery(
    private[scala] val matchStr: String,
    private[scala] val fuzziness: Option[Int] = None,
    private[scala] val prefixLength: Option[Int] = None,
    private[scala] val analyzer: Option[String] = None,
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends SearchQuery {

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): MatchQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): MatchQuery = {
    copy(boost = Some(boost))
  }

  /** Analyzers are used to transform input text into a stream of tokens for indexing. The Server comes with built-in
    * analyzers and the users can create their own.
    *
    * @param analyzer The string here is the name of the analyzer used.
    *
    * @return a copy of this, for chaining
    */
  def analyzer(analyzer: String): MatchQuery = {
    copy(analyzer = Some(analyzer))
  }

  /** This parameter can be used to require that the term also have the same prefix of the specified length.
    *
    * @return a copy of this, for chaining
    */
  def prefixLength(prefixLength: Int): MatchQuery = {
    copy(prefixLength = Some(prefixLength))
  }

  /** If a fuzziness is specified, variations of the term can be searched. Additionally, if fuzziness is enabled then
    * the prefix_length parameter is also taken into account (see below).
    *
    * For now the server interprets the fuzziness factor as a "Levenshtein edit distance" to apply on the term.
    *
    * @return a copy of this, for chaining
    */
  def fuzziness(fuzziness: Int): MatchQuery = {
    copy(fuzziness = Some(fuzziness))
  }

  override protected def injectParams(input: JsonObject): Unit = {
    input.put("match", matchStr)
    boost.foreach(v => input.put("boost", v))
    field.foreach(v => input.put("field", v))
    analyzer.foreach(v => input.put("analyzer", v))
    prefixLength.foreach(v => input.put("prefix_length", v))
    fuzziness.foreach(v => input.put("fuzziness", v))
  }
}
