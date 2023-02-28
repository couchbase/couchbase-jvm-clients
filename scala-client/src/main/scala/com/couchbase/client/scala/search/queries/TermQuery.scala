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

import com.couchbase.client.core.api.search.queries.CoreTermQuery

/**
  * An FTS query that matches terms (without further analysis). Usually for debugging purposes,
  * prefer using [[MatchQuery]].
  *
  * @param term the exact string that will be searched for in the index
  *
  * @since 1.0.0
  */
case class TermQuery(
    private[scala] val term: String,
    private[scala] val fuzziness: Option[Int] = None,
    private[scala] val prefixLength: Option[Int] = None,
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends SearchQuery {

  /** If a fuzziness is specified, variations of the term can be searched. Additionally, if fuzziness is enabled then
    * the prefix_length parameter is also taken into account (see below).
    *
    * For now the server interprets the fuzziness factor as a "Levenshtein edit distance" to apply on the term.
    *
    * @return a copy of this, for chaining
    */
  def fuzziness(fuzziness: Int): TermQuery = {
    copy(fuzziness = Some(fuzziness))
  }

  /** The prefix length only makes sense when fuzziness is enabled (see above). It allows to apply the fuzziness only on
    *  the part of the term that is after the prefix_length character mark.
    *
    * For example, with the term "something" and a prefix length of 4, only the "thing" part of the term will be
    * fuzzy-searched, and rows must start with "some".
    *
    * @return a copy of this, for chaining
    */
  def prefixLength(prefixLength: Int): TermQuery = {
    copy(prefixLength = Some(prefixLength))
  }

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): TermQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): TermQuery = {
    copy(boost = Some(boost))
  }

  override private[scala] def toCore =
    new CoreTermQuery(term,
      field.orNull,
      fuzziness.map(_.asInstanceOf[Integer]).orNull,
      prefixLength.map(_.asInstanceOf[Integer]).orNull,
      boost.map(_.asInstanceOf[java.lang.Double]).orNull)
}
