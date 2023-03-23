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

import com.couchbase.client.core.api.search.queries.CoreMatchPhraseQuery

/**
  * An FTS query that matches several given terms (a "phrase"), applying further processing
  * like analyzers to them.
  *
  * @param matchPhrase The input phrase to be matched against.
  *
  * @since 1.0.0
  */
case class MatchPhraseQuery(
    private[scala] val matchPhrase: String,
    private[scala] val analyzer: Option[String] = None,
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends SearchQuery {

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): MatchPhraseQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): MatchPhraseQuery = {
    copy(boost = Some(boost))
  }

  /** Analyzers are used to transform input text into a stream of tokens for indexing. The Server comes with built-in
    * analyzers and the users can create their own.
    *
    * @param analyzer The string here is the name of the analyzer used.
    *
    * @return a copy of this, for chaining
    */
  def analyzer(analyzer: String): MatchPhraseQuery = {
    copy(analyzer = Some(analyzer))
  }

  override private[scala] def toCore =
    new CoreMatchPhraseQuery(
      matchPhrase,
      analyzer.orNull,
      field.orNull,
      boost.map(_.asInstanceOf[java.lang.Double]).orNull
    )
}
