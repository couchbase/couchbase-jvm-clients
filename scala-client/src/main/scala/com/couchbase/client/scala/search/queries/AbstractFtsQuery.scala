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
import com.couchbase.client.scala.search.SearchQuery


/** Base class for FTS queries that are composite, compounding several other [[AbstractFtsQuery]].
  *
  * @since 1.0.0
  */
trait AbstractCompoundQuery extends AbstractFtsQuery

/** A base class for all FTS query classes. Exposes the common FTS query parameters.
  * In order to instantiate various flavors of queries, look at concrete classes or
  * static factory methods in [[SearchQuery]].
  *
  * @since 1.0.0
  */
private[scala] trait AbstractFtsQuery {

  /** Injects the query's parameters (including query-specific parameters)
    * into a prepared [[JsonObject]].
    *
    * @param input the prepared JsonObject to receive the parameters.
    */
  private[scala] def injectParamsAndBoost(input: JsonObject): Unit = {
    injectParams(input)
  }

  /** Override to inject query-specific parameters when doing the [[SearchQuery#export()]].
    *
    * @param input the prepared { @link JsonObject} that will represent the query.
    */
  protected def injectParams(input: JsonObject): Unit

  /** @return the String representation of the FTS query, which is its JSON representation without global parameters.
    */
  override def toString: String = {
    val json = JsonObject.create
    injectParamsAndBoost(json)
    json.toString
  }
}











