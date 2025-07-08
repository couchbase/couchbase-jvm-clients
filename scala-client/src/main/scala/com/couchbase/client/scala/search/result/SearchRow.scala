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
package com.couchbase.client.scala.search.result

import com.couchbase.client.core.api.search.result.CoreSearchRow
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.util.CoreCommonConverters

import java.nio.charset.StandardCharsets
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.util.Try

/** An FTS row (or "hit").
  *
  * @since 1.0.0
  */
case class SearchRow (private val internal: CoreSearchRow) {

  /** The name of the FTS index that gave this result. */
  def index: String = internal.index

  /** The id of the matching document. */
  def id: String = internal.id

  /** The score of this hit. */
  def score: Double = internal.score

  /** The value of each requested field.
    *
    * It can be returned in any supported JSON type, e.g. `com.couchbase.client.scala.json.JsonObject`.
    * See a full list at [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
    */
  def fieldsAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    deserializer.deserialize(internal.fields)
  }

  /** The location of this hit, if it was requested with
    * [[com.couchbase.client.scala.search.SearchOptions.includeLocations]]. */
  def locations: Option[SearchRowLocations] = {
    internal.locations.asScala.map(locations => SearchRowLocations(locations))
  }

  /** Fragments contain the results of any requested highlighting. */
  def fragments: collection.Map[String, Seq[String]] = {
    internal.fragments.asScala.toMap.map(k => k._1 -> k._2.asScala.toSeq)
  }

  /** If `explain` was set on the `SearchQuery` this will return an explanation of the match.
    *
    * The structure of the returned JSON is unspecified, and is not part of the public committed API.
    *
    * It can be returned in any supported JSON type, e.g. `com.couchbase.client.scala.json.JsonObject`.
    * See a full list at [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
    */
  def explanationAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    val bytes =
      if (internal.explanation.isEmpty) "{}".getBytes(StandardCharsets.UTF_8)
      else internal.explanation()
    deserializer.deserialize(bytes)
  }
}
