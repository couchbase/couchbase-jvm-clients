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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8

import com.couchbase.client.core.error.DecodingFailureException
import com.couchbase.client.core.msg.search.SearchChunkRow
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.transformers.JacksonTransformers

import scala.util.{Success, Try}

/** An FTS row (or "hit")
  *
  * @param index     The name of the FTS pindex that gave this result.
  * @param id        The id of the matching document.
  * @param score     The score of this hit.
  * @param locations This rows's location, as a [[SearchRowLocations]].
  * @param fragments The fragments for each field that was requested as highlighted.  A fragment is an extract of the
  *                  field's value where the matching terms occur.  Matching terms are surrounded by a
  *                  <code>&lt;match&gt;</code> tag.
  * @param fields    The value of each requested field, as a map.  The key is the field.
  *
  * @since 1.0.0
  */
case class SearchRow(
    index: String,
    id: String,
    score: Double,
    private val _explanation: Try[Array[Byte]],
    locations: Option[SearchRowLocations],
    fragments: collection.Map[String, Seq[String]],
    private[scala] val fields: Try[Array[Byte]]
) {

  /** If `explain` was set on the `SearchQuery` this will return an explanation of the match.
    *
    * It can be returned in any supported JSON type, e.g. `com.couchbase.client.scala.json.JsonObject`.
    * See a full list at [[https://docs.couchbase.com/scala-sdk/1.0/howtos/json.html these JSON docs]]
    */
  def explanationAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    _explanation.flatMap(v => deserializer.deserialize(v))
  }

  def fieldsAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    fields.flatMap(f => deserializer.deserialize(f))
  }
}

object SearchRow {
  /* Converts a SearchChunkRow to a SearchQueryRow.
   *
   * Allowed to throw on failure.
   * */
  private[scala] def fromResponse(row: SearchChunkRow): SearchRow =
    try {
      val hit  = JacksonTransformers.MAPPER.readValue(row.data, classOf[JsonObject])
      val safe = hit.safe

      val index = hit.str("index")
      val id    = hit.str("id")
      val score = hit.numDouble("score")

      val explanationJson: Try[Array[Byte]] = safe
        .obj("explanation")
        .map(o => o.toString.getBytes(UTF_8))

      val locations: Try[SearchRowLocations] = safe
        .obj("locations")
        .flatMap(x => SearchRowLocations.from(x.o))

      val fragments: collection.Map[String, Seq[String]] = safe.obj("fragments") match {
        case Success(fragmentsJson) =>
          fragmentsJson.names
            .map(field => {
              val fragment: Seq[String] = fragmentsJson.arr(field) match {
                case Success(fragmentJson) => fragmentJson.toSeq.map(_.toString)
                case _                     => Seq.empty
              }

              field -> fragment
            })
            .toMap
        case _ => Map.empty
      }

      val fields =
        safe.obj("fields").map(fields => fields.toString.getBytes(StandardCharsets.UTF_8))

      new SearchRow(index, id, score, explanationJson, locations.toOption, fragments, fields)
    } catch {
      case e: IOException =>
        throw new DecodingFailureException(
          "Failed to decode row '" + new String(row.data, UTF_8) + "'",
          e
        )
    }

}
