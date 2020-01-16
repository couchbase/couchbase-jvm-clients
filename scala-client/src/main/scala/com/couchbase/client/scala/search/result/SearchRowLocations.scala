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

import com.couchbase.client.scala.json.JsonObject
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

/**
  * Represents the locations of a search result row. Locations show
  * where a given term occurs inside of a given field.
  *
  * @since 1.0.0
  */
case class SearchRowLocations(
    private val locations: collection.Map[String, collection.Map[String, collection.Seq[
      SearchRowLocation
    ]]]
) {

  /** Gets all locations, for any field and term. */
  def getAll: Seq[SearchRowLocation] = {
    locations.flatMap(_._2).flatMap(_._2).toSeq
  }

  /** Gets all locations for a given field (any term). */
  def get(field: String): Seq[SearchRowLocation] = {
    locations
      .get(field)
      .map(_.flatMap(_._2).toSeq)
      .getOrElse(Seq.empty)
  }

  /** Gets all locations for a given field and term. */
  def get(field: String, term: String): Seq[SearchRowLocation] = {
    locations
      .get(field)
      .flatMap(_.get(term))
      .getOrElse(Seq.empty)
      .toSeq
  }

  /** Gets all fields in these locations. */
  def fields: Seq[String] = {
    locations.keys.toSeq
  }

  /** Gets all terms in these locations. */
  def terms: collection.Set[String] = {
    locations.flatMap(_._2).keySet
  }

  /** Gets all terms for a given field. */
  def termsFor(field: String): Seq[String] = {
    locations
      .get(field)
      .map(_.keys.toSeq)
      .getOrElse(Seq.empty)
  }
}

object SearchRowLocations {
  private[scala] def from(locationsJson: JsonObject): Try[SearchRowLocations] = {
    Try({
      val hitLocations =
        mutable.Map.empty[String, mutable.Map[String, ArrayBuffer[SearchRowLocation]]]

      for (field <- locationsJson.names) {
        val termsJson = locationsJson.obj(field)

        for (term <- termsJson.names) {
          val locsJson = termsJson.arr(term)
          for (i <- 0 until locsJson.size) {
            val loc   = locsJson.obj(i)
            val pos   = loc.num("pos")
            val start = loc.num("start")
            val end   = loc.num("end")
            val arrayPositions: Option[Seq[Int]] = loc.safe.arr("array_positions") match {
              case Success(arrayPositionsJson) =>
                Some(
                  Range(0, arrayPositionsJson.size)
                    .map(j => arrayPositionsJson.num(j).get)
                )
              case _ => None
            }

            val byTerm: mutable.Map[String, ArrayBuffer[SearchRowLocation]] =
              hitLocations.getOrElseUpdate(
                field,
                mutable.Map.empty[String, ArrayBuffer[SearchRowLocation]]
              )

            val list: ArrayBuffer[SearchRowLocation] =
              byTerm.getOrElseUpdate(term, ArrayBuffer.empty)

            list += SearchRowLocation(field, term, pos, start, end, arrayPositions)
          }
        }
      }

      SearchRowLocations(hitLocations)
    })
  }

}

/** An FTS result row location indicates at which position a given term occurs inside a given field.
  * In case the field is an array, `arrayPositions` will indicate which index/indices in the
  * array contain the term.
  *
  * @since 1.0.0
  */
case class SearchRowLocation(
    field: String,
    term: String,
    pos: Int,
    start: Int,
    end: Int,
    arrayPositions: Option[Seq[Int]]
)
