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

import scala.collection.GenMap
import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

/**
  * Represents the locations of a search result row. Locations show
  * where a given term occurs inside of a given field.
  *
  * @since 1.0.0
  */
case class RowLocations(locations: GenMap[String, GenMap[String, Seq[RowLocation]]])

object RowLocations {
  private[scala] def from(locationsJson: JsonObject): Try[RowLocations] = {
    Try({
      val hitLocations = collection.mutable.Map.empty[String, collection.mutable.Map[String, ArrayBuffer[RowLocation]]]

      for ( field <- locationsJson.names ) {
        val termsJson = locationsJson.obj(field)

        for ( term <- termsJson.names ) {
          val locsJson = termsJson.arr(term)
          for ( i <- 0 until locsJson.size ) {
            val loc = locsJson.obj(i)
            val pos = loc.numLong("pos")
            val start = loc.numLong("start")
            val end = loc.numLong("end")
            val arrayPositions: Array[Long] = loc.safe.arr("array_positions") match {
              case Success(arrayPositionsJson) =>
                Range(0, arrayPositionsJson.size).map(j => arrayPositionsJson.numLong(j).get).toArray
              case _ => Array.emptyLongArray
            }

            val byTerm: collection.mutable.Map[String, ArrayBuffer[RowLocation]] = hitLocations.getOrElseUpdate(field,
              collection.mutable.Map.empty[String, ArrayBuffer[RowLocation]])

            val list: ArrayBuffer[RowLocation] = byTerm.getOrElseUpdate(term, ArrayBuffer.empty)

            list += RowLocation(field, term, pos, start, end, arrayPositions)
          }
        }
      }

      RowLocations(hitLocations)
    })
  }

}

/** An FTS result row location indicates at which position a given term occurs inside a given field.
  * In case the field is an array, `arrayPositions`` will indicate which index/indices in the
  * array contain the term.
  *
  * @since 1.0.0
  */
case class RowLocation(field: String,
                       term: String,
                       pos: Long,
                       start: Long,
                       end: Long,
                       arrayPositions: Array[Long])