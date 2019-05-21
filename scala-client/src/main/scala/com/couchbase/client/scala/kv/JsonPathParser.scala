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
package com.couchbase.client.scala.kv

import com.couchbase.client.scala.json.{PathArray, PathElement, PathObjectOrField}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Parses a JSON projections string into a Seq of `PathElement`s.
  *
  * E.g. "foo.bar" or "foo[2].bar"
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] object JsonPathParser {
  def parse(path: String): Try[Seq[PathElement]] = {
    def debugPos(idx: Int) = s"position $idx"

    try {
      var elemIdx = 0
      var idx = 0
      val len = path.length
      val ret = ArrayBuffer.empty[PathElement]

      // This implementation is imperative for performance
      while (idx < len) {
        val ch = path.charAt(idx)
        idx += 1

        if (ch == '.') {
          val first = path.substring(elemIdx, idx - 1)
          elemIdx = idx
          ret += PathObjectOrField(first)

        }
        else if (ch == '[') {
          val arrayIdxStart = idx
          var out: Option[Int] = None

          while (idx < len && out.isEmpty) {
            val arrayIdxCh = path.charAt(idx)

            if (arrayIdxCh == ']') {
              val arrayIdxStr = path.substring(arrayIdxStart, idx)
              out = Some(arrayIdxStr.toInt)
            }
            else if (!(arrayIdxCh >= '0' && arrayIdxCh <= '9')) {
              throw new IllegalArgumentException(s"Found unexpected non-digit in middle of array index at ${debugPos(idx)}")
            }

            idx += 1
          }

          out match {
            case Some(v) =>
              val first = path.substring(elemIdx, arrayIdxStart - 1)
              elemIdx = idx
              ret += PathArray(first, v)

            case _ =>
              throw new IllegalArgumentException(s"Could not find ']' to complete array index")
          }

          // In "foo[2].bar", skip over the .
          if (idx < len && path.charAt(idx) == '.') {
            idx += 1
            elemIdx = idx
          }
        }
      }

      if (idx != elemIdx) {
        val first = path.substring(elemIdx, idx)
        ret += PathObjectOrField(first)
      }

      Success(ret)
    }
    catch {
      case NonFatal(err) => Failure(err)
    }
  }
}
