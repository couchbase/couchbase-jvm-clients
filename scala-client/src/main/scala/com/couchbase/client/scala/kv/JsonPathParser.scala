package com.couchbase.client.scala.kv

import com.couchbase.client.scala.json.{PathArray, PathElement, PathObjectOrField}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Parses a JSON projections string into a Seq of `PathElement`s.
  *
  * E.g. "foo.bar" or "foo[2].bar"
  */
// TODO move this into core
// TODO handle backticks encoded
// TODO check someArray[0][1]
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
