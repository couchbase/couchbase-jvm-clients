package com.couchbase.client.scala.kv

import com.couchbase.client.scala.json.{PathArray, PathElements, PathObjectOrField}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[scala] object JsonPathParser {
  def parse(path: String): Try[Seq[PathElements]] = {
    def debugPos(idx: Int) = s"position $idx"

    try {
      var elemIdx = 0
      var idx = 0
      val len = path.length
      val ret = ArrayBuffer.empty[PathElements]

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
