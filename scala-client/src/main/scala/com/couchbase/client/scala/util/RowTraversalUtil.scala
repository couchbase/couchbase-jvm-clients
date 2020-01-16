package com.couchbase.client.scala.util

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object RowTraversalUtil {
  def traverse[T](it: Iterator[Try[T]]): Try[collection.Seq[T]] = {
    // Not a functional implementation, going for performance
    var outFail: Try[_] = null
    val out             = ArrayBuffer.empty[T]
    while (it.hasNext && outFail == null) {
      val next = it.next()
      next match {
        case Success(v) => out += v
        case Failure(v) => outFail = next
      }
    }
    if (outFail != null) outFail.asInstanceOf[Try[Seq[T]]]
    else Success(out)
  }

}
