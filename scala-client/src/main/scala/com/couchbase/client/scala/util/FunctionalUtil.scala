package com.couchbase.client.scala.util

import scala.util.{Success, Try}

// Who needs cats?
object FunctionalUtil {
  def traverse[T](in: Seq[Try[T]]): Try[Seq[T]] = {
    in match {
      case x :: Nil => x.map(Seq(_))
      case x :: xs => x.flatMap(v => {
        val rest: Try[Seq[T]] = traverse(xs)
        val y: Seq[T] = Seq(v)
        rest.map(z => y ++ z)
      })
    }
  }
}
