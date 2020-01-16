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
package com.couchbase.client.scala.util

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/** Useful functional bits.
  *
  * Who needs Cats?
  *
  * @since 1.0.0
  */
private[scala] object FunctionalUtil {
  @Deprecated // Likely to be slow, not tailrec
  def traverse[T](in: List[Try[T]]): Try[List[T]] = {
    in match {
      case x :: Nil => x.map(List(_))
      case x :: xs =>
        x.flatMap(v => {
          val rest: Try[List[T]] = traverse(xs)
          val y: List[T]         = List(v)
          rest.map(z => y ++ z)
        })
      case Nil => Success(Nil)
    }
  }

  @Deprecated // Likely to be slow, not tailrec
  def traverse[T](in: Seq[Try[T]]): Try[Seq[T]] = {
    in match {
      case x +: Nil => x.map(Seq(_))
      case x +: xs =>
        x.flatMap(v => {
          val rest: Try[Seq[T]] = traverse(xs)
          val y: Seq[T]         = Seq(v)
          rest.map(z => y ++ z)
        })
      case Nil => Success(Nil)
    }
  }

}
