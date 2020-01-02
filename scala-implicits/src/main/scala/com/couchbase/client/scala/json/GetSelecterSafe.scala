/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.scala.json

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.projections.{PathArray, PathElement, PathObjectOrField}

import scala.language.dynamics
import scala.util.Try

/** A 'safe' version of [[GetSelecter]] whose methods return `Try` rather than throw exceptions.
  *
  * In all other respects it is identical to `GetSelecter`.  Please see that for documentation.
  */
case class GetSelecterSafe(
    private val in: Either[JsonObjectSafe, JsonArraySafe],
    private val path: Seq[PathElement]
) extends Dynamic {
  def selectDynamic(name: String): GetSelecterSafe =
    GetSelecterSafe(in, path :+ new PathObjectOrField(name))

  def applyDynamic(name: String)(index: Int): GetSelecterSafe =
    GetSelecterSafe(in, path :+ new PathArray(name, index))

  private def pathStr = path.toString()

  def str: Try[String] =
    GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.str(v, pathStr)))

  def num: Try[Int] = GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.num(v, pathStr)))

  def numDouble: Try[Double] =
    GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.numDouble(v, pathStr)))

  def numFloat: Try[Float] =
    GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.numFloat(v, pathStr)))

  def bool: Try[Boolean] =
    GetSelecter.eval(in, path).flatMap(v => Try(ValueConvertor.bool(v, pathStr)))

  def obj: Try[JsonObjectSafe] =
    GetSelecter.eval(in, path).flatMap(v => Try(JsonObjectSafe(ValueConvertor.obj(v, pathStr))))

  def arr: Try[JsonArraySafe] =
    GetSelecter.eval(in, path).flatMap(v => Try(JsonArraySafe(ValueConvertor.arr(v, pathStr))))
}
