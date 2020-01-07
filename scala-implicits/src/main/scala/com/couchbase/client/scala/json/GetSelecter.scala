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

import scala.annotation.tailrec
import scala.language.dynamics
import scala.util.{Failure, Success, Try}

/** Allows dynamically traversing a [[JsonObject]] using Scala's `Dynamic` feature, such as:
  *
  * {{{
  *   val json: JsonObject = // ...
  *   val str: String = json.dyn.someObject.someArray(3).someField.str
  * }}}
  *
  * It can be thought of as building up a cursor, which is only evaluated when pulling out a particular value.
  *
  * It is accessed through [[JsonObject.dyn]] and [[JsonArray.dyn]].
  *
  * This is one of the few APIs that can throw exceptions.  If a functional interface that returns `Try` is preferred,
  * then use [[GetSelecterSafe]].
  *
  * @define SupportedNumTypes the supported number types (Int, Double, Float, Long, Short)
  * @author Graham Pople
  * @since 1.0.0
  */
case class GetSelecter(
    private val in: Either[JsonObject, JsonArray],
    private val path: Seq[PathElement]
) extends Dynamic {
  private val mapped = in.left.map(_.safe).right.map(_.safe)

  def selectDynamic(name: String): GetSelecter =
    GetSelecter(in, path :+ new PathObjectOrField(name))

  def applyDynamic(name: String)(index: Int): GetSelecter =
    GetSelecter(in, path :+ new PathArray(name, index))

  private def pathStr = path.toString()

  /** Returns the value at the current cursor as a `String`.
    *
    * If that value is not itself a String, its `toString` value will be returned.  If the value is `null`, then
    * `null` will be returned.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated
    */
  def str: String = GetSelecter.eval(mapped, path).map(v => ValueConvertor.str(v, pathStr)).get

  /** Returns the value at the current cursor as an `Int`.
    *
    * If that value is actually an Int it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toInt` (which may throw if it cannot be converted).  Else if it is of a
    * different type then InvalidArgumentException will be thrown.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated, or if the value could
    *                                 not be converted
    */
  def num: Int = GetSelecter.eval(mapped, path).map(v => ValueConvertor.num(v, pathStr)).get

  /** Returns the value at the current cursor as a `Double`.
    *
    * If that value is actually a Double it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toDouble` (which may throw if it cannot be converted).  Else if it is of a
    * different type then InvalidArgumentException will be thrown.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated, or if the value could
    *                                 not be converted
    */
  def numDouble: Double =
    GetSelecter.eval(mapped, path).map(v => ValueConvertor.numDouble(v, pathStr)).get

  /** Returns the value at the current cursor as a `Float`.
    *
    * If that value is actually a Float it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toFloat` (which may throw if it cannot be converted).  Else if it is of a
    * different type then InvalidArgumentException will be thrown.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated, or if the value could
    *                                 not be converted
    */
  def numFloat: Float =
    GetSelecter.eval(mapped, path).map(v => ValueConvertor.numFloat(v, pathStr)).get

  /** Returns the value at the current cursor as a `Long`.
    *
    * If that value is actually a Long it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toLong` (which may throw if it cannot be converted).  Else if it is of a
    * different type then InvalidArgumentException will be thrown.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated, or if the value could
    *                                 not be converted
    */
  def numLong: Long =
    GetSelecter.eval(mapped, path).map(v => ValueConvertor.numLong(v, pathStr)).get

  /** Returns the value at the current cursor as a `Boolean`.
    *
    * If that value is actually a Boolean it is returned directly.  Else InvalidArgumentException will be thrown.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated, or if the value could
    *                                 not be converted
    */
  def bool: Boolean = GetSelecter.eval(mapped, path).map(v => ValueConvertor.bool(v, pathStr)).get

  /** Returns the value at the current cursor as a `JsonObject`.
    *
    * If that value is actually a JsonObject it is returned directly.  Else InvalidArgumentException will be thrown.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated, or if the value could
    *                                 not be converted
    */
  def obj: JsonObject = GetSelecter.eval(mapped, path).map(v => ValueConvertor.obj(v, pathStr)).get

  /** Returns the value at the current cursor as a `JsonArray`.
    *
    * If that value is actually a JsonArray it is returned directly.  Else InvalidArgumentException will be thrown.
    *
    * @throws InvalidArgumentException if path established by the cursor could not be evaluated, or if the value could
    *                                 not be converted
    */
  def arr: JsonArray = GetSelecter.eval(mapped, path).map(v => ValueConvertor.arr(v, pathStr)).get
}

private[scala] object GetSelecter {
  private def couldNotFindKey(name: String) =
    new InvalidArgumentException(s"Could not find key $name", null, null)
  private def expectedObjectButFoundArray(name: String) =
    new InvalidArgumentException(
      s"Expected object or field for '$name' but found an array",
      null,
      null
    )
  private def expectedArrayButFoundObject(name: String) =
    new InvalidArgumentException(s"Expected array for '$name' but found an object", null, null)

  /** The user has requested a path e.g. user.addresses[0].name.  Walk through the JSON returning whatever's at that
    * path. */
  @tailrec
  private[scala] def eval(
      cursor: Either[JsonObjectSafe, JsonArraySafe],
      path: Seq[PathElement]
  ): Try[Any] = {
    path match {
      // x is what we're looking for next, cursor is what out cursor's on

      case x :: Nil =>
        x match {
          case v: PathObjectOrField =>
            val name = v.str()
            cursor match {
              case Left(obj) =>
                obj.get(name) match {
                  case Success(o) => Success(o)
                  case _          => Failure(couldNotFindKey(name))
                }

              case Right(arr) =>
                Failure(expectedObjectButFoundArray(name))
            }

          case v: PathArray =>
            val name = v.str()
            val idx  = v.idx()
            cursor match {
              case Left(obj) =>
                obj.arr(name) match {
                  case Success(arr) =>
                    arr.get(idx) match {
                      case Success(v) => Success(v)
                      case Failure(err) =>
                        Failure(
                          new InvalidArgumentException(
                            s"Could not get idx $idx in array '$name'",
                            null,
                            null
                          )
                        )
                    }
                  case Failure(err) =>
                    Failure(
                      new InvalidArgumentException(s"Could not find array '$name'", null, null)
                    )
                }
              case Right(arr) =>
                Failure(expectedObjectButFoundArray(name))
            }
        }

      case x :: xs =>
        x match {
          case v: PathObjectOrField =>
            val name = v.str()
            cursor match {
              case Left(obj) =>
                val inObj = obj.get(name)

                inObj match {
                  case Success(o: JsonObjectSafe) => eval(Left(o), xs)
                  case Success(a: JsonArraySafe)  => eval(Right(a), xs)
                  case Success(o: JsonObject)     => eval(Left(JsonObjectSafe(o)), xs)
                  case Success(a: JsonArray)      => eval(Right(JsonArraySafe(a)), xs)
                  case Success(v: Any) =>
                    Failure(
                      new InvalidArgumentException(
                        s"Needed object or array at $name, but found '${v}'",
                        null,
                        null
                      )
                    )
                  case _ =>
                    Failure(
                      new InvalidArgumentException(
                        s"Could not find anything matching $name",
                        null,
                        null
                      )
                    )
                }

              case Right(arr) =>
                Failure(expectedObjectButFoundArray(name))
            }

          case v: PathArray =>
            val name = v.str()
            val idx  = v.idx()
            cursor match {
              case Left(obj) =>
                obj.arr(name) match {
                  case Success(arr) =>
                    val atIdx: Try[Any] = arr.get(idx)

                    atIdx match {
                      case Success(o: JsonObjectSafe) => eval(Left(o), xs)
                      case Success(a: JsonArraySafe)  => eval(Right(a), xs)
                      case Success(o: JsonObject)     => eval(Left(JsonObjectSafe(o)), xs)
                      case Success(a: JsonArray)      => eval(Right(JsonArraySafe(a)), xs)
                      case Success(v: Any) =>
                        Failure(
                          new InvalidArgumentException(
                            s"Needed object or array at $name[$idx], but found '${v}'",
                            null,
                            null
                          )
                        )
                      case _ =>
                        Failure(
                          new InvalidArgumentException(
                            s"Found array $name but nothing at index $idx",
                            null,
                            null
                          )
                        )
                    }

                  case _ =>
                    Failure(
                      new InvalidArgumentException(s"Could not find array '$name'", null, null)
                    )
                }
              case Right(arr) =>
                eval(Right(arr), xs)
            }
        }
    }
  }
}
