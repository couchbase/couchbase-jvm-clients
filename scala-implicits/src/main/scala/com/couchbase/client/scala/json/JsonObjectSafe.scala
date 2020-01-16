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

package com.couchbase.client.scala.json

import scala.collection.{Map => CMap, Set => CSet}
import scala.language.dynamics
import scala.util.{Success, Try}

/** A 'safe' alternative interface for [[JsonObject]] that does all operations with Try rather than throwing
  * exceptions.
  *
  * Note that, though this is a more functional interface than `JsonObject`, it remains a mutable object.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class JsonObjectSafe(private[scala] val o: JsonObject) {
  private val content = o.content

  /** Returns a [[GetSelecterSafe]] providing `Dynamic` access to this object's fields. */
  def dyn: GetSelecterSafe = GetSelecterSafe(Left(this), Seq())

  /** Puts a value into this object.
    *
    * This mutates this object.  The returned `JsonObjectSafe` is a reference to this, not a copy.
    *
    * @param name  $Name
    * @param value $SupportedType
    * @return a reference to this, to allow chaining operations
    */
  def put(name: String, value: Any): Try[JsonObjectSafe] = {
    // Cannot fail, returning a Try simply because it's easier to work with in a for-comprehension
    o.put(name, value)
    Success(this)
  }

  /** Gets a value from this object.
    *
    * @param name  $Name
    * @return `Success(Any)` if the value exists, else Failed(NoSuchElementException)
    */
  def get(name: String): Try[Any] = {
    Try(o.get(name))
  }

  /** Gets a String value from this object.
    *
    * If that value is not itself a String, it's `toString` value will be returned.  If the value is `null`, then
    * `null` will be returned.
    *
    * @param name  $Name
    * @return `Success(String)` if the value exists, else Failed(NoSuchElementException)
    */
  def str(name: String): Try[String] = {
    Try(o.str(name))
  }

  /** Gets an Int value from this object.
    *
    * If that value is actually an Int it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toInt` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (InvalidArgumentException)` will be returned.
    *
    * @param name  $Name
    * @return `Success(Int)` if the value exists and can successfully be converted to an Int.  `FailedFailed
    *          (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *          exists but could not be converted.
    */
  def num(name: String): Try[Int] = {
    Try(o.num(name))
  }

  /** Gets a Boolean value from this object.
    *
    * @param name  $Name
    * @return `Success(Boolean)` if the value exists and is a Boolean.  `FailedFailed
    *          (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *          exists but is not a Boolean.
    */
  def bool(name: String): Try[Boolean] = {
    Try(o.bool(name))
  }

  /** Gets a Long value from this object.
    *
    * If that value is actually a Long it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toInt` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (InvalidArgumentException)` will be returned.
    *
    * @param name  $Name
    * @return `Success(Long)` if the value exists and can successfully be converted to a Long.  `FailedFailed
    *          (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *          exists but could not be converted.
    */
  def numLong(name: String): Try[Long] = {
    Try(o.numLong(name))
  }

  /** Gets a Double value from this object.
    *
    * If that value is actually a Double it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toDouble` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (InvalidArgumentException)` will be returned.
    *
    * @param name  $Name
    * @return `Success(Double)` if the value exists and can successfully be converted to a Double.  `FailedFailed
    *          (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *          exists but could not be converted.
    */
  def numDouble(name: String): Try[Double] = {
    Try(o.numDouble(name))
  }

  /** Gets a Float value from this object.
    *
    * If that value is actually a Float it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toFloat` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (InvalidArgumentException)` will be returned.
    *
    * @param name  $Name
    * @return `Success(Float)` if the value exists and can successfully be converted to a Float.  `FailedFailed
    *          (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *          exists but could not be converted.
    */
  def numFloat(name: String): Try[Float] = {
    Try(o.numFloat(name))
  }

  /** Gets a `JsonObjectSafe` value from this object.
    *
    * @param name  $Name
    * @return `Success(JsonObjectSafe)` if the value exists and is a `JsonObject` or `JsonObjectSafe` (note this
    *         will be null if the
    *          value is null).  `FailedFailed(NoSuchElementException)` if the value did not exist.  `Failure
    *          (InvalidArgumentException)` if the value exists but is not a `JsonObject` or `JsonObjectSafe`.
    */
  def obj(name: String): Try[JsonObjectSafe] = {
    Try(o.obj(name)).map(JsonObjectSafe(_))
  }

  /** Gets a `JsonArraySafe` value from this object.
    *
    * @param name  $Name
    * @return `Success(JsonArraySafe)` if the value exists and is a `JsonArray` or `JsonArraySafe` (note this
    *         will be null if the
    *          value is null).  `FailedFailed(NoSuchElementException)` if the value did not exist.  `Failure
    *          (InvalidArgumentException)` if the value exists but is not a `JsonArray` or `JsonArraySafe`.
    */
  def arr(name: String): Try[JsonArraySafe] = {
    Try(o.arr(name)).map(v => JsonArraySafe(v))
  }

  /** Removes a key from this object.
    *
    * This mutates this object.  The returned `JsonObjectSafe` is a reference to this, not a copy.
    *
    * If the object does not contain the key then this is a no-op.
    *
    * @param name  $Name
    * @return a reference to this, to allow chaining operations
    */
  def remove(name: String): JsonObjectSafe = {
    content.remove(name)
    this
  }

  /** Returns true if this contains a key.
    *
    * @param name  $Name
    */
  def containsKey(name: String): Boolean = o.containsKey(name)

  /** Returns a `collection.Set` of all keys in this. */
  def names: CSet[String] = o.names

  /** Returns true if this contains no keys. */
  def isEmpty: Boolean = o.isEmpty

  /** Returns true if this contains keys. */
  def nonEmpty: Boolean = o.nonEmpty

  /** Converts this and its contents recursively into Scala collections representing the same JSON.
    *
    * [[JsonObject]] and [[JsonObjectSafe]] will be converted into `collection.Map[String, Any]`, and [[JsonArray]] and
    * [[JsonArraySafe]] will be converted into `Seq[Any]`.
    */
  def toMap: CMap[String, Any] = o.toMap

  /** Returns the number of keys in this. */
  def size: Int = o.size

  /** Recursively converts this and its contents into String representing the same JSON, e.g. """{"foo":"bar"}""".
    *
    * Will always return `Success` as long as only supported types have been put into this.  If invalid values have
    * been put into this then `Failure` will be returned.
    */
  def asString: Try[String] = {
    Try(o.toString)
  }

  override def toString: String = o.toString
}

/** Methods to construct a `JsonObjectSafe`. */
object JsonObjectSafe {

  /** Constructs a `JsonObjectSafe` from a String representing valid JSON ("""{"foo":"bar"}""").
    *
    * @return `Failure(IllegalArgumentException)` if the String contains invalid JSON, else `Success(JsonObjectSafe)`
    */
  def fromJson(json: String): Try[JsonObjectSafe] = {
    Try(JsonObject.fromJson(json)).map(JsonObjectSafe(_))
  }

  /** Constructs an empty `JsonObject`. */
  def create: JsonObjectSafe = JsonObjectSafe(JsonObject.create)

  /** Constructs a `JsonObjectSafe` from the supplied map.
    *
    * Note, benchmarking indicates it's roughly twice as fast to use `create` and then `put` all fields, so that
    * should be preferred.
    */
  def apply(values: (String, Any)*): JsonObjectSafe = {
    JsonObjectSafe(JsonObject(values: _*))
  }

  /** Constructs a `JsonObjectSafe` from the supplied map.
    *
    * Note, benchmarking indicates it's roughly twice as fast to use `create` and then `put` all fields, so that
    * should be preferred.
    */
  def apply(values: Map[String, Any]): JsonObjectSafe = {
    JsonObjectSafe(JsonObject(values))
  }
}
