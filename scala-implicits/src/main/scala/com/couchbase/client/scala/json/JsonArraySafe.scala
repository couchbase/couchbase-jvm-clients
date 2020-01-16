package com.couchbase.client.scala.json

import scala.util.{Success, Try}
import java.util

import com.couchbase.client.core.error.DecodingFailureException

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** A 'safe' alternative interface for [[JsonArray]] that does all operations with Try rather than throwing
  * exceptions.
  *
  * Note that, though this is a more functional interface than `JsonArray`, it remains a mutable object.
  *
  * @define Index             a zero-based index into the array
  * @define NotExist          if the provided index is out-of-bounds
  * @author Graham Pople
  * @since 1.0.0
  */
case class JsonArraySafe(private[scala] val a: JsonArray) {
  private val values = a.values

  /** Gets a String value from this array.
    *
    * If that value is not itself a String, its `toString` value will be returned.  If the value is `null`, then
    * `null` will be returned.
    *
    * @param idx  $Index
    *
    * @return `Success(String)` if the index is in bounds, else Failed(IndexOutOfBoundsException)
    */
  def str(idx: Int): Try[String] = {
    Try(a.str(idx))
  }

  /** Gets a Long value from this array.
    *
    * If that value is actually a Long it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toLong` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (DecodingFailureException)` will be returned.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @return `Success(Long)` if the value exists and can successfully be converted to a Long.  `FailedFailed
    *         (NoSuchElementException)` if the value did not exist.  `Failure(DecodingFailureException)` if the value
    *         exists but could not be converted.
    */
  def numLong(idx: Int): Try[Long] = {
    Try(a.numLong(idx))
  }

  /** Gets a Int value from this array.
    *
    * If that value is actually an Int it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toInt` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (InvalidArgumentException)` will be returned.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @return `Success(Int)` if the value exists and can successfully be converted to an Int.  `FailedFailed
    *         (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *         exists but could not be converted.
    */
  def num(idx: Int): Try[Int] = {
    Try(a.num(idx))
  }

  /** Gets a Double value from this array.
    *
    * If that value is actually a Double it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toDouble` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (InvalidArgumentException)` will be returned.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @return `Success(Double)` if the value exists and can successfully be converted to a Double.  `FailedFailed
    *         (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *         exists but could not be converted.
    */
  def numDouble(idx: Int): Try[Double] = {
    Try(a.numDouble(idx))
  }

  /** Gets a Float value from this array.
    *
    * If that value is actually a Float it is returned directly in a `Success`.  Else if it is one of
    * $SupportedNumTypes or String it will be converted with `toFloat` (which will return `Failure` if it cannot be
    * converted) and returned in a `Success`.  Else if it is of a different type then `Failure
    * (InvalidArgumentException)` will be returned.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @return `Success(Float)` if the value exists and can successfully be converted to a Float.  `FailedFailed
    *         (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *         exists but could not be converted.
    */
  def numFloat(idx: Int): Try[Float] = {
    Try(a.numFloat(idx))
  }

  /** Gets a Boolean value from this array.
    *
    * @param name  $Name
    * @return `Success(Boolean)` if the value exists and is a Boolean.  `FailedFailed
    *          (NoSuchElementException)` if the value did not exist.  `Failure(InvalidArgumentException)` if the value
    *          exists but is not a Boolean.
    */
  def bool(idx: Int): Try[Boolean] = {
    Try(a.bool(idx))
  }

  /** Gets a `JsonObjectSafe` value from this array.
    *
    * @param name  $Name
    * @return `Success(JsonObjectSafe)` if the value exists and is a `JsonObject` or `JsonObjectSafe` (note this
    *         will be null if the
    *          value is null).  `FailedFailed(NoSuchElementException)` if the value did not exist.  `Failure
    *          (InvalidArgumentException)` if the value exists but is not a `JsonObject` or `JsonObjectSafe`.
    */
  def obj(idx: Int): Try[JsonObjectSafe] = {
    Try(a.obj(idx)).map(JsonObjectSafe(_))
  }

  /** Gets a `JsonArraySafe` value from this array.
    *
    * @param name  $Name
    * @return `Success(JsonArraySafe)` if the value exists and is a `JsonArray` or `JsonArraySafe` (note this
    *         will be null if the
    *          value is null).  `FailedFailed(NoSuchElementException)` if the value did not exist.  `Failure
    *          (InvalidArgumentException)` if the value exists but is not a `JsonArray` or `JsonArraySafe`.
    */
  def arr(idx: Int): Try[JsonArraySafe] = {
    Try(a.arr(idx)).map(v => JsonArraySafe(v))
  }

  /** Adds a value into this object, which should be of one of the supported types (though, for performance, this is
    * not checked).
    *
    * This mutates this object.  The returned `JsonArraySafe` is a reference to this, not a copy.
    *
    * @param name  $Name
    * @param value $SupportedType
    *
    * @return a reference to this, to allow chaining operations
    */
  def add(item: Any): Try[JsonArraySafe] = {
    a.add(item)
    // Cannot fail, returning a Try simply because it's easier to work with in a for-comprehension
    Success(this)
  }

  /** Gets a value from this array.
    *
    * @param idx  $Index
    *
    * @return `Failure(IndexOutOfBoundsException)` if out-of-bounds, else `Success(Any)`
    */
  def get(idx: Int): Try[Any] = {
    Try(a.get(idx))
  }

  /** Returns true if this contains no items. */
  def isEmpty: Boolean = a.isEmpty

  /** Returns true if this contains items. */
  def nonEmpty: Boolean = a.nonEmpty

  /** Returns an `Iterator` over the items in the array. */
  def iterator: Iterator[Any] = a.iterator

  /** Returns the number of items in this. */
  def size: Int = a.size

  /** Converts this and its contents recursively into Scala collections representing the same JSON.
    *
    * [[JsonObject]] and [[JsonObjectSafe]] will be converted into `collection.Map[String, Any]`, and [[JsonArray]] and
    * [[JsonArraySafe]] will be converted into `Seq[Any]`.
    */
  def toSeq: Seq[Any] = a.toSeq

  /** Returns a [[GetSelecterSafe]] providing `Dynamic` access to this. */
  def dyn: GetSelecterSafe = GetSelecterSafe(Right(this), Seq())

  /** Recursively converts this and its contents into String representing the same JSON.
    *
    * Will always return `Success` as long as only supported types have been put into this.  If invalid values have
    * been put into this then `Failure` will be returned.
    */
  def asString: Try[String] = {
    Try(a.toString)
  }

  /** Recursively converts this and its contents into String representing the same JSON.
    *
    * Will not throw as long as only supported types have been put into this.
    */
  override def toString: String = super.toString
}

/** Methods to construct a `JsonArraySafe`. */
object JsonArraySafe {

  /** Constructs an empty `JsonArraySafe`. */
  def create: JsonArraySafe = JsonArraySafe(JsonArray.create)

  /** Constructs a `JsonArraySafe` from a String representing valid JSON.
    *
    * @return `Failure(IllegalArgumentException)` if the String contains invalid JSON, otherwise `Success
    *         (JsonArraySafe)`
    */
  def fromJson(json: String): Try[JsonArraySafe] = {
    Try(JsonArraySafe(JsonArray.create))
  }

  /** Constructs a `JsonArraySafe` from the supplied values.
    *
    * Performance note: benchmarking indicates it's much faster to do JsonArraySafe.create.put(x).put(y)
    * than JsonArraySafe(x, y)
    */
  def apply(in: Any*): JsonArraySafe = {
    JsonArraySafe(JsonArray(in))
  }
}
