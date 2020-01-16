package com.couchbase.client.scala.json

import java.util

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.scala.transformers.JacksonTransformers

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** A companion to [[JsonObject]], representing a JSON array.
  *
  * Construct with the functions in the companion object.
  *
  * Like `JsonObject`, this has a more functional alternative interface that returns `Try`s rather than throw
  * exceptions, which you may freely use instead.  See [[JsonArraySafe]].
  *
  * @define SupportedType     only certain types are allowed to be put into this.  They are: [[JsonObject]] and
  *                           [[JsonObjectSafe]], [[JsonArray]] and [[JsonArraySafe]], Int, Double, Short, Long, Float,
  *                           Boolean and null.
  * @define Index             a zero-based index into the array
  * @define NotExist          if the provided index is out-of-bounds
  * @author Graham Pople
  * @since 1.0.0
  */
case class JsonArray(private[scala] val values: java.util.ArrayList[Any]) {

  /** Adds a value into this object, which should be of one of the supported types (though, for performance, this is
    * not checked).
    *
    * This mutates this object.  The returned `JsonArray` is a reference to this, not a copy.
    *
    * @param name  $Name
    * @param value $SupportedType
    *
    * @return a reference to this, to allow chaining operations
    */
  def add(item: Any): JsonArray = {
    values.add(item)
    this
  }

  /** Gets a value from this array.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    */
  def get(idx: Int): Any = {
    values.get(idx)
  }

  // Note these methods are some of the few in the Scala SDK that can throw an exception.
  // They are optimised for performance: use the *t methods instead for functional safety.

  /** Gets a String value from this array.
    *
    * If that value is not itself a String, its `toString` value will be returned.  If the value is `null`, then
    * `null` will be returned.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    */
  def str(idx: Int): String = {
    ValueConvertor.str(values.get(idx), "array index")
  }

  /** Gets a Long value from this array.
    *
    * If that value is actually a Long it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toLong` (which may throw if it cannot be converted).  Else if it is of a
    * different type then DecodingFailureException will be thrown.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @throws InvalidArgumentException   if the value was not one of $SupportedNumTypes or String
    */
  def numLong(idx: Int): Long = {
    ValueConvertor.numLong(values.get(idx), "array index")
  }

  /** Gets an Int value from this array.
    *
    * If that value is actually an Int it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toInt` (which may throw if it cannot be converted).  Else if it is of a
    * different type then DecodingFailureException will be thrown.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @throws InvalidArgumentException   if the value was not one of $SupportedNumTypes or String
    */
  def num(idx: Int): Int = {
    ValueConvertor.num(values.get(idx), "array index")
  }

  /** Gets a Double value from this array.
    *
    * If that value is actually a Double it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toDouble` (which may throw if it cannot be converted).  Else if it is of a
    * different type then DecodingFailureException will be thrown.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @throws DecodingFailureException   if the value was not one of $SupportedNumTypes or String
    */
  def numDouble(idx: Int): Double = {
    ValueConvertor.numDouble(values.get(idx), "array index")
  }

  /** Gets a Float value from this array.
    *
    * If that value is actually a Float it is returned directly.  Else if it is one of $SupportedNumTypes or
    * String it will be converted with `toFloat` (which may throw if it cannot be converted).  Else if it is of a
    * different type then DecodingFailureException will be thrown.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @throws InvalidArgumentException   if the value was not one of $SupportedNumTypes or String
    */
  def numFloat(idx: Int): Float = {
    ValueConvertor.numFloat(values.get(idx), "array index")
  }

  /** Gets a Boolean value from this array.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @throws InvalidArgumentException  if the value was not of type `Boolean`
    */
  def bool(idx: Int): Boolean = {
    ValueConvertor.bool(values.get(idx), "array index")
  }

  /** Gets a `JsonObject` value from this array.
    *
    * If that value is `null`, then `null` will be returned.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @throws InvalidArgumentException  if the value was not of type `JsonObject`
    */
  def obj(idx: Int): JsonObject = {
    ValueConvertor.obj(values.get(idx), "array index")
  }

  /** Gets a `JsonArray` value from this array.
    *
    * If that value is `null`, then `null` will be returned.
    *
    * @param idx  $Index
    *
    * @throws IndexOutOfBoundsException $NotExist
    * @throws InvalidArgumentException  if the value was not of type `JsonArray`
    */
  def arr(idx: Int): JsonArray = {
    ValueConvertor.arr(values.get(idx), "array index")
  }

  /** Returns true if this contains no items. */
  def isEmpty: Boolean = values.isEmpty

  /** Returns true if this contains items. */
  def nonEmpty: Boolean = !values.isEmpty

  /** Returns an `Iterator` over the items in the array. */
  def iterator: Iterator[Any] = values.asScala.iterator

  /** Returns the number of items in this. */
  def size: Int = values.size

  /** Converts this and its contents recursively into Scala collections representing the same JSON.
    *
    * [[JsonObject]] and [[JsonObjectSafe]] will be converted into `collection.Map[String, Any]`, and [[JsonArray]] and
    * [[JsonArraySafe]] will be converted into `Seq[Any]`.
    */
  def toSeq: Seq[Any] = {
    val copy = ArrayBuffer.empty[Any]
    val it   = iterator
    while (it.hasNext) {
      val value = it.next()
      copy += (value match {
        case v: JsonObject     => v.toMap
        case v: JsonObjectSafe => v.toMap
        case v: JsonArray      => v.toSeq
        case v: JsonArraySafe  => v.toSeq
        case _                 => value
      })
    }
    copy.toSeq
  }

  /** Returns a [[GetSelecter]] providing `Dynamic` access to this. */
  def dyn: GetSelecter = GetSelecter(Right(this), Seq())

  /** Recursively converts this and its contents into String representing the same JSON.
    *
    * Will not throw as long as only supported types have been put into this.
    */
  override def toString: String = JacksonTransformers.MAPPER.writeValueAsString(this)

  /** This is an 'unsafe', non-functional interface that can throw.  It's provided for convenience.
    *
    * If you would rather use a functional interface that returns `Try` for all operations and does not throw, then
    * use this method to get a [[JsonArraySafe]] interface.
    */
  def safe = JsonArraySafe(this)
}

/** Methods to construct a `JsonArray`. */
object JsonArray {

  /** Constructs an empty `JsonArray`. */
  def create: JsonArray = new JsonArray(new util.ArrayList[Any])

  /** Constructs a `JsonArray` from a String representing valid JSON.
    *
    * @throws IllegalArgumentException if the String contains invalid JSON
    */
  def fromJson(json: String): Try[JsonArray] = {
    try {
      Success(JacksonTransformers.stringToJsonArray(json))
    } catch {
      case NonFatal(err) =>
        Failure(new InvalidArgumentException("Failed to decode json", err, null))
    }
  }

  /** Constructs a `JsonArray` from the supplied values.
    *
    * Performance note: benchmarking indicates it's much faster to do `JsonArray.create.put(x).put(y)`
    * than `JsonArray(x, y)`
    */
  def apply(in: Any*): JsonArray = {
    val lst = new util.ArrayList[Any](in.size)
    // Iterators benchmarked as much faster than foreach
    val it = in.iterator
    while (it.hasNext) {
      lst.add(it.next())
    }
    new JsonArray(lst)
  }
}
