package com.couchbase.client.scala.json

import java.util

import com.couchbase.client.core.error.DecodingFailedException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// Using Java ArrayList as benchmarking found it 4x faster than ArrayBuffer
case class JsonArray(private[scala] val values: java.util.ArrayList[Any]) {
  def add(item: Any): JsonArray = {
    values.add(item)
    this
  }
  def addNull: JsonArray = {
    values.add(null)
    this
  }
  def get(idx: Int): Any = {
    values.get(idx)
  }

  // Note these methods are some of the few in the Scala SDK that can throw an exception.
  // They are optimised for performance: use the *t methods instead for functional safety.

  def str(idx: Int): String = {
    ValueConvertor.str(values.get(idx), "array index")
  }

  def numLong(idx: Int): Long = {
    ValueConvertor.numLong(values.get(idx), "array index")
  }

  def num(idx: Int): Int = {
    ValueConvertor.num(values.get(idx), "array index")
  }

  def numDouble(idx: Int): Double = {
    ValueConvertor.numDouble(values.get(idx), "array index")
  }

  def numFloat(idx: Int): Float = {
    ValueConvertor.numFloat(values.get(idx), "array index")
  }

  def obj(idx: Int): JsonObject = {
    ValueConvertor.obj(values.get(idx), "array index")
  }

  def arr(idx: Int): JsonArray = {
    ValueConvertor.arr(values.get(idx), "array index")
  }

  def isEmpty: Boolean = values.isEmpty
  def nonEmpty: Boolean = !values.isEmpty
  def iterator: Iterator[Any] = values.asScala.iterator
  def size: Int = values.size

  def toSeq: Seq[Any] = values.asScala
  def toJavaList: java.util.List[Any] = values

  def dyn: GetSelecter = GetSelecter(Right(this), Seq())

  def asString: Try[String] = {
    Try(JacksonTransformers.MAPPER.writeValueAsString(this))
  }

  override def toString: String = asString.get

//  override def toString: String = {
//    val sb = new StringBuilder
//    sb += '['
//    val it = values.iterator()
//    while (it.hasNext) {
//      val next = it.next()
//      next match {
//        case v: String =>
//          sb += '"'
//          sb.append(v)
//          sb += '"'
//        case v =>
//          sb.append(v.toString)
//      }
//      if (it.hasNext) sb.append(',')
//    }
//    sb += ']'
//    sb.toString()
//  }

  def safe = JsonArraySafe(this)
}





object JsonArray {
  def create: JsonArray = new JsonArray(new util.ArrayList[Any])

  def fromJson(json: String): Try[JsonArray] = {
    try {
      Success(JacksonTransformers.stringToJsonArray(json))
    }
    catch {
      case NonFatal(err) => Failure(new DecodingFailedException(err))
    }
  }

  /**
    * Performance note: benchmarking indicates it's much faster to do JsonArray.create.put(x).put(y)
    * than JsonArray(x, y)
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



  // TODO checkType
}
