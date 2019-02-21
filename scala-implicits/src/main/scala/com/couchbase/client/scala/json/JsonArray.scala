package com.couchbase.client.scala.json

import java.util

import com.couchbase.client.core.error.DecodingFailedException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// Using Java ArrayList as benchmarking found it 4x faster than ArrayBuffer
case class JsonArray(private val values: java.util.ArrayList[Any]) {
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
  def getOpt(idx: Int): Option[Any] = {
    Option(values.get(idx))
  }

  // Note these methods are some of the few in the Scala SDK that can throw an exception.
  // They are optimised for performance: use the *t methods instead for functional safety.

  // TODO test using these from Java
  def str(idx: Int): String = {
    val out = values.get(idx)
    if (out == null) throw new IllegalArgumentException(s"Array index $idx contans null")
    else out.asInstanceOf[String]
  }
  def long(idx: Int): Long = {
    val out = values.get(idx)
    if (out == null) throw new IllegalArgumentException(s"Array index $idx contans null")
    else out.asInstanceOf[Long]
  }
  def int(idx: Int): Int = {
    val out = values.get(idx)
    if (out == null) throw new IllegalArgumentException(s"Array index $idx contans null")
    else out.asInstanceOf[Int]
  }
  def double(idx: Int): Double = {
    val out = values.get(idx)
    if (out == null) throw new IllegalArgumentException(s"Array index $idx contans null")
    else out.asInstanceOf[Double]
  }
  def float(idx: Int): Float = {
    val out = values.get(idx)
    if (out == null) throw new IllegalArgumentException(s"Array index $idx contans null")
    else out.asInstanceOf[Float]
  }
  def obj(idx: Int): JsonObject = {
    val out = values.get(idx)
    if (out == null) throw new IllegalArgumentException(s"Array index $idx contans null")
    else out.asInstanceOf[JsonObject]
  }
  def arr(idx: Int): JsonArray = {
    val out = values.get(idx)
    if (out == null) throw new IllegalArgumentException(s"Array index $idx contans null")
    else out.asInstanceOf[JsonArray]
  }

  def strt(idx: Int): Try[String] = {
    if (idx < 0 || idx >= values.size()) Failure(new IllegalArgumentException(s"Array index $idx out of bounds"))
    val out = values.get(idx)
    if (out == null) Failure(new IllegalArgumentException(s"Array index $idx contains a null field does not exist"))
    else Try(out.asInstanceOf[String])
  }
  def longt(idx: Int): Try[String] = {
    if (idx < 0 || idx >= values.size()) Failure(new IllegalArgumentException(s"Array index $idx out of bounds"))
    val out = values.get(idx)
    if (out == null) Failure(new IllegalArgumentException(s"Array index $idx contains a null field does not exist"))
    else Try(out.asInstanceOf[String])
  }
  def intt(idx: Int): Try[String] = {
    if (idx < 0 || idx >= values.size()) Failure(new IllegalArgumentException(s"Array index $idx out of bounds"))
    val out = values.get(idx)
    if (out == null) Failure(new IllegalArgumentException(s"Array index $idx contains a null field does not exist"))
    else Try(out.asInstanceOf[String])
  }
  def doublet(idx: Int): Try[String] = {
    if (idx < 0 || idx >= values.size()) Failure(new IllegalArgumentException(s"Array index $idx out of bounds"))
    val out = values.get(idx)
    if (out == null) Failure(new IllegalArgumentException(s"Array index $idx contains a null field does not exist"))
    else Try(out.asInstanceOf[String])
  }
  def floatt(idx: Int): Try[String] = {
    if (idx < 0 || idx >= values.size()) Failure(new IllegalArgumentException(s"Array index $idx out of bounds"))
    val out = values.get(idx)
    if (out == null) Failure(new IllegalArgumentException(s"Array index $idx contains a null field does not exist"))
    else Try(out.asInstanceOf[String])
  }
  def objt(idx: Int): Try[String] = {
    if (idx < 0 || idx >= values.size()) Failure(new IllegalArgumentException(s"Array index $idx out of bounds"))
    val out = values.get(idx)
    if (out == null) Failure(new IllegalArgumentException(s"Array index $idx contains a null field does not exist"))
    else Try(out.asInstanceOf[String])
  }
  def arrt(idx: Int): Try[String] = {
    if (idx < 0 || idx >= values.size()) Failure(new IllegalArgumentException(s"Array index $idx out of bounds"))
    val out = values.get(idx)
    if (out == null) Failure(new IllegalArgumentException(s"Array index $idx contains a null field does not exist"))
    else Try(out.asInstanceOf[String])
  }

  def isEmpty: Boolean = values.isEmpty
  def iterator: Iterator[Any] = values.asScala.iterator
  def size: Int = values.size

  def toSeq: Seq[Any] = values.asScala
  def toJavaList: java.util.List[Any] = values

  def dyn: GetSelecter = GetSelecter(Right(this), Seq())

  // TODO make output valid JSON
  override def toString: String = {
    val sb = new StringBuilder
    sb += '['
    val it = values.iterator()
    while (it.hasNext) {
      val next = it.next()
      sb.append(next.toString)
      if (it.hasNext) sb.append(',')
    }
    sb += ']'
    sb.toString()
  }

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
