package com.couchbase.client.scala.json

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

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
  def str(idx: Int): String = values.get(idx).asInstanceOf[String]
  def long(idx: Int): Long = values.get(idx).asInstanceOf[Long]
  def int(idx: Int): Int = values.get(idx).asInstanceOf[Int]
  def double(idx: Int): Double = values.get(idx).asInstanceOf[Double]
  def float(idx: Int): Float = values.get(idx).asInstanceOf[Float]
  def obj(idx: Int): JsonObject = values.get(idx).asInstanceOf[JsonObject]
  def arr(idx: Int): JsonArray = values.get(idx).asInstanceOf[JsonArray]

  def strOpt(idx: Int): Option[String] = Option(values.get(idx)).map(_.asInstanceOf[String])
  def longOpt(idx: Int): Option[Long] = Option(values.get(idx)).map(_.asInstanceOf[Long])
  def intOpt(idx: Int): Option[Int] = Option(values.get(idx)).map(_.asInstanceOf[Int])
  def doubleOpt(idx: Int): Option[Double] = Option(values.get(idx)).map(_.asInstanceOf[Double])
  def floatOpt(idx: Int): Option[Float] = Option(values.get(idx)).map(_.asInstanceOf[Float])
  def objOpt(idx: Int): Option[JsonObject] = Option(values.get(idx)).map(_.asInstanceOf[JsonObject])
  def arrOpt(idx: Int): Option[JsonArray] = Option(values.get(idx)).map(_.asInstanceOf[JsonArray])

  def isEmpty: Boolean = values.isEmpty
  def iterator: Iterator[Any] = values.asScala.iterator
  def size: Int = values.size

  def toSeq: Seq[Any] = values.asScala
  def toJavaList: java.util.List[Any] = values
}



object JsonArray {
  def create: JsonArray = new JsonArray(new util.ArrayList[Any])

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
