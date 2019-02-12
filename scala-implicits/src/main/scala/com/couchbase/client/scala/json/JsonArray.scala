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
  def getString(idx: Int): String = values.get(idx).asInstanceOf[String]
  def getLong(idx: Int): Long = values.get(idx).asInstanceOf[Long]
  def getInt(idx: Int): Int = values.get(idx).asInstanceOf[Int]
  def getDouble(idx: Int): Double = values.get(idx).asInstanceOf[Double]
  def getFloat(idx: Int): Float = values.get(idx).asInstanceOf[Float]
  def getObject(idx: Int): JsonObject = values.get(idx).asInstanceOf[JsonObject]
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
