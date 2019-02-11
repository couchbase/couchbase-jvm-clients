package com.couchbase.client.scala.json

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// Choosing vector as we'll mostly be adding to the end of it
case class JsonArray(val values: ArrayBuffer[Any]) {
  def add(item: Any): JsonArray = {
    values += item
    this
  }
  def addNull: JsonArray = {
    values += null
    this
  }
  def get(idx: Int): Any = {
    values(idx)
  }
  def getOpt(idx: Int): Option[Any] = {
    Option(values(idx))
  }
  def getString(idx: Int): String = values(idx).asInstanceOf[String]
  // TODO does getLong & getInt make sense?  Always using wrong one...
  def getLong(idx: Int): Long = values(idx).asInstanceOf[Long]
  def getInt(idx: Int): Int = values(idx).asInstanceOf[Int]
  def getDouble(idx: Int): Double = values(idx).asInstanceOf[Double]
  def getFloat(idx: Int): Float = values(idx).asInstanceOf[Float]
  def getObject(idx: Int): JsonObject = values(idx).asInstanceOf[JsonObject]
  def isEmpty: Boolean = values.isEmpty
  def iterator: Iterator[Any] = values.iterator
  def size: Int = values.size

  def toSeq: Seq[Any] = values
  def toJavaList: java.util.List[Any] = values.asJava
}



object JsonArray {
  def create: JsonArray = new JsonArray(ArrayBuffer())

  def apply(in: Any*): JsonArray = new JsonArray(ArrayBuffer(in))
}

//  private val mapper = new ObjectMapper()
//
//  def fromJson(json: String): JsonArray = {
//    mapper.readValue(json, classOf[JsonArray])
//  }
//
//  def toJsonType(in: Any): Try[JsonType] = {
//    in match {
//      case x: String => Success(JsonString(x))
//      case x: Int => Success(JsonNumber(x))
//      case x: Long => Success(JsonNumber(x))
//      case x: Double => Success(JsonNumber(x))
//      case x: Boolean => Success(JsonBoolean(x))
//        // TODO MVP Seq
////      case x: Seq[_] =>
////        val arr: Try[Seq[JsonType]] = x.map(toJsonType(_))
////          arr.map(v => JsonArray(v)))
//      case _ => Failure(CouldNotEncodeToJsonType(in))
//    }
//  }
//
//  // TODO checkItems
//  def from(items: Any*): Try[JsonArray] = {
//    // TODO MVP
//    ???
////    val arr = items.map(toJsonType(_))
////    new JsonArray(.toVector
//  }
//  // TODO more advanced from that converts into JsonObjects etc
//
//  private val EMPTY = JsonArray.create
//
//  def empty: JsonArray = EMPTY
//
//  def create: JsonArray = new JsonArray(Vector.empty)
////}