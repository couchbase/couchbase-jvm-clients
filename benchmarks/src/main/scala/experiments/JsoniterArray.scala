package experiments

import com.couchbase.client.scala.json.JsonObject

import scala.collection.mutable.ArrayBuffer

// Choosing vector as we'll mostly be adding to the end of it
case class JsoniterArray(val values: ArrayBuffer[Any]) {
  def add(item: String): JsoniterArray = {
    values += item
    this
  }
  def add(item: Int): JsoniterArray = {
    values += item
    this
  }
  def addNull: JsoniterArray = {
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
  def getLong(idx: Int): Long = values(idx).asInstanceOf[Long]
  def getInt(idx: Int): Int = values(idx).asInstanceOf[Int]
  def getDouble(idx: Int): Double = values(idx).asInstanceOf[Double]
  def getFloat(idx: Int): Float = values(idx).asInstanceOf[Float]
  def getObject(idx: Int): JsonObject = values(idx).asInstanceOf[JsonObject]
  def isEmpty: Boolean = values.isEmpty
  def iterator: Iterator[Any] = values.iterator
  def size: Int = values.size
  //  def toSeq: Seq[JsonType] = values


//  def add(item: String): JsoniterArray = copy(values :+ JsonString(item))
//  def add(item: Int): JsoniterArray = copy(values :+ JsonNumber(item))
//  def addNull: JsoniterArray = copy(values :+ null)
//  def get(idx: Int): Option[JsonType] = Some(values(idx))
//  def str(idx: Int): String = values(idx).asInstanceOf[String]
//  def long(idx: Int): Long = values(idx).asInstanceOf[Long]
//  def int(idx: Int): Int = values(idx).asInstanceOf[Int]
//  def double(idx: Int): Double = values(idx).asInstanceOf[Double]
//  def float(idx: Int): Float = values(idx).asInstanceOf[Float]
//  def obj(idx: Int): JsonObject = values(idx).asInstanceOf[JsonObject]
//  def isEmpty: Boolean = values.isEmpty
//  def iterator: Iterator[Any] = values.iterator
//  //  def toSeq: Seq[JsonType] = values
}
