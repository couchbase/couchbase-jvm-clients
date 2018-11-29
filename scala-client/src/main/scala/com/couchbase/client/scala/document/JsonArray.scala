package com.couchbase.client.scala.document

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.parallel.immutable

// Choosing vector as we'll mostly be adding to the end of it
case class JsonArray(private val values: Vector[Any]) {
  def add(item: Any): JsonArray = copy(values :+ item)
  def addNull: JsonArray = copy(values :+ null)
  def get(idx: Int): Any = values(idx)
  def getString(idx: Int): String = values(idx).asInstanceOf[String]
  // TODO does getLong & getInt make sense?  Always using wrong one...
  def getLong(idx: Int): Long = values(idx).asInstanceOf[Long]
  def getInt(idx: Int): Int = values(idx).asInstanceOf[Int]
  def getDouble(idx: Int): Double = values(idx).asInstanceOf[Double]
  def getFloat(idx: Int): Float = values(idx).asInstanceOf[Float]
  def getObject(idx: Int): JsonObject = values(idx).asInstanceOf[JsonObject]
  def isEmpty: Boolean = values.isEmpty
  def iterator: Iterator[Any] = values.iterator
  def toSeq: Seq[Any] = values
  // TODO add immutable variant
}

object JsonArray {
  private val mapper = new ObjectMapper()

  def fromJson(json: String): JsonArray = {
    mapper.readValue(json, classOf[JsonArray])
  }

  // TODO checkItems
  def from(items: Any*) = new JsonArray(items.toVector)
  // TODO more advanced from that converts into JsonObjects etc

  private val EMPTY = JsonArray.create

  def empty: JsonArray = EMPTY

  def create: JsonArray = new JsonArray(Vector.empty)

  // TODO from(Map)
}