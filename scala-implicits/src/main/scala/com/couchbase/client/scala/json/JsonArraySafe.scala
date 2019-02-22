package com.couchbase.client.scala.json

import scala.util.{Failure, Try}

/** A 'safe' wrapper around JsonArray that does operations with Try rather than throwing exceptions.
  */
case class JsonArraySafe(private[scala] val a: JsonArray) {
  private val values = a.values

  def str(idx: Int): Try[String] = {
    Try(a.str(idx))
  }

  def numLong(idx: Int): Try[Long] = {
    Try(a.numLong(idx))
  }

  def num(idx: Int): Try[Int] = {
    Try(a.num(idx))
  }

  def numDouble(idx: Int): Try[Double] = {
    Try(a.numDouble(idx))
  }

  def numFloat(idx: Int): Try[Float] = {
    Try(a.numFloat(idx))
  }

  def obj(idx: Int): Try[JsonObjectSafe] = {
    Try(a.obj(idx)).map(JsonObjectSafe(_))
  }

  def arr(idx: Int): Try[JsonArraySafe] = {
    Try(a.arr(idx)).map(JsonArraySafe)
  }

  def add(item: Any): JsonArray = a.add(item)
  def addNull: JsonArray = a.addNull

  def get(idx: Int): Try[Any] = {
    Try(a.get(idx))
  }

  def isEmpty: Boolean = a.isEmpty
  def nonEmpty: Boolean = a.nonEmpty
  def iterator: Iterator[Any] = a.iterator
  def size: Int = a.size

  def toSeq: Seq[Any] = a.toSeq
  def toJavaList: java.util.List[Any] = a.toJavaList

  def dyn: GetSelecterSafe = GetSelecterSafe(Right(this), Seq())
  override def toString: String = super.toString
}
