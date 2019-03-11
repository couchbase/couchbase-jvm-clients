package com.couchbase.client.scala.json

import com.couchbase.client.core.error.DecodingFailedException

/** Utility methods to convert between supported types stored in
  * [[com.couchbase.client.scala.json.JsonObject]] and
  * [[com.couchbase.client.scala.json.JsonArray]].
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] object ValueConvertor {
  def str(out: Any, name: String): String = {
    out match {
      case v: String => v
      case null => null
      case _ => out.toString
    }
  }

  def num(out: Any, name: String): Int = {
    out match {
      case v: Int => v
      case v: Long => v.toInt
      case v: Double => v.toInt
      case v: Float => v.toInt
      case v: Short => v.toInt
      case v: String => v.toInt
      case _ => throw new DecodingFailedException(s"$name '$out' cannot be converted to Int")
    }
  }

  def bool(out: Any, name: String): Boolean = {
    out match {
      case v: Boolean => v
      case _ => throw new DecodingFailedException(s"$name '$out' cannot be converted to Boolean")
    }
  }

  def numLong(out: Any, name: String): Long = {
    out match {
      case v: Long => v
      case v: Int => v.toLong
      case v: Short => v.toLong
      case v: Float => v.toLong
      case v: Double => v.toLong
      case v: String => v.toLong
      case _ => throw new DecodingFailedException(s"$name '$out' cannot be converted to Long")
    }
  }

  def numDouble(out: Any, name: String): Double = {
    out match {
      case v: Float => v.toDouble
      case v: Double => v
      case v: Long => v.toDouble
      case v: Short => v.toDouble
      case v: Int => v.toDouble
      case v: String => v.toDouble
      case _ => throw new DecodingFailedException(s"$name '$out' cannot be converted to Double")
    }
  }

  def numFloat(out: Any, name: String): Float = {
    out match {
      case v: Float => v
      case v: Double => v.toFloat
      case v: Long => v.toFloat
      case v: Short => v.toFloat
      case v: Int => v.toFloat
      case v: String => v.toFloat
      case _ => throw new DecodingFailedException(s"$name '$out' cannot be converted to Double")
    }
  }

  def obj(out: Any, name: String): JsonObject = {
    out match {
      case v: JsonObject => v
      case v: JsonObjectSafe => v.o
      case null => null
      case _ => throw new DecodingFailedException(s"$name '$out' cannot be converted to JsonObject")
    }
  }

  def arr(out: Any, name: String): JsonArray = {
    out match {
      case v: JsonArray => v
      case v: JsonArraySafe => v.a
      case null => null
      case _ => throw new DecodingFailedException(s"$name '$out' cannot be converted to JsonArray")
    }
  }}
