package com.couchbase.client.java

import java.util.function.Function

import com.couchbase.client.scala.document.JsonObject
import scala.language.dynamics


case class GetSelecter(private val result: GetResult, thisName: String) extends Dynamic {
  def selectDynamic(name: String): GetSelecter = GetSelecter(result, thisName + "." + name)
  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(result, thisName + "[" + index + "]." + name)

  def getString() = result.contentAs[String](thisName)
  def getInt() = result.contentAs[Int](thisName)
  def getAs[T]() = result.contentAs[T](thisName)
}

/**
  * Experimental prototype for a different result type on fetch.
  */
class GetResult(val id: String,
                val cas: Long,
                private val _content: Array[Byte]) extends Dynamic {

  def content: JsonObject = contentAs[JsonObject]

  def content(path: String): JsonObject = contentAs[JsonObject](path)

  def contentAs[T]: T = contentAs(null)

  def contentAs[T](path: String): T = contentAs(path, null)

  def contentAs[T](path: String, decoder: (Array[Byte]) => T): T = None.asInstanceOf[T]

  def selectDynamic(name: String): GetSelecter = GetSelecter(this, name)
  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, name + "[" + index + "]")
}
