/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package experiments

import java.util

import com.couchbase.client.scala.json.JsonArray

import scala.collection.GenSet
import scala.language.dynamics

//import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._

/**
  * Supports Jsoniter
  */

case class JsoniterObject(val content: java.util.HashMap[String, Any] = new util.HashMap[String, Any]()) {

  // Don't make this Dynamic, it makes it easy to misuse

//  def selectDynamic(name: String): GetSelecter = GetSelecter(this, PathElements(List(PathObjectOrField(name))))
//  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, PathElements(List(PathArray(name, index))))

  def put(name: String, value: Any): JsoniterObject = {
    content.put(name, value)
//    Objects.requireNonNull(name)
//    if (!checkType(value)) throw new IllegalArgumentException("Unsupported type for JsoniterObject: " + value.getClass)
    this
  }

//  def put(name: String, value: Int): JsoniterObject = {
//    Objects.requireNonNull(name)
//    //    if (!checkType(value)) throw new IllegalArgumentException("Unsupported type for JsoniterObject: " + value.getClass)
//    copy(content + (name -> JsonNumber(value)))
//  }

  def getString(name: String): String = {
    content.get(name) match {
      case v: com.jsoniter.any.Any => v.toString
      case v: String => v
    }
  }

  def getInt(name: String): Int = {
    content.get(name) match {
      case v: com.jsoniter.any.Any => v.toInt()
      case v: Int => v
    }
  }

  def getBoolean(name: String): Boolean = {
    content.get(name).asInstanceOf[Boolean]
  }

  def getLong(name: String): Long = {
    content.get(name).asInstanceOf[Long]
  }

  def getDouble(name: String): Double = {
    content.get(name).asInstanceOf[Double]
  }

  def getObject(name: String): JsoniterObject = {
    content.get(name).asInstanceOf[JsoniterObject]
  }

  def getArray(name: String): JsonArray = {
    content.get(name).asInstanceOf[JsonArray]
  }

  def getBigInt(name: String): BigInt = {
    content.get(name).asInstanceOf[BigInt]
  }

  def getBigDecimal(name: String): BigDecimal = {
    content.get(name).asInstanceOf[BigDecimal]
  }


  def getStringOpt(name: String): Option[String] = {
    Option(content.get(name)).map(_.asInstanceOf[String])
  }

  def getIntOpt(name: String): Option[Int] = {
    Option(content.get(name)).map(_.asInstanceOf[Int])
  }

  def getBooleanOpt(name: String): Option[Boolean] = {
    Option(content.get(name)).map(_.asInstanceOf[Boolean])
  }

  def getLongOpt(name: String): Option[Long] = {
    Option(content.get(name)).map(_.asInstanceOf[Long])
  }

  def getDoubleOpt(name: String): Option[Double] = {
    Option(content.get(name)).map(_.asInstanceOf[Double])
  }

  def getObjectOpt(name: String): Option[JsoniterObject] = {
    Option(content.get(name)).map(_.asInstanceOf[JsoniterObject])
  }

  def getArrayOpt(name: String): Option[JsonArray] = {
    Option(content.get(name)).map(_.asInstanceOf[JsonArray])
  }

  def getBigIntOpt(name: String): Option[BigInt] = {
    Option(content.get(name)).map(_.asInstanceOf[BigInt])
  }

  def getBigDecimalOpt(name: String): Option[BigDecimal] = {
    Option(content.get(name)).map(_.asInstanceOf[BigDecimal])
  }

  def removeKey(name: String): JsoniterObject = {
    content.remove(name)
    this
  }

  def containsKey(name: String): Boolean = {
    content.containsKey(name)
  }

  def names: GenSet[String] = {
    content.keySet.asScala
  }

  def isEmpty: Boolean = {
    content.isEmpty
  }

  def size: Int = {
    content.size
  }

  private def checkType(value: Any): Boolean = {
    value match {
      case x: String => true
      case x: Int => true
      case x: Long => true
      case x: Double => true
      case x: Boolean => true
      case x: BigInt => true
      case x: BigDecimal => true
      case x: JsoniterObject => true
      case x: JsonArray => true
      case _ => false
    }
  }

}


object JsoniterObject {
  def create = new JsoniterObject()
}

